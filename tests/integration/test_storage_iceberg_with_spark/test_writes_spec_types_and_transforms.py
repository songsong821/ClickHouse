import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_creation_expression,
    get_uuid_str,
)

# 2023-12-31 22:00:00.000001 UTC, which is already 2024-01-01 in Asia/Kolkata.
MODERN_MICROS = 1704060000000001
MODERN_LOCAL = "2023-12-31 22:00:00.000001"
# 1969-12-31 00:00:00.000001 UTC, before the epoch every date transform counts from.
PRE_EPOCH_MICROS = -86399999999
PRE_EPOCH_LOCAL = "1969-12-31 00:00:00.000001"

EXPECTED_PARTITION_VALUES = {
    "icebergYear": ["53", "-1"],
    "icebergMonth": ["647", "-1"],
    "icebergDay": ["2023-12-31", "1969-12-31"],
    "icebergHour": ["473350", "-24"],
}


def table_path(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"


def download(started_cluster, storage_type, table_name):
    path = table_path(table_name)
    default_download_directory(started_cluster, storage_type, f"{path}/", f"{path}/")
    return path


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_timestamp_and_unsigned_types(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_timestamp_and_unsigned_types_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(u32 UInt32, ts DateTime64(6), tsz DateTime64(6, 'UTC'))",
    )

    instance.query(
        f"""
        INSERT INTO {table_name}
        SELECT 4294967290, fromUnixTimestamp64Micro({MODERN_MICROS}), fromUnixTimestamp64Micro({MODERN_MICROS})
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(
        f"SELECT u32, toUnixTimestamp64Micro(ts), toUnixTimestamp64Micro(tsz) FROM {table_name}"
    ) == f"4294967290\t{MODERN_MICROS}\t{MODERN_MICROS}\n"

    path = download(started_cluster_iceberg_with_spark, storage_type, table_name)

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {"u32": "bigint", "ts": "timestamp_ntz", "tsz": "timestamp"}
    row = table.selectExpr("u32", "cast(ts as string) AS ts", f"unix_micros(tsz) AS tsz").collect()[0]
    assert row["u32"] == 4294967290
    assert row["ts"] == MODERN_LOCAL
    assert row["tsz"] == MODERN_MICROS

    assert dict(spark.read.parquet(f"{path}/data").dtypes) == {
        "u32": "bigint",
        "ts": "timestamp_ntz",
        "tsz": "timestamp",
    }


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_timestamp_types_orc(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_timestamp_types_orc_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(ts DateTime64(6), tsz DateTime64(6, 'UTC'))",
        format="ORC",
    )

    instance.query(
        f"""
        INSERT INTO {table_name}
        SELECT fromUnixTimestamp64Micro({MODERN_MICROS}), fromUnixTimestamp64Micro({MODERN_MICROS})
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    path = download(started_cluster_iceberg_with_spark, storage_type, table_name)

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {"ts": "timestamp_ntz", "tsz": "timestamp"}
    row = table.selectExpr("cast(ts as string) AS ts", "unix_micros(tsz) AS tsz").collect()[0]
    assert row["ts"] == MODERN_LOCAL
    assert row["tsz"] == MODERN_MICROS

    if storage_type == "local":
        described = instance.query(
            f"DESCRIBE file('iceberg_data/default/{table_name}/data/*.orc', ORC) FORMAT TSVRaw"
        )
        assert [line.split("\t")[:2] for line in described.strip().split("\n")] == [
            ["ts", "Nullable(DateTime64(9))"],
            ["tsz", "Nullable(DateTime64(9, 'UTC'))"],
        ]


@pytest.mark.parametrize("unsupported_type", ["UInt64", "Time", "Time64(6)"])
def test_writes_reject_unrepresentable_types(started_cluster_iceberg_with_spark, unsupported_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_writes_reject_unrepresentable_types_" + get_uuid_str()

    assert "BAD_ARGUMENTS" in instance.query_and_get_error(
        get_creation_expression(
            "local",
            table_name,
            started_cluster_iceberg_with_spark,
            f"(x {unsupported_type})",
        ),
        settings={"enable_time_time64_type": 1},
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
@pytest.mark.parametrize("transform", ["icebergYear", "icebergMonth", "icebergDay", "icebergHour"])
def test_writes_date_transform_partition_values_are_utc(
    started_cluster_iceberg_with_spark, storage_type, transform
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = (
        "test_writes_date_transform_" + transform.lower() + "_" + storage_type + "_" + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(ts DateTime64(6), i Int64)",
        2,
        f"({transform}(ts))",
    )

    instance.query(
        f"""
        INSERT INTO {table_name} VALUES
            (fromUnixTimestamp64Micro({MODERN_MICROS}), 1), (fromUnixTimestamp64Micro({PRE_EPOCH_MICROS}), 2)
        """,
        settings={"allow_insert_into_iceberg": 1, "session_timezone": "Asia/Kolkata"},
    )

    path = download(started_cluster_iceberg_with_spark, storage_type, table_name)
    table = spark.read.format("iceberg").load(path)
    assert table.count() == 2

    for expected_i, local in ((1, MODERN_LOCAL), (2, PRE_EPOCH_LOCAL)):
        rows = table.filter(f"ts = to_timestamp_ntz('{local}')").collect()
        assert [row["i"] for row in rows] == [expected_i], f"Spark cannot find the row for {local}"

    partitions = spark.read.format("iceberg").load(f"{path}#partitions")
    written = sorted(str(row["partition"]["ts"]) for row in partitions.collect())
    assert written == sorted(EXPECTED_PARTITION_VALUES[transform])


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_column_bound_widths(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_column_bound_widths_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(d Date32, i Int32, l Int64, ts DateTime64(6))",
    )

    instance.query(
        f"""
        INSERT INTO {table_name}
        SELECT toDate32('2024-01-01'), -5, -5, fromUnixTimestamp64Micro({MODERN_MICROS})
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    path = download(started_cluster_iceberg_with_spark, storage_type, table_name)
    row = spark.read.format("iceberg").load(f"{path}#files").select("lower_bounds", "upper_bounds").collect()[0]

    # `date` and `int` are stored in 4 little-endian bytes, `long` and `timestamp` in 8.
    expected_widths = {1: 4, 2: 4, 3: 8, 4: 8}
    assert {field_id: len(value) for field_id, value in row["lower_bounds"].items()} == expected_widths
    assert {field_id: len(value) for field_id, value in row["upper_bounds"].items()} == expected_widths


def test_iceberg_date_transform_functions(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    assert instance.query(
        """
        SELECT
            icebergYear(toDate32('1969-05-05')), icebergMonth(toDate32('1969-01-01')),
            icebergDay(toDate32('1969-12-31')), icebergHour(toDateTime64('1969-12-31 23:00:00', 6, 'UTC'))
        """
    ) == "-1\t-12\t-1\t-1\n"

    assert instance.query(
        f"""
        SELECT icebergYear(ts), icebergMonth(ts), icebergDay(ts), icebergHour(ts)
        FROM (SELECT CAST(fromUnixTimestamp64Micro({MODERN_MICROS}) AS DateTime64(6)) AS ts)
        """,
        settings={"session_timezone": "Asia/Kolkata"},
    ) == "53\t647\t19722\t473350\n"

    assert instance.query(
        """
        SELECT
            toTypeName(icebergDay(toDate('2024-01-01'))), toTypeName(icebergBucket(8, 'iceberg')),
            icebergYear(CAST(NULL AS Nullable(Date32)))
        """
    ) == "Int32\tInt32\t\\N\n"

    assert "ILLEGAL_TYPE_OF_ARGUMENT" in instance.query_and_get_error(
        "SELECT icebergHour(toDate('2024-01-01'))"
    )
