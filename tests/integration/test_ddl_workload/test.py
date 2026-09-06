# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

# use_ddl_workload=1 -> DDL queries are scheduled under the `ddl_workload` setting.
node: ClickHouseInstance = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["configs/use_ddl_workload.xml"],
    with_zookeeper=True,
    cpu_limit=15,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clean():
    node.query(
        """
        drop table if exists ddl_dst sync;
        drop workload if exists regular;
        drop workload if exists ddlwl;
        drop workload if exists all;
        drop resource if exists query;
        """
    )
    yield


def setup_workloads() -> None:
    node.query(
        """
        create resource query (query);
        create workload all settings max_concurrent_queries=20;
        create workload regular in all;
        create workload ddlwl in all;
        """
    )


def inflight(workload: str) -> int:
    return int(
        node.query(
            f"select inflight_requests from system.scheduler where "
            f"path like '%/{workload}/semaphore' and resource='query'"
        ).strip()
        or "0"
    )


def run_in_background(query: str) -> "BgQuery":
    bg = BgQuery(query)
    bg.start()
    return bg


class BgQuery:
    def __init__(self, query: str):
        self.query = query
        self.error: str = None
        self.thread = threading.Thread(target=self._run)

    def _run(self) -> None:
        try:
            node.query(self.query)
        except Exception as ex:  # noqa: BLE001
            self.error = str(ex)

    def start(self) -> None:
        self.thread.start()

    def join(self) -> None:
        self.thread.join()


def wait_inflight(workload: str, expected: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if inflight(workload) == expected:
            return
        time.sleep(0.1)
    assert inflight(workload) == expected, f"workload '{workload}' inflight != {expected}"


def test_server_setting_is_enabled() -> None:
    assert (
        node.query(
            "select value from system.server_settings where name='use_ddl_workload'"
        ).strip()
        == "1"
    )


def test_ddl_query_uses_ddl_workload() -> None:
    # A slow DDL (CREATE ... AS SELECT) holds a QUERY slot for the whole INSERT SELECT.
    # With use_ddl_workload=1 it must be admitted under `ddl_workload` ('ddlwl'), NOT the
    # session `workload` ('regular').
    setup_workloads()
    bg = run_in_background(
        "create table ddl_dst engine=MergeTree order by tuple() as "
        "select number from numbers_mt(100000000) "
        "settings ddl_workload='ddlwl', workload='regular', max_threads=2"
    )
    try:
        wait_inflight("ddlwl", 1)
        # The session workload must not be touched by the DDL.
        assert inflight("regular") == 0
    finally:
        bg.join()
    assert bg.error is None, bg.error


def test_regular_query_uses_workload() -> None:
    # A non-DDL query is unaffected: it stays under the `workload` setting ('regular').
    setup_workloads()
    bg = run_in_background(
        "select count(*) from numbers_mt(100000000) "
        "settings workload='regular', max_threads=2"
    )
    try:
        wait_inflight("regular", 1)
        assert inflight("ddlwl") == 0
    finally:
        bg.join()
    assert bg.error is None, bg.error
