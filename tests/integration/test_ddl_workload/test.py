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
        drop table if exists ddl_dst_0 sync;
        drop table if exists ddl_dst_1 sync;
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


class QueryPool:
    """Keep `num` copies of a query continuously in flight (a single such query finishes too
    fast to observe an in-flight slot), mirroring tests/integration/test_scheduler_query."""

    def __init__(self, make_query, num: int = 2):
        self.make_query = make_query  # i -> SQL
        self.num = num
        self.threads: list = []
        self.stop_event = threading.Event()
        self.error: str = None

    def start(self) -> None:
        def run(i: int) -> None:
            while not self.stop_event.is_set():
                try:
                    node.query(self.make_query(i))
                except Exception as ex:  # noqa: BLE001
                    self.error = str(ex)
                    return

        for i in range(self.num):
            t = threading.Thread(target=run, args=(i,))
            self.threads.append(t)
            t.start()

    def stop(self) -> None:
        self.stop_event.set()
        for t in self.threads:
            t.join()
        self.threads.clear()


def wait_inflight_at_least(workload: str, limit: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    last = 0
    while time.time() < deadline:
        last = inflight(workload)
        if last >= limit:
            return
        time.sleep(0.1)
    raise AssertionError(f"inflight('{workload}') never reached {limit} (last={last})")


def test_server_setting_is_enabled() -> None:
    assert (
        node.query(
            "select value from system.server_settings where name='use_ddl_workload'"
        ).strip()
        == "1"
    )


def test_ddl_query_uses_ddl_workload() -> None:
    # With use_ddl_workload=1 a DDL (CREATE ... AS SELECT, kept CPU-bound so it holds its
    # query slot) must be admitted under `ddl_workload` ('ddlwl'), NOT the session `workload`
    # ('regular').
    setup_workloads()
    pool = QueryPool(
        lambda i: (
            f"create or replace table ddl_dst_{i} engine=MergeTree order by tuple() as "
            f"select count(*) from numbers_mt(100000000) "
            f"settings ddl_workload='ddlwl', workload='regular', max_threads=2"
        ),
        num=2,
    )
    pool.start()
    try:
        wait_inflight_at_least("ddlwl", 1)
        # The session workload must not be touched by the DDL.
        assert inflight("regular") == 0, "DDL must not run under the session workload"
    finally:
        pool.stop()
    assert pool.error is None, pool.error


def test_regular_query_uses_workload() -> None:
    # A non-DDL query is unaffected: it stays under the `workload` setting ('regular').
    setup_workloads()
    pool = QueryPool(
        lambda i: (
            "select count(*) from numbers_mt(100000000) "
            "settings workload='regular', max_threads=2"
        ),
        num=2,
    )
    pool.start()
    try:
        wait_inflight_at_least("regular", 1)
        assert inflight("ddlwl") == 0
    finally:
        pool.stop()
    assert pool.error is None, pool.error
