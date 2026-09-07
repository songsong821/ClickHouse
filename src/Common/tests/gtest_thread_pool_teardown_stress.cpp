#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Reproduction harness for the distributed-query thread-teardown heap corruption
/// (ASan `sanitizer_allocator_secondary.h` CHECK on master, exposed by
/// `max_free_threads = 0`). The existing `LIFONotifyWithThreadSelfRemoval` test
/// already covers self-removal with a quiescent producer; this one adds the two
/// ingredients the real workload has and that test lacks:
///   1. many producers scheduling concurrently while workers self-remove, and
///   2. a concurrent resizer that constantly shrinks the limits, driving
///      `wakeUpExcessIdleThreadsNoLock` to race with self-removal and with
///      `popNewestIdleThreadNoLock` in `scheduleImpl`.
/// If the pool's own teardown bookkeeping has a use-after-free, ASan/TSan should
/// trip here. If this stays clean under ASan, the corruption is above the pool
/// (ThreadStatus / MemoryTracker / async-IO lifetime), not in the pool itself.
TEST(ThreadPool, TeardownStressConcurrentScheduleAndResize)
{
    constexpr size_t max_threads = 16;
    constexpr auto duration = std::chrono::seconds(20);
    constexpr size_t num_producers = 8;

    ThreadPool pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        max_threads, /*max_free_threads*/ 0, /*queue_size*/ 4096);

    std::atomic<bool> stop = false;
    std::atomic<size_t> done = 0;

    /// Producers hammer the pool with tiny jobs. Each job touches heap so a
    /// corrupted allocator surfaces quickly.
    std::vector<std::thread> producers;
    producers.reserve(num_producers);
    for (size_t p = 0; p < num_producers; ++p)
    {
        producers.emplace_back([&]
        {
            while (!stop.load(std::memory_order_relaxed))
            {
                const bool scheduled = pool.trySchedule([&]
                {
                    std::vector<int> v(8, 1);
                    done.fetch_add(static_cast<size_t>(v.front()), std::memory_order_relaxed);
                });
                if (!scheduled)
                    std::this_thread::yield();
            }
        });
    }

    /// Resizer constantly shrinks/grows the limits, forcing excess idle threads
    /// to retire concurrently with scheduling and self-removal.
    std::thread resizer([&]
    {
        std::mt19937 rng(0xC0FFEE);
        while (!stop.load(std::memory_order_relaxed))
        {
            pool.setMaxThreads(1 + (rng() % max_threads));
            pool.setMaxFreeThreads(rng() % 4);
            std::this_thread::yield();
        }
    });

    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    for (auto & t : producers)
        t.join();
    resizer.join();

    pool.setMaxThreads(max_threads);
    pool.wait();

    EXPECT_GT(done.load(), 0u);
}
