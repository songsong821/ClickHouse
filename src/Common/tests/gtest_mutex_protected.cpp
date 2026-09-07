#include <gtest/gtest.h>

#include <Common/MutexProtected.h>

#include <atomic>
#include <mutex>
#include <type_traits>
#include <utility>

namespace
{

struct A
{
    explicit A(int i_) : i(i_) {}

    int i;
};

using ReadOnlyAccessor = decltype(std::declval<const DB::MutexProtected<A> &>().getReadOnly());
using WriteEnabledAccessor = decltype(std::declval<DB::MutexProtected<A> &>().getWriteEnabled());

static_assert(!std::is_copy_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_move_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_move_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_copy_assignable_v<WriteEnabledAccessor>);
static_assert(!std::is_move_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_move_assignable_v<WriteEnabledAccessor>);
static_assert(std::is_same_v<decltype(*std::declval<ReadOnlyAccessor &>()), const A &>);
static_assert(std::is_same_v<decltype(*std::declval<WriteEnabledAccessor &>()), A &>);

struct LockCounts
{
    static void reset()
    {
        unique_locks = 0;
        shared_locks = 0;
    }

    static inline std::atomic_size_t unique_locks = 0;
    static inline std::atomic_size_t shared_locks = 0;
};

template <class Mutex>
class TrackingSharedLock
{
public:
    explicit TrackingSharedLock(Mutex &)
    {
        ++LockCounts::shared_locks;
    }

    ~TrackingSharedLock()
    {
        --LockCounts::shared_locks;
    }

    TrackingSharedLock(const TrackingSharedLock &) = delete;
    TrackingSharedLock & operator=(const TrackingSharedLock &) = delete;
};

template <class Mutex>
class TrackingUniqueLock
{
public:
    explicit TrackingUniqueLock(Mutex &)
    {
        ++LockCounts::unique_locks;
    }

    ~TrackingUniqueLock()
    {
        --LockCounts::unique_locks;
    }

    TrackingUniqueLock(const TrackingUniqueLock &) = delete;
    TrackingUniqueLock & operator=(const TrackingUniqueLock &) = delete;
};

using TrackingMutexProtected = DB::MutexProtected<A, DB::SharedMutex, TrackingUniqueLock, TrackingSharedLock>;

}

TEST(MutexProtected, GetReadOnly)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto roa = a.getReadOnly();
        i = roa->i;
    }

    EXPECT_EQ(i, 5);
}

TEST(MutexProtected, GetWriteEnabled)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        i = ++rwa->i;
    }

    EXPECT_EQ(i, 6);
}

TEST(MutexProtected, GetReadOnlyAcquiresAndReleasesSharedLock)
{
    LockCounts::reset();
    TrackingMutexProtected a{A{5}};

    {
        auto roa = a.getReadOnly();
        EXPECT_EQ(roa->i, 5);
        EXPECT_EQ(LockCounts::shared_locks, 1);
        EXPECT_EQ(LockCounts::unique_locks, 0);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, GetWriteEnabledAcquiresAndReleasesExclusiveLock)
{
    LockCounts::reset();
    TrackingMutexProtected a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        EXPECT_EQ(rwa->i, 5);
        EXPECT_EQ(LockCounts::shared_locks, 0);
        EXPECT_EQ(LockCounts::unique_locks, 1);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, SupportsExclusiveOnlyMutex)
{
    using StdMutexProtected = DB::MutexProtected<A, std::mutex, std::unique_lock, std::unique_lock>;
    StdMutexProtected a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        ++rwa->i;
    }

    {
        auto roa = a.getReadOnly();
        EXPECT_EQ(roa->i, 6);
    }
}
