#pragma once

#include <Common/SharedMutex.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{

/// Provides scoped access to a mutex-protected object while holding a lock guard.
template <typename T, typename LockGuard>
class MutexProtectedAccessor
{
public:
    template <typename Mutex>
    MutexProtectedAccessor(Mutex & mutex_, T & object_)
        : lock(mutex_)
        , object(object_)
    {
    }

    MutexProtectedAccessor(const MutexProtectedAccessor &) = delete;
    MutexProtectedAccessor & operator=(const MutexProtectedAccessor &) = delete;
    MutexProtectedAccessor(MutexProtectedAccessor &&) = delete;
    MutexProtectedAccessor & operator=(MutexProtectedAccessor &&) = delete;

    T * operator->() &
    {
        return std::addressof(object);
    }

    const T * operator->() const &
    {
        return std::addressof(object);
    }

    T * operator->() && = delete;
    const T * operator->() const && = delete;

    T & operator*() &
    {
        return object;
    }

    const T & operator*() const &
    {
        return object;
    }

    T & operator*() && = delete;
    const T & operator*() const && = delete;

private:
    LockGuard lock;
    T & object;
};

/// Protects an object with a mutex and provides scoped read-only or write-enabled access.
template <
    typename T,
    class Mutex = SharedMutex,
    template <class> class UniqueLock = std::unique_lock,
    template <class> class SharedLock = std::shared_lock>
class MutexProtected
{
public:
    using type = T;

    MutexProtected()
        : mutex()
        , object()
    {
    }

    explicit MutexProtected(T object_)
        : mutex()
        , object(std::move(object_))
    {
    }

    template <typename... Args>
    explicit MutexProtected(std::in_place_t, Args &&... args)
        : mutex()
        , object(std::forward<Args>(args)...)
    {
    }

    [[nodiscard]] auto getReadOnly() const &
        -> MutexProtectedAccessor<const T, SharedLock<Mutex>>
    {
        return {mutex, object};
    }

    auto getReadOnly() const &&
        -> MutexProtectedAccessor<const T, SharedLock<Mutex>> = delete;

    [[nodiscard]] auto getWriteEnabled() &
        -> MutexProtectedAccessor<T, UniqueLock<Mutex>>
    {
        return {mutex, object};
    }

    auto getWriteEnabled() &&
        -> MutexProtectedAccessor<T, UniqueLock<Mutex>> = delete;

private:
    mutable Mutex mutex;
    T object;
};

template <typename T>
MutexProtected(T) -> MutexProtected<T>;

}
