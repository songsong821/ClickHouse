#pragma once

#include <base/types.h>

#include <mutex>
#include <unordered_set>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/** A read-only proxy for a configuration, remembering which keys have been read through it.
  *
  * It answers the question: which elements of the configuration does nothing know about?
  * An element that is present in the configuration but is never read does nothing at all,
  * and it is almost always a typo or an invented name, which is better to report
  * than to silently ignore.
  *
  * Enumerating the keys of a section (`keys`) is not a read of these keys:
  * the code that enumerates a section still has to read the values it is interested in.
  */
class ConfigurationWithUsageTracking : public Poco::Util::AbstractConfiguration
{
public:
    explicit ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_);

    ~ConfigurationWithUsageTracking() override;

    /// Remember a key as used, for the keys that are read by someone else, not through this object.
    void markAsUsed(const String & key) const;

    /// The leaf keys inside `prefix` that were neither read through this object nor marked as used.
    /// An empty prefix means the whole configuration. The names are returned relative to `prefix`.
    /// A key is also considered used when one of its parents has been read, because reading a section
    /// as a whole (usually with `has`) is a legitimate way to use everything inside it.
    Strings getUnusedKeys(const String & prefix) const;

protected:
    bool getRaw(const std::string & key, std::string & value) const override;
    void setRaw(const std::string & key, const std::string & value) override;
    void enumerate(const std::string & key, Keys & range) const override;

private:
    const Poco::Util::AbstractConfiguration & config;

    mutable std::mutex mutex;
    mutable std::unordered_set<String> used_keys;

    bool isUsed(const String & key) const;
    void collectUnusedKeys(const String & prefix, const String & relative_key, bool parent_is_used, Strings & result) const;
};

}
