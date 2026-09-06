#include <Common/Config/ConfigurationWithUsageTracking.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Poco skips the leading dots of a key (see `XMLConfiguration::findNode`), and the code reading
/// a configuration usually forms a key as `config_prefix + ".name"`, which gives ".name" when the
/// prefix is empty. Bring such keys to a single form to be able to compare them.
String normalizeKey(const String & key)
{
    size_t pos = key.find_first_not_of('.');
    if (pos == String::npos)
        return {};
    return key.substr(pos);
}

}

ConfigurationWithUsageTracking::ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_)
    : config(config_)
{
}

ConfigurationWithUsageTracking::~ConfigurationWithUsageTracking() = default;

bool ConfigurationWithUsageTracking::getRaw(const std::string & key, std::string & value) const
{
    markAsUsed(key);

    /// A missing key is reported by returning false, while `getRawString` throws.
    if (!config.has(key))
        return false;

    value = config.getRawString(key);
    return true;
}

void ConfigurationWithUsageTracking::setRaw(const std::string & key, const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot modify a read-only configuration, key: {}", key);
}

void ConfigurationWithUsageTracking::enumerate(const std::string & key, Keys & range) const
{
    config.keys(key, range);
}

void ConfigurationWithUsageTracking::markAsUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    used_keys.insert(normalizeKey(key));
}

bool ConfigurationWithUsageTracking::isUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    return used_keys.contains(normalizeKey(key));
}

Strings ConfigurationWithUsageTracking::getUnusedKeys(const String & prefix) const
{
    Strings result;
    collectUnusedKeys(prefix, "", false, result);
    return result;
}

void ConfigurationWithUsageTracking::collectUnusedKeys(
    const String & prefix, const String & relative_key, bool parent_is_used, Strings & result) const
{
    String key;
    if (relative_key.empty())
        key = prefix;
    else if (prefix.empty())
        key = relative_key;
    else
        key = prefix + "." + relative_key;

    /// The section itself (an empty relative key) is used by definition - we are looking inside it.
    const bool is_used = parent_is_used || (!relative_key.empty() && isUsed(key));

    Keys children;
    config.keys(key, children);

    if (children.empty())
    {
        if (!is_used && !relative_key.empty())
            result.push_back(relative_key);
        return;
    }

    for (const auto & child : children)
        collectUnusedKeys(prefix, relative_key.empty() ? child : relative_key + "." + child, is_used, result);
}

}
