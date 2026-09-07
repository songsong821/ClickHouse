#include <optional>
#include "config.h"

#if USE_AVRO

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <Common/DateLUT.h>
#include <Core/DecimalFunctions.h>
#include <base/arithmeticOverflow.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <Common/quoteString.h>
#include <fmt/ranges.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int LOGICAL_ERROR;
}

namespace DB::Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name)
{
    auto transform_and_argument = parseTransformAndArgument(transform_name_src);
    if (!transform_and_argument)
    {
        LOG_WARNING(&Poco::Logger::get("Iceberg Partition Pruning"), "Cannot parse iceberg transform name: {}.", transform_name_src);
        return nullptr;
    }

    std::string transform_name = Poco::toLower(transform_name_src);
    if (transform_name == "identity")
        return make_intrusive<ASTIdentifier>(column_name);

    if (transform_name == "void")
        return makeASTOperator("tuple");

    if (transform_and_argument->argument.has_value())
    {
        return makeASTFunction(
                transform_and_argument->transform_name, make_intrusive<ASTLiteral>(*transform_and_argument->argument), make_intrusive<ASTIdentifier>(column_name));
    }
    return makeASTFunction(transform_and_argument->transform_name, make_intrusive<ASTIdentifier>(column_name));
}

namespace
{
    constexpr const char * row_id_column = "_row_id";
    constexpr const char * last_sequence_number_column = "_last_updated_sequence_number";
}

std::unique_ptr<DB::ActionsDAG> ManifestFilesPruner::transformFilterDagForManifest(
    const DB::ActionsDAG * source_dag,
    std::vector<Int32> & used_columns_in_filter,
    std::unordered_map<Int32, DB::NameAndTypePair> & row_lineage_columns_in_filter) const
{
    const auto & inputs = source_dag->getInputs();

    for (const auto & input : inputs)
    {
        if (input->type == ActionsDAG::ActionType::INPUT)
        {
            std::string input_name = input->result_name;
            if (input_name == row_id_column || input_name == last_sequence_number_column)
            {
                const Int32 field_id = input_name == row_id_column ? row_id_field_id : last_updated_sequence_number_field_id;
                used_columns_in_filter.push_back(field_id);
                row_lineage_columns_in_filter.emplace(field_id, DB::NameAndTypePair(input_name, input->result_type));
                continue;
            }

            std::optional<Int32> input_id = schema_processor.tryGetColumnIDByName(current_schema_id, input_name);
            if (input_id)
                used_columns_in_filter.push_back(*input_id);
        }
    }

    ActionsDAG dag_with_renames;
    for (const auto column_id : used_columns_in_filter)
    {
        if (auto lineage_column = row_lineage_columns_in_filter.find(column_id); lineage_column != row_lineage_columns_in_filter.end())
        {
            const auto * node = &dag_with_renames.addInput(lineage_column->second.name, lineage_column->second.type);
            dag_with_renames.getOutputs().push_back(node);
            continue;
        }

        auto column = schema_processor.tryGetFieldCharacteristics(current_schema_id, column_id);

        /// Columns which we dropped and don't exist in current schema
        /// cannot be queried in WHERE expression.
        if (!column.has_value())
            continue;

        /// We take data type from manifest schema, not latest type
        auto column_from_manifest = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);
        if (!column_from_manifest.has_value())
            continue;

        auto numeric_column_name = DB::backQuote(DB::toString(column_id));
        const auto * node = &dag_with_renames.addInput(numeric_column_name, column_from_manifest->type);
        node = &dag_with_renames.addAlias(*node, column->name);
        dag_with_renames.getOutputs().push_back(node);
    }
    auto result = std::make_unique<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(dag_with_renames), source_dag->clone()));
    result->removeUnusedActions();
    return result;
}


ManifestFilesPruner::ManifestFilesPruner(
    const IcebergSchemaProcessor & schema_processor_,
    Int32 current_schema_id_,
    Int32 initial_schema_id_,
    const DB::ActionsDAG * filter_dag,
    const ManifestFileIterator & manifest_file,
    DB::ContextPtr context)
    : schema_processor(schema_processor_)
    , current_schema_id(current_schema_id_)
    , initial_schema_id(initial_schema_id_)
{
    if (filter_dag == nullptr)
    {
        return;
    }

    std::unique_ptr<ActionsDAG> transformed_dag;
    std::vector<Int32> used_columns_in_filter;
    transformed_dag = transformFilterDagForManifest(filter_dag, used_columns_in_filter, row_lineage_columns);
    chassert(transformed_dag != nullptr);

    if (manifest_file.hasPartitionKey())
    {
        partition_key = &manifest_file.getPartitionKeyDescription();
        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
        partition_key_condition.emplace(
            inverted_dag, context, partition_key->column_names, partition_key->expression, true /* single_point */);
    }

    for (Int32 used_column_id : used_columns_in_filter)
    {
        std::optional<NameAndTypePair> name_and_type;
        if (auto lineage_column = row_lineage_columns.find(used_column_id); lineage_column != row_lineage_columns.end())
        {
            name_and_type = lineage_column->second;
        }
        else
        {
            name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, used_column_id);
            if (!name_and_type.has_value())
                continue;

            name_and_type->name = DB::backQuote(DB::toString(used_column_id));
        }

        ExpressionActionsPtr expression
            = std::make_shared<ExpressionActions>(ActionsDAG({name_and_type.value()}), ExpressionActionsSettings(context));

        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
        min_max_key_conditions.emplace(used_column_id, KeyCondition(inverted_dag, context, {name_and_type->name}, expression));
    }
}

namespace
{

/// Iceberg keeps a decimal partition value as an Avro `fixed`: the unscaled value in two's-complement
/// big-endian form, using the minimum number of bytes. ClickHouse reads such a `fixed` as a `String`,
/// so restore the decimal here. Accumulate into the unsigned counterpart, pre-filled with the sign
/// bits, so that the sign extension comes out of the shifts themselves.
template <typename DecimalType>
Field decodePartitionDecimal(const String & bytes, const IDataType & type)
{
    using NativeType = typename DecimalType::NativeType;
    using UnsignedType = make_unsigned_t<NativeType>;

    if (bytes.empty() || bytes.size() > sizeof(NativeType))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg partition value of a decimal column is {} bytes long, which does not fit into {} bytes of {}",
            bytes.size(),
            sizeof(NativeType),
            type.getName());

    UnsignedType unscaled_value = (bytes[0] & 0x80) ? ~UnsignedType(0) : UnsignedType(0);
    for (const auto byte : bytes)
        unscaled_value = (unscaled_value << 8) | static_cast<UInt8>(byte);

    return DecimalField<DecimalType>(static_cast<NativeType>(unscaled_value), getDecimalScale(type));
}

Field decodePartitionDecimalByType(const String & bytes, const IDataType & type)
{
    if (checkDecimal<Decimal32>(type))
        return decodePartitionDecimal<Decimal32>(bytes, type);
    if (checkDecimal<Decimal64>(type))
        return decodePartitionDecimal<Decimal64>(bytes, type);
    if (checkDecimal<Decimal128>(type))
        return decodePartitionDecimal<Decimal128>(bytes, type);
    if (checkDecimal<Decimal256>(type))
        return decodePartitionDecimal<Decimal256>(bytes, type);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected decimal type {} of an Iceberg partition column", type.getName());
}

}

namespace
{

enum class PartitionTransformKind : uint8_t
{
    Day,
    Month,
    Year,
    Hour,
    NotInvertible,
};

PartitionTransformKind parsePartitionTransformKind(const String & transform_name_src)
{
    const String transform_name = Poco::toLower(transform_name_src);

    if (transform_name == "day" || transform_name == "days" || transform_name == "date" || transform_name == "dates")
        return PartitionTransformKind::Day;
    if (transform_name == "month" || transform_name == "months")
        return PartitionTransformKind::Month;
    if (transform_name == "year" || transform_name == "years")
        return PartitionTransformKind::Year;
    if (transform_name == "hour" || transform_name == "hours")
        return PartitionTransformKind::Hour;
    return PartitionTransformKind::NotInvertible;
}

std::optional<Int64> multiplyChecked(Int64 left, Int64 right)
{
    Int64 result = 0;
    if (common::mulOverflow(left, right, result))
        return {};
    return result;
}

std::optional<Int64> addChecked(Int64 left, Int64 right)
{
    Int64 result = 0;
    if (common::addOverflow(left, right, result))
        return {};
    return result;
}

std::optional<std::pair<Int64, Int64>> dayIntervalOfPartitionValue(PartitionTransformKind kind, Int64 value)
{
    const auto & utc = DateLUT::instance("UTC");
    const auto epoch = ExtendedDayNum(0);

    switch (kind)
    {
        case PartitionTransformKind::Day:
            return std::pair{value, value};
        case PartitionTransformKind::Month:
        {
            const auto first = utc.addMonths(epoch, value);
            if (utc.toMonthNumSinceEpoch(first) != value)
                return {};
            return std::pair{Int64{first}, Int64{utc.addMonths(epoch, value + 1)} - 1};
        }
        case PartitionTransformKind::Year:
        {
            const auto first = utc.addYears(epoch, value);
            if (utc.toYearSinceEpoch(first) != value)
                return {};
            return std::pair{Int64{first}, Int64{utc.addYears(epoch, value + 1)} - 1};
        }
        default:
            return {};
    }
}

std::optional<std::pair<Int64, Int64>> secondIntervalOfPartitionValue(PartitionTransformKind kind, Int64 value)
{
    if (kind == PartitionTransformKind::Hour)
    {
        auto first = multiplyChecked(value, 3600);
        auto next = first ? addChecked(*first, 3600) : std::nullopt;
        if (!next)
            return {};
        return std::pair{*first, *next - 1};
    }

    auto days = dayIntervalOfPartitionValue(kind, value);
    if (!days)
        return {};

    auto first = multiplyChecked(days->first, 86400);
    auto next_day = addChecked(days->second, 1);
    auto next = first && next_day ? multiplyChecked(*next_day, 86400) : std::nullopt;
    if (!next)
        return {};
    return std::pair{*first, *next - 1};
}

std::optional<Range> rangeOfPartitionValue(const String & transform_name, const Field & partition_value, const IDataType & source_type)
{
    if (partition_value.isNull())
        return {};

    Int64 value = 0;
    if (partition_value.getType() == Field::Types::Int64)
        value = partition_value.safeGet<Int64>();
    else if (partition_value.getType() == Field::Types::UInt64)
    {
        UInt64 unsigned_value = partition_value.safeGet<UInt64>();
        if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return {};
        value = static_cast<Int64>(unsigned_value);
    }
    else
        return {};

    const PartitionTransformKind kind = parsePartitionTransformKind(transform_name);
    const WhichDataType which(source_type);

    if (which.isDateOrDate32())
    {
        auto days = dayIntervalOfPartitionValue(kind, value);
        if (!days)
            return {};
        return Range(days->first, true, days->second, true);
    }

    if (!which.isDateTime() && !which.isDateTime64())
        return {};

    auto seconds = secondIntervalOfPartitionValue(kind, value);
    if (!seconds)
        return {};

    if (which.isDateTime())
        return Range(seconds->first, true, seconds->second, true);

    const UInt32 scale = getDecimalScale(source_type);
    const Int64 ticks_per_second = DecimalUtils::scaleMultiplier<Int64>(scale);
    auto first = multiplyChecked(seconds->first, ticks_per_second);
    auto next = addChecked(seconds->second, 1);
    next = next ? multiplyChecked(*next, ticks_per_second) : std::nullopt;
    if (!first || !next)
        return {};
    return Range(DecimalField<Decimal64>(*first, scale), true, DecimalField<Decimal64>(*next - 1, scale), true);
}

}

PruningReturnStatus ManifestFilesPruner::canBePruned(
    const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const
{
    if (partition_key_condition.has_value())
    {
        const auto & partition_value = entry->parsed_entry->partition_key_value;
        std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
        for (size_t i = 0; i < index_value.size(); ++i)
        {
            auto & field = index_value[i];
            const auto & type = partition_key->data_types.at(i);
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
            else if (field.getType() == Field::Types::Int64 && WhichDataType(type).isDateTime64()) /// clickhouse used to write timestamp as simple long in avro
                field = DecimalField<Decimal64>(field.safeGet<Int64>(), getDecimalScale(*type));
            else if (field.getType() == Field::Types::String && WhichDataType(type).isDecimal())
                field = decodePartitionDecimalByType(field.safeGet<String>(), *type);
        }

        bool can_be_true = partition_key_condition->mayBeTrueInRange(
            partition_value.size(), index_value.data(), index_value.data(), partition_key->data_types);

        if (!can_be_true)
        {
            return PruningReturnStatus::PARTITION_PRUNED;
        }
    }

    if (partition_key_condition.has_value() && entry->common_partition_specification)
    {
        const auto & partition_value = entry->parsed_entry->partition_key_value;
        for (const auto & partition_field : *entry->common_partition_specification)
        {
            auto key_condition_it = min_max_key_conditions.find(partition_field.source_id);
            if (key_condition_it == min_max_key_conditions.end())
                continue;

            auto name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, partition_field.source_id);
            if (!name_and_type.has_value())
                continue;

            if (partition_field.tuple_index < 0 || static_cast<size_t>(partition_field.tuple_index) >= partition_value.size())
                continue;

            auto range = rangeOfPartitionValue(
                partition_field.transform_name,
                partition_value[partition_field.tuple_index],
                *removeNullable(name_and_type->type));
            if (!range.has_value())
                continue;

            if (!key_condition_it->second.mayBeTrueInRange(1, &range->left, &range->right, {name_and_type->type}))
                return PruningReturnStatus::PARTITION_PRUNED;
        }
    }

    for (const auto & [column_id, key_condition] : min_max_key_conditions)
    {
        std::optional<NameAndTypePair> name_and_type;
        bool has_no_nulls = true;

        if (auto lineage_column = row_lineage_columns.find(column_id); lineage_column != row_lineage_columns.end())
        {
            name_and_type = lineage_column->second;
        }
        else
        {
            name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);

            if (!name_and_type.has_value())
            {
                continue;
            }

            auto info_it = entry->parsed_entry->columns_infos.find(column_id);
            has_no_nulls = info_it != entry->parsed_entry->columns_infos.end() && info_it->second.nulls_count.has_value()
                && *info_it->second.nulls_count == 0;
        }

        auto rect_it = entry_hyperrectangles.find(column_id);
        if (rect_it == entry_hyperrectangles.end())
            continue;

        if (has_no_nulls && !key_condition.mayBeTrueInRange(1, &rect_it->second.left, &rect_it->second.right, {name_and_type->type}))
        {
            return PruningReturnStatus::MIN_MAX_INDEX_PRUNED;
        }
    }

    return PruningReturnStatus::NOT_PRUNED;
}
}

#endif
