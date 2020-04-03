#include "storage/sql_table.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/macros.h"
#include "storage/storage_util.h"

namespace terrier::storage {

SqlTable::SqlTable(const common::ManagedPointer<BlockStore> store, const catalog::Schema &schema)
    : block_store_(store) {
  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnOidToIdMap col_oid_to_id;
  ColumnIdToOidMap col_id_to_oid;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema.GetColumns()) {
    switch (column.AttrSize()) {
      case VARLEN_COLUMN:
        col_id_to_oid[col_id_t(offsets[0])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        col_id_to_oid[col_id_t(offsets[1])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        col_id_to_oid[col_id_t(offsets[2])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        col_id_to_oid[col_id_t(offsets[3])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        col_id_to_oid[col_id_t(offsets[4])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  auto layout = storage::BlockLayout(attr_sizes);
  tables_ = {
      {layout_version_t(0),
       {new DataTable(block_store_, layout, layout_version_t(0)), layout, col_oid_to_id, col_id_to_oid}}
  };
}


bool SqlTable::Select(const common::ManagedPointer <transaction::TransactionContext> txn, layout_version_t layout_version,
                      const TupleSlot slot, ProjectedRow *const out_buffer) const {

  // get the version of current tuple slot
  const auto tuple_version = slot.GetBlock()->data_table_->layout_version_;

  TERRIER_ASSERT(tuple_version <= layout_version, "The iterator should not go to data tables with more recent version than the current transaction.");

  if (tuple_version == layout_version) {
    // when current version is same as the intended layout version, get the tuple without transformation
    return tables_.at(tuple_version).data_table_->Select(txn, slot, out_buffer);
  } else {
    // the tuple exists in an older version.
    // TODO(schema-change): handle versions from add and/or drop column only
    col_id_t ori_header[out_buffer->NumColumns()];
    std::vector<catalog::col_oid_t> missing_cols;

    auto desired_v = tables_.at(layout_version);
    auto tuple_v = tables_.at(tuple_version);
    AlignHeaderToVersion(out_buffer, tuple_v, desired_v, &ori_header[0], &missing_cols);
    auto result = tables_.at(tuple_version).data_table_->Select(txn, slot, out_buffer);
    // TODO(Schema-Change): fill in default values if there are missing columns
    if (!missing_cols.empty()) {

    }
    // TODO(Schema-Change): Do we need to copy back the original header
    return result;
  }
}

bool SqlTable::AlignHeaderToVersion(ProjectedRow *const out_buffer, const DataTableVersion &tuple_version,
                                    const DataTableVersion &desired_version, col_id_t *cached_ori_header,
                                    std::vector<catalog::col_oid_t>* const missing_cols) const {
  // reserve the original header, aka intended column ids'
  std::memcpy(cached_ori_header, out_buffer->ColumnIds(), sizeof(col_id_t) * out_buffer->NumColumns());

  // for each column id in the intended version of datatable, change it to match the current schema version
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Output buffer should not read the version pointer column.");
    catalog::col_oid_t col_oid = desired_version.column_id_to_oid_map_.at(out_buffer->ColumnIds()[i]);
    if (tuple_version.column_oid_to_id_map_.count(col_oid) > 0) {
      out_buffer->ColumnIds()[i] = tuple_version.column_oid_to_id_map_.at(col_oid);
    } else {
      out_buffer->ColumnIds()[i] = IGNORE_COLUMN_ID;
      missing_cols->emplace_back(col_oid);
    }
  }
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(table_.column_map_.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = table_.column_map_.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

ProjectionMap SqlTable::ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids) {
  // Resolve OIDs to storage IDs
  auto col_ids = ColIdsForOids(col_oids);

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<col_id_t, catalog::col_oid_t> inverse_map;
  for (uint16_t i = 0; i < col_oids.size(); i++) inverse_map[col_ids[i]] = col_oids[i];

  // Populate the projection map using the in-order iterator on std::map
  ProjectionMap projection_map;
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map[iter.second] = i++;

  return projection_map;
}

catalog::col_oid_t SqlTable::OidForColId(const col_id_t col_id) const {
  const auto oid_to_id = std::find_if(table_.column_map_.cbegin(), table_.column_map_.cend(),
                                      [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id; });
  return oid_to_id->first;
}

}  // namespace terrier::storage
