#include "storage/sql_table.h"

#include <map>
#include <string>
#include <vector>

#include "common/macros.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/storage_util.h"
#include "type/transient_value_peeker.h"

namespace terrier::storage {

SqlTable::SqlTable(const common::ManagedPointer<BlockStore> store, const catalog::Schema &schema)
    : block_store_(store), tables_(MAX_NUM_VERSIONS, DataTableVersion()) {
  // Initialize a DataTable
  auto success UNUSED_ATTRIBUTE =
      CreateTable(common::ManagedPointer<const catalog::Schema>(&schema), layout_version_t(0));
  TERRIER_ASSERT(success, "Creating first data table should not fail.");
}

bool SqlTable::Select(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
                      ProjectedRow *const out_buffer, layout_version_t layout_version) const {
  // get the version of current tuple slot
  const auto tuple_version = slot.GetBlock()->data_table_->layout_version_;

  TERRIER_ASSERT(tuple_version <= layout_version,
                 "The iterator should not go to data tables with more recent version than the current transaction.");

  if (tuple_version == layout_version) {
    // when current version is same as the desired layout version, get the tuple without transformation
    return tables_.at(tuple_version).data_table_->Select(txn, slot, out_buffer);
  }

  // the tuple exists in an older version.
  std::vector<col_id_t> orig_header(out_buffer->NumColumns());
  AttrSizeMap size_map;

  auto desired_v = tables_.at(layout_version);
  DataTableVersion tuple_v = tables_.at(tuple_version);
  auto missing_cols = AlignHeaderToVersion(out_buffer, tuple_v, desired_v, orig_header.data(), &size_map);
  auto result = tuple_v.data_table_->Select(txn, slot, out_buffer, &size_map);

  // copy back the original header
  std::memcpy(out_buffer->ColumnIds(), orig_header.data(), sizeof(col_id_t) * out_buffer->NumColumns());

  if (!missing_cols.empty()) {
    // fill in missing columns and default values
    FillMissingColumns(out_buffer, missing_cols, tuple_version, layout_version);
  }

  return result;
}

// TODO(schema-change): currently if our update fails, TransactionManager::GCLastUpdateOnAbort and
// LogSerializerTask::SerializeRecord will report failure, because there is a version mismatch: TransactionManager
// and LogSerializerTask uses the blocklayout of redo->GetTupleSlot().GetBlock() (note that they do not have access
// to version), which is the blocklayout of the old version, but redo->delta uses the col_ids of the new version.
// One possible solution is to add versioning info to RedoRecord, but that would be a major change.
bool SqlTable::Update(const common::ManagedPointer<transaction::TransactionContext> txn, RedoRecord *const redo,
                      layout_version_t layout_version, TupleSlot *updated_slot) const {
  TERRIER_ASSERT(redo->GetTupleSlot() != TupleSlot(nullptr, 0), "TupleSlot was never set in this RedoRecord.");
  TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                             ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                 "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                 "immediately before?");
  // get the version of current tuple slot
  auto curr_tuple = redo->GetTupleSlot();

  const auto tuple_version = curr_tuple.GetBlock()->data_table_->layout_version_;

  TERRIER_ASSERT(tuple_version <= layout_version,
                 "The iterator should not go to data tables with more recent version than the current transaction.");

  bool result;
  if (tuple_version == layout_version) {
    result = tables_.at(layout_version).data_table_->Update(txn, curr_tuple, *(redo->Delta()));
    // If the updated_slot pointer is used
    if (updated_slot != nullptr) *updated_slot = curr_tuple;
  } else {
    // tuple in an older version, check if all modified columns are in the datatable version where the tuple is in
    col_id_t orig_header[redo->Delta()->NumColumns()];
    AttrSizeMap size_map;

    auto desired_v = tables_.at(layout_version);
    auto tuple_v = tables_.at(tuple_version);
    auto missing = AlignHeaderToVersion(redo->Delta(), tuple_v, desired_v, &orig_header[0], &size_map);

    if (missing.empty()) {
      result = tuple_v.data_table_->Update(txn, curr_tuple, *(redo->Delta()));
      if (updated_slot != nullptr) *updated_slot = curr_tuple;
      std::memcpy(redo->Delta()->ColumnIds(), orig_header, sizeof(col_id_t) * redo->Delta()->NumColumns());
    } else {
      // touching columns that are in the desired version, but not the tuple version
      // do an delete followed by an insert
      // get projected row from redo (This projection is deterministic for identical set of columns)
      std::vector<col_id_t> col_ids;
      for (const auto &it : desired_v.column_id_to_oid_map_) col_ids.emplace_back(it.first);
      auto initializer = ProjectedRowInitializer::Create(desired_v.layout_, col_ids);
      auto *const buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      auto *pr = initializer.InitializeRow(buffer);

      // fill in values to the projection
      result = Select(txn, curr_tuple, pr, layout_version);
      if (updated_slot != nullptr) *updated_slot = curr_tuple;

      if (result) {
        // delete it from old datatable
        result = tuple_v.data_table_->Delete(txn, curr_tuple);
        if (result) {
          // insert it to new datatable

          // apply the change
          StorageUtil::ApplyDelta(desired_v.layout_, *(redo->Delta()), pr);
          if (updated_slot != nullptr) *updated_slot = desired_v.data_table_->Insert(txn, *pr);
        }
      }
      delete[] buffer;
      std::memcpy(redo->Delta()->ColumnIds(), orig_header, sizeof(col_id_t) * redo->Delta()->NumColumns());
    }
  }
  if (!result) {
    // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
    // correctly.
    txn->SetMustAbort();
  }
  return result;
}

TupleSlot SqlTable::Insert(const common::ManagedPointer<transaction::TransactionContext> txn, RedoRecord *const redo,
                           layout_version_t layout_version) const {
  TERRIER_ASSERT(redo->GetTupleSlot() == TupleSlot(nullptr, 0), "TupleSlot was set in this RedoRecord.");
  TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                             ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                 "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                 "immediately before?");
  const auto slot = tables_.at(layout_version).data_table_->Insert(txn, *(redo->Delta()));
  redo->SetTupleSlot(slot);
  return slot;
}

void SqlTable::Scan(const terrier::common::ManagedPointer<transaction::TransactionContext> txn,
                    DataTable::SlotIterator *const start_pos, ProjectedColumns *const out_buffer,
                    const layout_version_t layout_version) const {
  // typically Scan is done in a for loop, where start_pos is initially begin(), and then set to the tupleslot where the
  // last Scan left off. Therefore, we can start from the physical tupleslot location the last Scan leftoff, until the
  // last datatable this transaction can possibly view (the datatable with layout_version)
  layout_version_t tuple_version = (*start_pos)->GetBlock()->layout_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.at(layout_version).column_oid_to_id_map_.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  // Check for version match
  if (tuple_version == layout_version) {
    tables_.at(layout_version).data_table_->Scan(txn, start_pos, out_buffer);
    return;
  }

  std::vector<col_id_t> orig_header(out_buffer->NumColumns());
  uint32_t filled = 0;
  auto desired_v = tables_.at(layout_version);

  for (layout_version_t i = tuple_version; i <= layout_version && out_buffer->NumTuples() < out_buffer->MaxTuples();
       i++) {
    auto start_idx = filled;
    auto tuple_v = tables_.at(i);
    AttrSizeMap size_map;
    auto missing_cols = AlignHeaderToVersion(out_buffer, tuple_v, desired_v, orig_header.data(), &size_map);

    if (i != tuple_version) *start_pos = tuple_v.data_table_->begin();
    tuple_v.data_table_->IncrementalScan(txn, start_pos, out_buffer, filled);
    filled = out_buffer->NumTuples();
    // copy back the original header
    std::memcpy(out_buffer->ColumnIds(), orig_header.data(), sizeof(col_id_t) * out_buffer->NumColumns());

    if (!missing_cols.empty()) {
      for (uint32_t idx = start_idx; idx < filled; idx++) {
        ProjectedColumns::RowView row = out_buffer->InterpretAsRow(idx);
        FillMissingColumns(&row, missing_cols, tuple_version, layout_version);
      }
    }
  }
}

template <class RowType>
void SqlTable::FillMissingColumns(RowType *out_buffer,
                                  const std::vector<std::pair<size_t, catalog::col_oid_t>> &missingl_cols,
                                  layout_version_t tuple_version, layout_version_t layout_version) const {
  for (auto &it : missingl_cols) {
    TERRIER_ASSERT(tables_.at(layout_version).default_value_map_.at(it.second)->GetExpressionType() ==
                       parser::ExpressionType::VALUE_CONSTANT,
                   " For now, we only handle constant default values.");

    // Find the until a version has the required column, use the default value that is closest to the tuple version
    for (layout_version_t start_version = tuple_version + 1; start_version <= layout_version; start_version++) {
      auto curr_version = tables_.at(start_version);
      // Not found in this version
      if (curr_version.default_value_map_.find(it.second) == curr_version.default_value_map_.end()) continue;

      // Found the default value at curr_version
      auto default_const = curr_version.default_value_map_.at(it.second)
                               .CastManagedPointerTo<const parser::ConstantValueExpression>()
                               ->GetValue();
      auto value_size = curr_version.schema_->GetColumn(it.second).AttrSize();

      // zero out the temporary buffer before peeking
      byte output[value_size];
      memset(output, 0, value_size);

      if (type::TransientValuePeeker::PeekValue(default_const, &(output[0]))) {
        StorageUtil::CopyWithNullCheck(output, out_buffer, curr_version.schema_->GetColumn(it.second).AttrSize(),
                                       it.first);
        out_buffer->SetNotNull(it.first);
        break;
      }
    }
  }
}

template void SqlTable::FillMissingColumns<ProjectedRow>(
    ProjectedRow *out_buffer, const std::vector<std::pair<size_t, catalog::col_oid_t>> &missingl_cols,
    layout_version_t tuple_version, layout_version_t layout_version) const;
template void SqlTable::FillMissingColumns<ProjectedColumns::RowView>(
    ProjectedColumns::RowView *out_buffer, const std::vector<std::pair<size_t, catalog::col_oid_t>> &missingl_cols,
    layout_version_t tuple_version, layout_version_t layout_version) const;

template <class RowType>
std::vector<std::pair<size_t, catalog::col_oid_t>> SqlTable::AlignHeaderToVersion(
    RowType *const out_buffer, const DataTableVersion &tuple_version, const DataTableVersion &desired_version,
    col_id_t *cached_orig_header, AttrSizeMap *const size_map) const {
  std::vector<std::pair<size_t, catalog::col_oid_t>> missing_col;
  // reserve the original header, aka desired version column ids
  std::memcpy(cached_orig_header, out_buffer->ColumnIds(), sizeof(col_id_t) * out_buffer->NumColumns());

  // map each desired version col_id (preserving order) to tuple version col_id, by matching col_oid
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    auto col_id = out_buffer->ColumnIds()[i];
    TERRIER_ASSERT(col_id != VERSION_POINTER_COLUMN_ID, "Output buffer should not read the version pointer column.");
    TERRIER_ASSERT(desired_version.column_id_to_oid_map_.count(col_id) > 0,
                   "col_id from out_buffer should be in desired_version map");
    catalog::col_oid_t col_oid = desired_version.column_id_to_oid_map_.at(col_id);
    if (tuple_version.column_oid_to_id_map_.count(col_oid) > 0) {
      auto tuple_col_id = tuple_version.column_oid_to_id_map_.at(col_oid);
      out_buffer->ColumnIds()[i] = tuple_col_id;

      // If the physical stored attr has a larger size, we cannot copy the attribute with its size stored in the
      // tupleaccessor, but with explicit smaller size of the desired projectedrow's attribute
      // If the phsyical stored attr has a smaller size, we also need to memset the projected row to be 0 first before
      // copying a smaller attribute from the tupleslot
      auto tuple_attr_size = tuple_version.layout_.AttrSize(tuple_col_id);
      auto pr_attr_size = desired_version.layout_.AttrSize(col_id);
      if (tuple_attr_size != pr_attr_size) {
        size_map->insert({tuple_col_id, pr_attr_size});
      }
    } else {
      // oid is not represented in datatable with tuple version, so put a placeholder in outbuffer
      missing_col.emplace_back(i, col_oid);
      out_buffer->ColumnIds()[i] = IGNORE_COLUMN_ID;
    }
  }
  return missing_col;
}

template std::vector<std::pair<size_t, catalog::col_oid_t>> SqlTable::AlignHeaderToVersion<ProjectedRow>(
    ProjectedRow *const out_buffer, const DataTableVersion &tuple_version, const DataTableVersion &desired_version,
    col_id_t *cached_orig_header, AttrSizeMap *const size_map) const;
template std::vector<std::pair<size_t, catalog::col_oid_t>> SqlTable::AlignHeaderToVersion<ProjectedColumns::RowView>(
    ProjectedColumns::RowView *const out_buffer, const DataTableVersion &tuple_version,
    const DataTableVersion &desired_version, col_id_t *cached_orig_header, AttrSizeMap *const size_map) const;

bool SqlTable::CreateTable(common::ManagedPointer<const catalog::Schema> schema, layout_version_t version) {
  auto curr_num = ++num_versions_;
  if (curr_num >= MAX_NUM_VERSIONS) {
    num_versions_.store(MAX_NUM_VERSIONS);
    return false;
  }

  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema->GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema->GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema->GetColumns()) {
    auto default_value = column.StoredExpression();
    switch (column.AttrSize()) {
      case VARLEN_COLUMN:
        if (default_value != nullptr) tables_[curr_num - 1].default_value_map_[column.Oid()] = default_value;
        tables_[curr_num - 1].column_id_to_oid_map_[col_id_t(offsets[0])] = column.Oid();
        tables_[curr_num - 1].column_oid_to_id_map_[column.Oid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        if (default_value != nullptr) tables_[curr_num - 1].default_value_map_[column.Oid()] = default_value;
        tables_[curr_num - 1].column_id_to_oid_map_[col_id_t(offsets[1])] = column.Oid();
        tables_[curr_num - 1].column_oid_to_id_map_[column.Oid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        if (default_value != nullptr) tables_[curr_num - 1].default_value_map_[column.Oid()] = default_value;
        tables_[curr_num - 1].column_id_to_oid_map_[col_id_t(offsets[2])] = column.Oid();
        tables_[curr_num - 1].column_oid_to_id_map_[column.Oid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        if (default_value != nullptr) tables_[curr_num - 1].default_value_map_[column.Oid()] = default_value;
        tables_[curr_num - 1].column_id_to_oid_map_[col_id_t(offsets[3])] = column.Oid();
        tables_[curr_num - 1].column_oid_to_id_map_[column.Oid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        if (default_value != nullptr) tables_[curr_num - 1].default_value_map_[column.Oid()] = default_value;
        tables_[curr_num - 1].column_id_to_oid_map_[col_id_t(offsets[4])] = column.Oid();
        tables_[curr_num - 1].column_oid_to_id_map_[column.Oid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  tables_[curr_num - 1].layout_ = storage::BlockLayout(attr_sizes);
  tables_[curr_num - 1].schema_ = schema;
  tables_[curr_num - 1].data_table_ = new DataTable(block_store_, tables_[curr_num - 1].layout_, version);
  return true;
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                              layout_version_t layout_version) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(layout_version).column_oid_to_id_map_.count(col_oid) > 0,
                   "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(layout_version).column_oid_to_id_map_.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

ProjectionMap SqlTable::ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                             layout_version_t layout_version) {
  // Resolve OIDs to storage IDs
  //  auto col_ids = ColIdsForOids(col_oids);

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<col_id_t, catalog::col_oid_t> inverse_map;
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(layout_version).column_oid_to_id_map_.count(col_oid) > 0,
                   "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(layout_version).column_oid_to_id_map_.at(col_oid);
    inverse_map[col_id] = col_oid;
  }

  // Populate the projection map with oids using the in-order iterator on std::map
  ProjectionMap projection_map;
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map[iter.second] = i++;

  return projection_map;
}

catalog::col_oid_t SqlTable::OidForColId(const col_id_t col_id, layout_version_t layout_version) const {
  return tables_.at(layout_version).column_id_to_oid_map_.at(col_id);
}

}  // namespace terrier::storage
