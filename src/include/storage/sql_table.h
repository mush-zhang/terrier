#pragma once
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/data_table.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_context.h"

namespace terrier {
// Forward Declaration
class LargeSqlTableTestObject;
class RandomSqlTableTransaction;
}  // namespace terrier

namespace terrier::storage {

/**
 * A SqlTable is a thin layer above DataTable that replaces storage layer concepts like BlockLayout with SQL layer
 * concepts like Schema. The goal is to hide concepts like col_id_t and BlockLayout above the SqlTable level.
 * The SqlTable API should only refer to storage concepts via things like Schema and col_oid_t, and then perform the
 * translation to BlockLayout and col_id_t to talk to the DataTable and other areas of the storage layer.
 */
class SqlTable {
  /**
   * Contains all of the metadata the SqlTable needs to reference a DataTable. We shouldn't ever have to expose these
   * concepts to anyone above the SqlTable level. If you find yourself wanting to return BlockLayout or col_id_t above
   * this layer, consider alternatives.
   */
  struct DataTableVersion {
    DataTable *data_table_;
    BlockLayout layout_;
    ColumnOidToIdMap column_oid_to_id_map_;
    // TODO(Ling): used in transforming between different versions.
    //  It only works for adding and dropping columns, but not modifying type/constraint/default of the column
    //  Consider storing forward and backward delta of the schema change maybe in the future
    ColumnIdToOidMap column_id_to_oid_map_;
    common::ManagedPointer<const catalog::Schema> schema_;
    DefaultValueMap default_value_map_;
  };

 public:
  /**
   * Constructs a new SqlTable with the given Schema, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param schema the initial Schema of this SqlTable
   */
  SqlTable(common::ManagedPointer<BlockStore> store, const catalog::Schema &schema);

  /**
   * Destructs a SqlTable, frees all its member datatables.
   */
  ~SqlTable() {
    for (const auto &it : tables_) delete it.data_table_;
  }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @param layout_version Schema version the current querying transaction should see
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot, ProjectedRow *out_buffer,
              layout_version_t layout_version = layout_version_t{0}) const;

  /**
   * Update the tuple according to the redo buffer given. StageWrite must have been called as well in order for the
   * operation to be logged.
   *
   * @param txn the calling transaction
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest. The
   * TupleSlot in this RedoRecord must be set to the intended tuple.
   * @param layout_version Schema version the current querying transaction should see
   * @param updated_slot used to return tupleslot T after the update. Set this to null if T is not used by caller.
   * @return true if successful, false otherwise. Populates updated_slot with Tupleslot after the update. This will be
   * different from the original tupleslot if migration occurs.
   */
  bool Update(common::ManagedPointer<transaction::TransactionContext> txn, RedoRecord *redo,
              layout_version_t layout_version = layout_version_t{0}, TupleSlot *updated_slot = nullptr) const;

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple. StageWrite must have been
   * called as well in order for the operation to be logged. Always inserts to datatable with layout_version.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @param layout_version Schema version the current querying transaction should see
   * @return TupleSlot for the inserted tuple
   */
  TupleSlot Insert(common::ManagedPointer<transaction::TransactionContext> txn, RedoRecord *redo,
                   layout_version_t layout_version = layout_version_t{0}) const;

  /**
   * Deletes the given TupleSlot. StageDelete must have been called as well in order for the operation to be logged.
   * Note that we always delete from the datatable containing the tupleslot, regardless of the layout_version
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot) {
    TERRIER_ASSERT(txn->redo_buffer_.LastRecord() != nullptr,
                   "The RedoBuffer is empty even though StageDelete should have been called.");
    TERRIER_ASSERT(
        reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                ->GetUnderlyingRecordBodyAs<DeleteRecord>()
                ->GetTupleSlot() == slot,
        "This Delete is not the most recent entry in the txn's RedoBuffer. Was StageDelete called immediately before?");

    const auto tuple_version = slot.GetBlock()->data_table_->layout_version_;
    const auto result = tables_.at(tuple_version).data_table_->Delete(txn, slot);

    if (!result) {
      // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
      // correctly.
      txn->SetMustAbort();
    }
    return result;
  }

  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot past the
   * last slot scanned in the invocation.
   * @warning a tuple inserted early than start_pos might appear in the scan (by migrating into a later version schema
   * datatable so the ordering of the scanning is not strictly defined here. However, if start_pos is obtained through
   * SqlTable::begin(), the results will always contain the same tuples
   * @param txn the calling transaction
   * @param start_pos iterator to the starting location for the sequential scan.
   * @param out_buffer output buffer. The object should already contain projection list information. This buffer is
   *                   always cleared of old values.
   * @param layout_version Schema version the current querying transaction should see
   */
  void Scan(common::ManagedPointer<transaction::TransactionContext> txn, DataTable::SlotIterator *start_pos,
            ProjectedColumns *out_buffer, layout_version_t layout_version = layout_version_t{0}) const;

  /**
   * Creates a new tableversion given a schema. Conccurent UpdateSchema is synchronized at the Catalog table.
   * Since the catalog table prevents write-write conflict with version pointer, calling UpdateSchema here is always
   * thread-safe.
   * @param txn the calling transaction
   * @param layout_version Version number for the new schema
   * @param schema the updated schema of this SqlTable
   */
  bool UpdateSchema(const common::ManagedPointer<transaction::TransactionContext> txn, const catalog::Schema &schema,
                    const layout_version_t layout_version = layout_version_t{0}) {
    TERRIER_ASSERT(layout_version >= num_versions_, "Input version should be strictly larger than all versions");
    return CreateTable(common::ManagedPointer<const catalog::Schema>(&schema), layout_version);
  }

  // TODO(Schema-Change): Do we retain the begin() and end(), or implement begin and end function with version number?

  /**
   * @return the first tuple slot contained in the first DataTable
   */
  // NOLINTNEXTLINE for STL name compability
  DataTable::SlotIterator begin() const {
    TERRIER_ASSERT(!tables_.empty(), "sqltable should have at least one underlying datatable");
    return tables_.begin()->data_table_->begin();
  }

  /**
   * @return one past the last tuple slot contained in the last DataTable
   */
  // NOLINTNEXTLINE for STL name compability
  DataTable::SlotIterator end() const {
    TERRIER_ASSERT(!tables_.empty(), "sqltable should have at least one underlying datatable");
    return tables_[num_versions_ - 1].data_table_->end();
  }  // NOLINT for STL name compability

  /**
   * @param layout_version the last schema version the querying transaction should be able to see
   * @return one past the last tuple slot contained in the DataTable with layout_version
   */
  // NOLINTNEXTLINE for STL name compability
  DataTable::SlotIterator end(layout_version_t layout_version) const {
    TERRIER_ASSERT(!tables_.empty(), "sqltable should have at least one underlying datatable");
    return tables_.at(layout_version).data_table_->end();
  }  // NOLINT for STL name compability

  // TODO(Schema-Change): add projection considering table
  //  We might have to seperate the use cases here: one implementation that does not expect schema chnage at all, one
  //  does. In many cases, this function is called not in transactional context (thus layout_version not really
  //  relevant). For example, in TPCC, the worker will pre-allocate a buffer with size equal to the projectedrow's size.
  //  For the version that does expect a version change: we can save the col_oids and the reference to the SqlTable in
  //  the ProjRow(Colum)Initlizer, and only later when a transactional context is known, we materialize this initializer
  //  with the correct layout_version
  /**
   * Generates an ProjectedColumnsInitializer for the execution layer to use. This performs the translation from col_oid
   * to col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param max_tuples the maximum number of tuples to store in the ProjectedColumn
   * @param layout_version Version of schema to be projected
   * @return initializer to create ProjectedColumns
   * @warning col_oids must be a set (no repeats)
   */
  ProjectedColumnsInitializer InitializerForProjectedColumns(
      const std::vector<catalog::col_oid_t> &col_oids, const uint32_t max_tuples,
      const layout_version_t layout_version = layout_version_t(0)) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids, layout_version);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    return ProjectedColumnsInitializer(tables_.at(layout_version).layout_, col_ids, max_tuples);
  }

  /**
   * Generates an ProjectedRowInitializer for the execution layer to use. This performs the translation from col_oid to
   * col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param layout_version Version of schema to be projected
   * @return initializer to create ProjectedRow
   * @warning col_oids must be a set (no repeats)
   */
  ProjectedRowInitializer InitializerForProjectedRow(const std::vector<catalog::col_oid_t> &col_oids,
                                                     layout_version_t layout_version = layout_version_t(0)) const {
    // TODO(Schema-Change): this function is called for calculating the optimal layout of the columns
    //  (so they are not stored in logical order).
    //  This should not be using the inside data, but only the layout/column information.
    //  Therefore it should return the initializer of the current intended datatable version
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids, layout_version);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    return ProjectedRowInitializer::Create(tables_.at(layout_version).layout_, col_ids);
  }

  /**
   * Generate a projection map given column oids
   * @param col_oids oids that will be scanned.
   * @param layout_version Version of schema to be projected
   * @return the projection map
   */
  ProjectionMap ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                     layout_version_t layout_version = layout_version_t{0});

  /**
   * @param layout_version  version of the datatable
   * @return the column oid to id map of a layout_version
   */
  const ColumnOidToIdMap &GetColumnOidToIdMap(layout_version_t layout_version) const {
    TERRIER_ASSERT(layout_version < num_versions_, "Version should exist.");
    return tables_.at(layout_version).column_oid_to_id_map_;
  }

  /**
   * @param layout_version  version of the datatable
   * @return the column id to oid map of a layout_version
   */
  const ColumnIdToOidMap &GetColumnIdToOidMap(layout_version_t layout_version) const {
    TERRIER_ASSERT(layout_version < MAX_NUM_VERSIONS && tables_[layout_version].data_table_ != nullptr,
                   "Version does not exist.");
    return tables_.at(layout_version).column_id_to_oid_map_;
  }

  /**
   * Returns the layout version of a datatable.
   * @warning This is only used for testing purpose
   * @param layout_version Version of schema
   * @return
   */
  const BlockLayout &GetBlockLayout(layout_version_t layout_version = layout_version_t{0}) {
    TERRIER_ASSERT(layout_version < MAX_NUM_VERSIONS && tables_[layout_version].data_table_ != nullptr,
                   "Version does not exist.");
    return tables_.at(layout_version).layout_;
  }

 private:
  friend class catalog::DatabaseCatalog;
  friend class RecoveryManager;  // Needs access to OID and ID mappings
  friend class terrier::RandomSqlTableTransaction;
  friend class terrier::LargeSqlTableTestObject;
  friend class RecoveryTests;

  // TODO(Matt): do we need this stashed at this layer? We don't use it.
  const common::ManagedPointer<BlockStore> block_store_;

  // TODO(Schema-Change): add concurrent layout version to dataTable lookup
  //  when layout version is not monotonically increasing from 0;
  //  for example, when we implement garbage collecting empty old datatable, or when we collapse versions
  //  We could potentially used ordered map for traversing data table that are less or equal to curr version
  // Vector of tables with fixed size of MAX_NUM_VERSIONS
  //  We could later see if a unbounded concurrent vector greatly a affect the performance
  std::vector<DataTableVersion> tables_;

  std::atomic<uint8_t> num_versions_ = 0;

  /**
   * Translates out_buffer from desired version col_ids to tuple version col_ids, by mapping each col_id in out_buffer
   * to its matching col_id in tuple version (2 col_ids match if they map to the same col_oid) or IGNORE_COLUMN_ID if
   * no match exists in tuple version.
   * @tparam RowType Type of the output (i.e. projected row, projected column)
   * @param out_buffer a buffer initially with desired version, and translated to tuple version
   * @param tuple_version the old schema version
   * @param desired_version the desired schema version
   * @param cached_orig_header original header cached
   * @param size_map columns which have size mismatch
   * @return the columns in the desired schema version but not in the tuple version.
   */
  template <class RowType>
  std::vector<std::pair<size_t, catalog::col_oid_t>> AlignHeaderToVersion(RowType *out_buffer,
                                                                          const DataTableVersion &tuple_version,
                                                                          const DataTableVersion &desired_version,
                                                                          col_id_t *cached_orig_header,
                                                                          AttrSizeMap *size_map) const;

  /**
   * Fill the missing columns in the out_buffer with default values of those columns in the desired_version
   * @tparam RowType Type of the output (i.e. projected row, projected column)
   * @param out_buffer a buffer that corresponds to an old schema version
   * @param desired_version desired schema version
   */
  template <class RowType>
  void FillMissingColumns(RowType *out_buffer, const std::vector<std::pair<size_t, catalog::col_oid_t>> &missingl_cols,
                          layout_version_t tuple_version, layout_version_t layout_version) const;

  /**
   * Creates a new datatble version given the schema and version number
   * @param schema the initial Schema of this SqlTable
   * @param version Schema version of the created/updated table version
   * @return If the datatable version has been successfully created
   */
  bool CreateTable(common::ManagedPointer<const catalog::Schema> schema, layout_version_t version);

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
   * @param col_oids set of col_oids, they must be in the table's ColumnMap
   * @return vector of col_ids for these col_oids
   */
  std::vector<col_id_t> ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                      layout_version_t layout_version = layout_version_t{0}) const;

  /**
   * Returns the col oid for the given col id
   * @param col_id given col id
   * @return col oid for the provided col id
   */
  catalog::col_oid_t OidForColId(col_id_t col_id, layout_version_t layout_version = layout_version_t{0}) const;
};
}  // namespace terrier::storage
