#include "storage/sql_table.h"

#include <cstring>
#include <vector>

#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

class SqlTableTests : public TerrierTest {
 public:
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
  void SetUp() override {}
  void TearDown() override {}
};

// drop the columns with oids in the set drop_oids. And add the new columns to the end of columns in schema
static std::unique_ptr<catalog::Schema> AddDropColumns(const catalog::Schema &schema,
    const std::unordered_set<catalog::col_oid_t> drop_oids, std::vector<catalog::Schema::Column*>& new_columns) {
  auto old_columns = schema.GetColumns();
  std::vector<catalog::Schema::Column> columns;

  catalog::col_oid_t max_oid = old_columns.begin()->Oid();
  for (auto &col : old_columns) {
    if (col.Oid() > max_oid) {
      max_oid = col.Oid();
    }
    // only include cols that are not dropped
    if (drop_oids.find(col.Oid()) == drop_oids.end()) {
      columns.push_back(col);
    }
  }

  // add the new columns, set their oids to be larger than all existing oids
  for (int i = 0; i < new_columns.size(); i++) {
    catalog::col_oid_t new_oid = max_oid + 1 + i;
    StorageTestUtil::SetOid(new_columns[i], new_oid);
    columns.push_back(*new_columns[i]);
  }
  return std::make_unique<catalog::Schema>(columns);
}

class RandomSqlTableTestObject {
 public:
  struct TupleVersion {
    transaction::timestamp_t ts_;
    storage::ProjectedRow *pr_;
    storage::layout_version_t version_;
  };

  template <class Random>
  RandomSqlTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator,
                           double null_bias)
      : null_bias_(null_bias) {
    auto schema = StorageTestUtil::RandomSchemaNoVarlen(max_col, generator);
    table_ = std::make_unique<storage::SqlTable>(common::ManagedPointer<storage::BlockStore>(block_store), *schema);
    UpdateSchema({nullptr}, std::unique_ptr<catalog::Schema>(schema), storage::layout_version_t(0));
  }

  // TODO(Schema-Change): redos_ seems to be producing problem here.
  //  We should look into this function further
  ~RandomSqlTableTestObject() {
    for (auto &it : buffers_) {
      delete[] it.second;
    }
    for (auto &txn : txns_) {
      txn->redo_buffer_.Finalize(false);
    }
  }

  template <class Random>
  void FillNullValue(storage::ProjectedRow *pr, const catalog::Schema &schema, const ColumnIdToOidMap &col_id_to_oid,
                     const storage::BlockLayout &layout, Random *const generator) {
    // Make sure we have a mix of inlined and non-inlined values
    std::uniform_int_distribution<uint32_t> varlen_size(1, MAX_TEST_VARLEN_SIZE);

    for (uint16_t pr_idx = 0; pr_idx < pr->NumColumns(); pr_idx++) {
      auto col_id = pr->ColumnIds()[pr_idx];
      auto col_oid = col_id_to_oid.at(col_id);
      const auto &schema_col = schema.GetColumn(col_oid);

      if (pr->IsNull(pr_idx) && !schema_col.Nullable()) {
        // Ricky (Schema-Change):
        // Some non-nullable columns could be null. But the schema we generated have default values all being null, we
        // just need to fill some random values here. Ideally we want to do this while we populate the projectedrow,
        // but that function has coupled with too many other parts

        if (layout.IsVarlen(col_id)) {
          uint32_t size = varlen_size(*generator);
          if (size > storage::VarlenEntry::InlineThreshold()) {
            byte *varlen = common::AllocationUtil::AllocateAligned(size);
            StorageTestUtil::FillWithRandomBytes(size, varlen, generator);
            // varlen entries always start off not inlined
            *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_idx)) =
                storage::VarlenEntry::Create(varlen, size, true);
          } else {
            byte buf[storage::VarlenEntry::InlineThreshold()];
            StorageTestUtil::FillWithRandomBytes(size, buf, generator);
            *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_idx)) =
                storage::VarlenEntry::CreateInline(buf, size);
          }
        } else {
          StorageTestUtil::FillWithRandomBytes(layout.AttrSize(col_id), pr->AccessForceNotNull(pr_idx), generator);
        }
      }
    }
  }

  template <class Random>
  storage::TupleSlot InsertRandomTuple(const transaction::timestamp_t timestamp, Random *generator,
                                       storage::RecordBufferSegmentPool *buffer_pool,
                                       storage::layout_version_t layout_version) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);

    // generate a random ProjectedRow to Insert
    auto redo_initilizer = pris_.at(layout_version);
    auto *insert_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initilizer);
    auto *insert_tuple = insert_redo->Delta();
    auto layout = table_->GetBlockLayout(layout_version);
    StorageTestUtil::PopulateRandomRow(insert_tuple, layout, null_bias_, generator);

    // Fill up the random bytes for non-nullable columns
    FillNullValue(insert_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                  table_->GetBlockLayout(layout_version), generator);

    redos_.emplace_back(insert_redo);
    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(txn), insert_redo, layout_version);
    inserted_slots_.push_back(slot);
    tuple_versions_[slot].push_back({timestamp, insert_tuple, layout_version});

    return slot;
  }

  storage::ProjectedColumns *AllocateColumnBuffer(const storage::layout_version_t version, byte **bufferp,
                                                  size_t size) {
    auto old_layout = GetBlockLayout(version);
    storage::ProjectedColumnsInitializer initializer(old_layout, StorageTestUtil::ProjectionListAllColumns(old_layout),
                                                     size);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns = initializer.Initialize(buffer);
    *bufferp = buffer;
    return columns;
  }

  TupleVersion GetReferenceVersionedTuple(const storage::TupleSlot slot, const transaction::timestamp_t timestamp) {
    TERRIER_ASSERT(tuple_versions_.find(slot) != tuple_versions_.end(), "Slot not found.");
    auto &versions = tuple_versions_[slot];
    // search backwards so the first entry with smaller timestamp can be returned
    for (auto i = static_cast<int64_t>(versions.size() - 1); i >= 0; i--) {
      if (transaction::TransactionUtil::NewerThan(timestamp, versions[i].ts_) || timestamp == versions[i].ts_)
        return versions[i];
    }
    return {transaction::timestamp_t{transaction::INVALID_TXN_TIMESTAMP}, nullptr, storage::layout_version_t{0}};
  }

  storage::ProjectedRow *Select(const storage::TupleSlot slot, const transaction::timestamp_t timestamp,
                                storage::RecordBufferSegmentPool *buffer_pool,
                                storage::layout_version_t layout_version) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);

    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = pris_.at(layout_version).InitializeRow(buffers_.at(layout_version));
    table_->Select(common::ManagedPointer(txn), slot, select_row, layout_version);
    return select_row;
  }

  void UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn,
                    std::unique_ptr<catalog::Schema> schema, const storage::layout_version_t layout_version) {
    if (txn != nullptr) table_->UpdateSchema(txn, *schema, layout_version);

    auto columns = schema->GetColumns();
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(columns.size());
    for (auto &col : columns) {
      oids.push_back(col.Oid());
    }
    auto pri = table_->InitializerForProjectedRow(oids, layout_version);
    schemas_.insert(std::make_pair(layout_version, std::unique_ptr<catalog::Schema>(std::move(schema))));
    pris_.insert(std::make_pair(layout_version, pri));
    buffers_.insert(std::make_pair(layout_version, common::AllocationUtil::AllocateAligned(pri.ProjectedRowSize())));
  }

  common::ManagedPointer<transaction::TransactionContext> NewTransaction(
      transaction::timestamp_t timestamp, storage::RecordBufferSegmentPool *buffer_pool) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);
    return common::ManagedPointer<transaction::TransactionContext>(txn);
  }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }

  storage::BlockLayout GetBlockLayout(storage::layout_version_t version) const {
    return table_->GetBlockLayout(version);
  }

  const catalog::Schema &GetSchema(storage::layout_version_t version) const { return *schemas_.at(version); }
  const storage::SqlTable &GetTable() const { return *table_; }
  storage::SqlTable &GetTable() { return *table_; }

  storage::ProjectionMap GetProjectionMapForOids(storage::layout_version_t version) {
    auto &schema = schemas_.at(version);
    auto columns = schema->GetColumns();
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(columns.size());
    for (auto &col : columns) oids.push_back(col.Oid());
    return table_->ProjectionMapForOids(oids, version);
  }

 private:
  std::unique_ptr<storage::SqlTable> table_;
  double null_bias_;
  std::unordered_map<storage::layout_version_t, storage::ProjectedRowInitializer> pris_;
  std::unordered_map<storage::layout_version_t, byte *> buffers_;
  std::vector<common::ManagedPointer<storage::RedoRecord>> redos_;
  std::vector<std::unique_ptr<transaction::TransactionContext>> txns_;
  std::vector<storage::TupleSlot> inserted_slots_;
  // oldest to newest
  std::unordered_map<storage::TupleSlot, std::vector<TupleVersion>> tuple_versions_;
  std::unordered_map<storage::layout_version_t, std::unique_ptr<catalog::Schema>> schemas_;
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SimpleInsertSelect) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 100;

  storage::layout_version_t version(0);

  // Insert into SqlTable
  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());

  // Compare each inserted
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(1), &buffer_pool_, version);
    auto ref = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), stored, test_table.GetProjectionMapForOids(version), {}, {}));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, InsertWithSchemaChange) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  storage::layout_version_t version(0);

  // Insert first half into SqlTable
  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  for (uint16_t i = 0; i < num_inserts / 2; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts / 2, test_table.InsertedTuples().size());

  // Schema Update: drop the first column, and add 2 new columns to the end
  storage::layout_version_t new_version(1);
  txn_ts++;
  catalog::Schema::Column col1("new_col1", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(1)));
  catalog::Schema::Column col2("new_col2", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(2)));
  std::vector<catalog::Schema::Column*> cols{&col1, &col2};

  catalog::Schema::Column col_to_drop = test_table.GetSchema(version).GetColumns()[0];
  std::unordered_set<catalog::col_oid_t> drop_oids{col_to_drop.Oid()};

  auto new_schema = AddDropColumns(test_table.GetSchema(version), drop_oids, cols);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), new_version);

  // Insert the second half with new version
  txn_ts++;
  for (uint16_t i = num_inserts / 2; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  // Compare each inserted by selecting as the new version
  txn_ts++;
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    std::unordered_set<catalog::col_oid_t> add_cols;
    std::unordered_set<catalog::col_oid_t> drop_cols;
    if (tuple_version.version_ != new_version) {
      for (auto col: cols) {
        add_cols.insert(col -> Oid());
      }
      drop_cols.insert(col_to_drop.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), add_cols, drop_cols));
  }

  // This txn should not observe the updated schema
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    if (tuple_version.version_ != new_version) {
      // Select the tuple with its tuple version
      storage::ProjectedRow *stored =
          test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, tuple_version.version_);
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(test_table.GetBlockLayout(tuple_version.version_), stored,
                                                              tuple_version.pr_));
    }
  }

  // Scan the table with version 0, seeing only half of the tuples
  byte *buffer = nullptr;
  auto columns = test_table.AllocateColumnBuffer(version, &buffer, num_inserts / 2);
  auto it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             version);
  EXPECT_EQ(num_inserts / 2, columns->NumTuples());
  EXPECT_EQ(it, test_table.GetTable().end(version));
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), &stored, test_table.GetProjectionMapForOids(version), {}, {}));
  }
  delete[] buffer;

  // Scan the table with the newest version, seeing all tuples
  buffer = nullptr;
  columns = test_table.AllocateColumnBuffer(new_version, &buffer, num_inserts);
  it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             new_version);
  EXPECT_EQ(num_inserts, columns->NumTuples());
  EXPECT_EQ(it, test_table.GetTable().end(new_version));
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    std::unordered_set<catalog::col_oid_t> add_cols;
    std::unordered_set<catalog::col_oid_t> drop_cols;
    if (ref.version_ != new_version) {
      for (auto col: cols) {
        add_cols.insert(col -> Oid());
      }
      drop_cols.insert(col_to_drop.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(new_version), &stored, test_table.GetProjectionMapForOids(new_version), add_cols,
        drop_cols));
  }
  delete[] buffer;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, AddThenDropColumns) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  // Update the schema
  storage::layout_version_t version(0);

  // Insert some tuples
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  storage::layout_version_t new_version(1);

  // We will check the default values of those selected. For now, only test Integer default values
  int32_t default_int = 15719;
  std::vector<int> default_values;
  int num_new_cols = 3;
  for (int i = 0; i < num_new_cols; i++) {
    default_values.push_back(default_int + i);
  }
  catalog::Schema::Column col1("new_col1", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[0])));
  catalog::Schema::Column col2("new_col2", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[1])));
  catalog::Schema::Column col3("new_col3", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[2])));
  std::vector<catalog::Schema::Column*> cols{&col1, &col2, &col3};
  std::unordered_set<catalog::col_oid_t> drop_oids;
  auto new_schema = AddDropColumns(test_table.GetSchema(version), drop_oids, cols);

  std::vector<catalog::col_oid_t> oids;
  for (auto col_ptr : cols) {
    oids.push_back(col_ptr->Oid());
  }
  std::unordered_set<catalog::col_oid_t> oids_set(oids.begin(), oids.end());

  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), new_version);

  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    // Check added column default value
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(new_version),
                                                           test_table.GetBlockLayout(new_version), oids,
                                                           default_values));
    // Check tuple equality
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), oids_set, {}));
  }

  // Drop a column
  txn_ts++;
  storage::layout_version_t vers2(2);

  std::vector<catalog::Schema::Column*> new_cols;
  new_schema = AddDropColumns(test_table.GetSchema(new_version), oids_set, new_cols);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), vers2);

  // Select check if the column is dropped
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsNone(stored, test_table.GetProjectionMapForOids(vers2),
                                                          test_table.GetBlockLayout(vers2), oids));

    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(vers2), stored,
        test_table.GetProjectionMapForOids(vers2), {}, {}));
  }
}


}  // namespace terrier::storage
