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

static std::unique_ptr<catalog::Schema> ChangeColType(const catalog::Schema &schema, catalog::col_oid_t oid, type::TypeId type_id) {
  auto columns = schema.GetColumns();
  size_t i = 0;
  for (; i < columns.size(); i++) {
    if (columns[i].Oid() == oid) {
      StorageTestUtil::SetType(&columns[i], type_id);
      break;
    }
  }

  return std::make_unique<catalog::Schema>(columns);
}

static std::unique_ptr<catalog::Schema> AddColumn(const catalog::Schema &schema, catalog::Schema::Column *column) {
  std::vector<catalog::Schema::Column> new_columns(schema.GetColumns());
  std::vector<catalog::col_oid_t> oids;
  oids.reserve(new_columns.size() + 1);
  catalog::col_oid_t next_oid = new_columns.begin()->Oid();
  for (auto &col : new_columns) {
    if (col.Oid() > next_oid) {
      next_oid = col.Oid();
    }
  }
  next_oid = next_oid + 1;
  StorageTestUtil::SetOid(column, next_oid);
  new_columns.push_back(*column);
  return std::make_unique<catalog::Schema>(new_columns);
}

// static std::unique_ptr<catalog::Schema> DropColumn(const catalog::Schema &schema, size_t col_idx,
//                                                    catalog::col_oid_t *oidp) {
//   auto columns = schema.GetColumns();
//   *oidp = columns[col_idx].Oid();
//   columns.erase(columns.begin() + col_idx);
//   return std::make_unique<catalog::Schema>(columns);
// }

static std::unique_ptr<catalog::Schema> DropColumn(const catalog::Schema &schema, const catalog::col_oid_t oid) {
  auto columns = schema.GetColumns();
  size_t i = 0;
  for (; i < columns.size(); i++) {
    if (columns[i].Oid() == oid) {
      break;
    }
  }
  TERRIER_ASSERT(i != columns.size(), "column to drop not found in the schema");
  columns.erase(columns.begin() + i);
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
        // just need to fill some random values here. Ideallly we want to do this while we populate the projectedrow,
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
    schemas_.insert(std::make_pair(layout_version, std::move(schema)));
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

  // Schema Update with column added
  storage::layout_version_t new_version(1);
  txn_ts++;
  catalog::Schema::Column col("new_col", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(1)));
  auto new_schema = AddColumn(test_table.GetSchema(version), &col);
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
      add_cols.insert(col.Oid());
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
      add_cols.insert(col.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(new_version), &stored, test_table.GetProjectionMapForOids(new_version), add_cols,
        drop_cols));
  }
  delete[] buffer;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, AddDropColumn) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  // Update the schema
  storage::layout_version_t version(0);

  // Inserted some
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  storage::layout_version_t new_version(1);
  int32_t default_int = 15719;
  catalog::Schema::Column col("new_col", type::TypeId::INTEGER, false,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_int)));
  auto new_schema = AddColumn(test_table.GetSchema(version), &col);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), new_version);

  // Check the default values of those selected
  byte default_value[type::TypeUtil::GetTypeSize(type::TypeId::INTEGER)];
  memcpy(default_value, &default_int, type::TypeUtil::GetTypeSize(type::TypeId::INTEGER));

  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    // Check added column default value
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(new_version),
                                                           test_table.GetBlockLayout(new_version), {col.Oid()},
                                                           {&default_value[0]}));

    // Check tuple equality
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), {col.Oid()}, {}));
  }

  // Drop a column
  txn_ts++;
  storage::layout_version_t vers2(2);
  new_schema = DropColumn(test_table.GetSchema(new_version), col.Oid());
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), vers2);

  // Select check if the column is dropped
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsNone(stored, test_table.GetProjectionMapForOids(vers2),
                                                          test_table.GetBlockLayout(vers2), {col.Oid()}));

    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(vers2), stored,
        test_table.GetProjectionMapForOids(vers2), {}, {}));
  }
}




// NOLINTNEXTLINE
TEST_F(SqlTableTests, ChangeIntType) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  storage::layout_version_t vers1(1);
  int8_t default_tiny_int = 15;
  catalog::Schema::Column col("new_col", type::TypeId::TINYINT, true,
                              parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(default_tiny_int)));
  auto schema1 = AddColumn(test_table.GetSchema(storage::layout_version_t{0}), &col);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(schema1), vers1);

  // Insert with new schema
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, vers1);
  }

  // Update the schema by changeing the col type
  byte smallint_val[type::TypeUtil::GetTypeSize(type::TypeId::SMALLINT)];
  int16_t default_smallint = int16_t(default_tiny_int);
  memcpy(smallint_val, &default_smallint, type::TypeUtil::GetTypeSize(type::TypeId::SMALLINT));
  auto vers2 = vers1+1;
  auto schema2 = ChangeColType(test_table.GetSchema(vers1), col.Oid(), type::TypeId::SMALLINT);
  txn_ts++;
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(schema2), vers2);

  // Select to check that changed column
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    // Check added column default value
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(vers2),
                                                           test_table.GetBlockLayout(vers2), {col.Oid()},
                                                           {&smallint_val[0]}));
  }
}


}  // namespace terrier::storage
