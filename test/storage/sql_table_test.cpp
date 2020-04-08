#include "storage/sql_table.h"

#include <cstring>
#include <vector>

#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace terrier {

struct SqlTableTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};


static std::unique_ptr<catalog::Schema> AddColumn(const catalog::Schema &schema, catalog::Schema::Column column) {
  std::vector<catalog::Schema::Column> new_columns(schema.GetColumns());
  std::vector<catalog::col_oid_t> oids;
  oids.reserve(new_columns.size() + 1);
  catalog::col_oid_t next_oid = new_columns.begin()->Oid();
  for(auto &col: new_columns) {
    if (col.Oid() > next_oid) {
      next_oid = col.Oid();
    }
  }
  next_oid = next_oid + 1;
  StorageTestUtil::SetOid(column, next_oid);
  new_columns.push_back(column);
  return std::unique_ptr<catalog::Schema>(new catalog::Schema(new_columns));
}

class RandomSqlTableTestObject {
 public:
  struct tuple_version {
    transaction::timestamp_t ts_;
    storage::ProjectedRow * pr_;
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

  ~RandomSqlTableTestObject() {
    // redos_ seems to be producing problem here
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

    redos_.emplace_back(insert_redo);
    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(txn), insert_redo);
    inserted_slots_.push_back(slot);
    tuple_versions_[slot].push_back({timestamp, insert_tuple, layout_version});

    return slot;
  }

  tuple_version GetReferenceVersionedTuple(const storage::TupleSlot slot,
                                                          const transaction::timestamp_t timestamp) {
    TERRIER_ASSERT(tuple_versions_.find(slot) != tuple_versions_.end(), "Slot not found.");
    auto &versions = tuple_versions_[slot];
    // search backwards so the first entry with smaller timestamp can be returned
    for (auto i = static_cast<int64_t>(versions.size() - 1); i >= 0; i--)
      if (transaction::TransactionUtil::NewerThan(timestamp, versions[i].ts_) || timestamp == versions[i].ts_)
        return versions[i];
    return {transaction::timestamp_t{transaction::INVALID_TXN_TIMESTAMP},nullptr,
            storage::layout_version_t {0}};
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

  void UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<catalog::Schema> schema, const storage::layout_version_t layout_version) {
    if(txn != nullptr) table_->UpdateSchema(txn, *schema, layout_version);

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

  common::ManagedPointer<transaction::TransactionContext> NewTransaction(transaction::timestamp_t timestamp,
      storage::RecordBufferSegmentPool *buffer_pool) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);
    return common::ManagedPointer<transaction::TransactionContext>(txn);
  }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }

  storage::BlockLayout GetBlockLayout(storage::layout_version_t version) const {
    return table_->GetBlockLayout(version);
  }

  const catalog::Schema &GetSchema(storage::layout_version_t version) const {
    return *schemas_.at(version);
  }

  storage::ProjectionMap GetProjectionMapForOids(storage::layout_version_t version) {
    auto &schema = schemas_.at(version);
    auto columns = schema->GetColumns();
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(columns.size());
    for(auto &col : columns) oids.push_back(col.Oid());
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
  std::unordered_map<storage::TupleSlot, std::vector<tuple_version>> tuple_versions_;
  std::unordered_map<storage::layout_version_t , std::unique_ptr<catalog::Schema>> schemas_;
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
    auto *ref =
        test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1)).pr_;

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(test_table.GetBlockLayout(version), stored, ref));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, DISABLED_SimpleAddColumnTest) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 100;
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
  auto new_schema = AddColumn(test_table.GetSchema(version), col);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts},
                                                    &buffer_pool_), std::move(new_schema), new_version);

  // Insert the second half with new version
  txn_ts++;
  for (uint16_t i = num_inserts / 2 ; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version);
  }


  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  // Compare each inserted
  txn_ts++;
  std::unordered_set<catalog::col_oid_t> add_cols;
  std::unordered_set<catalog::col_oid_t> drop_cols{col.Oid()};
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, version);
    auto tuple_version =
        test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    //TODO(Schema-change): we need to find a way to compare these 2 projected rows with different schema/blocklayout with
    // other schema changes. e.g: change column type, add constrain.
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_, test_table.GetProjectionMapForOids(tuple_version.version_),
        test_table.GetBlockLayout(version), stored, test_table.GetProjectionMapForOids(version),
        add_cols, drop_cols));
  }
}


}  // namespace terrier
