#include "execution/sql/ddl_executors.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "main/db_main.h"
#include "planner/plannodes/alter_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "test_util/catalog_test_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::execution::sql::test {

class DDLExecutorsTests : public TerrierTest {
 public:
  void SetUp() override {
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));
    table_schema_ = std::make_unique<catalog::Schema>(std::vector<catalog::Schema::Column>{col});

    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    index_schema_ =
        std::make_unique<catalog::IndexSchema>(keycols, storage::index::IndexType::BWTREE, true, true, false, true);

    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);
  }

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;

  catalog::db_oid_t db_;

  std::unique_ptr<catalog::Schema> table_schema_;
  std::unique_ptr<catalog::IndexSchema> index_schema_;

  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
};

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNode) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto db_oid = accessor_->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNodeNameConflict) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto db_oid = accessor_->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNode) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto ns_oid = accessor_->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNodeNameConflict) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto ns_oid = accessor_->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNode) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeAbort) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeTableNameConflict) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKey) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo_pkey";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKeyAbort) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo_pkey";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKeyNameConflict) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_EQ(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNode) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeAbort) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeIndexNameConflict) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, DropTablePlanNode) {
  planner::CreateTablePlanNode::Builder create_builder;
  auto create_table_node = create_builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);

  planner::DropTablePlanNode::Builder drop_builder;
  auto drop_table_node = drop_builder.SetTableOid(table_oid).Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::DropTableExecutor(
      common::ManagedPointer<planner::DropTablePlanNode>(drop_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));

  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, AlterTablePlanNode) {
  // TODO(SC): alter table command tests moving to another test file?

  // Create table
  planner::CreateTablePlanNode::Builder builder;
  catalog::Schema original_schema(table_schema_->GetColumns());
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size());
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Add a column
  txn_ = txn_manager_->BeginTransaction();
  accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);

  planner::AlterPlanNode::Builder alter_builder;
  auto default_val = parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(15712));
  catalog::Schema::Column col("new_column", type::TypeId::INTEGER, false, default_val);
  std::vector<std::unique_ptr<planner::AlterCmdBase>> cmds;
  auto add_cmd = std::make_unique<planner::AlterPlanNode::AddColumnCmd>(std::move(col), nullptr, nullptr, nullptr);
  cmds.push_back(std::move(add_cmd));
  auto alter_table_node = alter_builder.SetTableOid(table_oid)
                              .SetCommands(std::move(cmds))
                              .SetColumnOIDs({catalog::INVALID_COLUMN_OID})
                              .Build();
  EXPECT_TRUE(
      execution::sql::DDLExecutors::AlterTableExecutor(common::ManagedPointer<planner::AlterPlanNode>(alter_table_node),
                                                       common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));

  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size() + 1);
  auto &cur_schema = accessor_->GetSchema(table_oid);
  EXPECT_EQ(cur_schema.GetColumns().size(), original_schema.GetColumns().size() + 1);
  EXPECT_NO_THROW(cur_schema.GetColumn("new_column"));
  auto &new_col = cur_schema.GetColumn("new_column");
  EXPECT_FALSE(new_col.Nullable());
  auto stored_default_val = new_col.StoredExpression();
  EXPECT_EQ(stored_default_val->GetReturnValueType(), type::TypeId::INTEGER);
  auto val = stored_default_val.CastManagedPointerTo<const parser::ConstantValueExpression>()->GetValue();
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(val), 15712);

  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop a column
  txn_ = txn_manager_->BeginTransaction();
  accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);

  std::vector<std::unique_ptr<planner::AlterCmdBase>> cmds2;
  auto col_id = new_col.Oid();
  EXPECT_NE(col_id, catalog::INVALID_COLUMN_OID);
  auto drop_cmd = std::make_unique<planner::AlterPlanNode::DropColumnCmd>("new_column", false, false, col_id);
  cmds2.push_back(std::move(drop_cmd));
  auto alter_table_node_2 =
      alter_builder.SetTableOid(table_oid).SetCommands(std::move(cmds2)).SetColumnOIDs({col_id}).Build();

  EXPECT_TRUE(execution::sql::DDLExecutors::AlterTableExecutor(
      common::ManagedPointer<planner::AlterPlanNode>(alter_table_node_2),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size());
  auto schema_after_drop = accessor_->GetSchema(table_oid);
  EXPECT_EQ(schema_after_drop.GetColumns().size(), original_schema.GetColumns().size());
  auto cols = schema_after_drop.GetColumns();
  for (auto const &c : cols) {
    EXPECT_NE(c.Oid(), col_id);
    EXPECT_NE(c.Name(), "new_column");
  }
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, ConcurrentAlterTablePlanNode) {
  // Create table
  planner::CreateTablePlanNode::Builder builder;
  catalog::Schema original_schema(table_schema_->GetColumns());
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size());
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<std::unique_ptr<catalog::CatalogAccessor>> accessors;
  std::vector<std::unique_ptr<planner::AlterPlanNode>> alter_table_nodes;
  std::vector<transaction::TransactionContext *> txns;
  planner::AlterPlanNode::Builder alter_builder;
  // run 3 update schema transactions concurrently
  int num_threads = 3;

  auto default_val = parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(15712));
  std::vector<std::string> col_names{"new_column1", "new_column2", "new_column3"};
  std::vector<catalog::Schema::Column> cols;

  // each thread tries to adds a new column to the original schema
  for (int i = 0; i < num_threads; i++) {
    txn_ = txn_manager_->BeginTransaction();
    accessors.push_back(catalog_->GetAccessor(common::ManagedPointer(txn_), db_));
    txns.push_back(txn_);

    catalog::Schema::Column col(col_names[i], type::TypeId::INTEGER, false, default_val);
    cols.push_back(col);

    std::vector<std::unique_ptr<planner::AlterCmdBase>> cmds;
    auto add_cmd = std::make_unique<planner::AlterPlanNode::AddColumnCmd>(std::move(col), nullptr, nullptr, nullptr);
    cmds.push_back(std::move(add_cmd));
    alter_table_nodes.push_back(alter_builder.SetTableOid(table_oid)
                                    .SetCommands(std::move(cmds))
                                    .SetColumnOIDs({catalog::INVALID_COLUMN_OID})
                                    .Build());
  }

  int orig_value = -1;
  std::atomic<int> thread_that_succeeds = orig_value;

  common::WorkerPool thread_pool(num_threads, {});

  // concurrent perform altertable with several transactions, each adding a column. Note that only one transaction will
  // succeed while the others will fail
  auto workload = [&](uint32_t thread_id) {
    bool res = execution::sql::DDLExecutors::AlterTableExecutor(
        common::ManagedPointer<planner::AlterPlanNode>(alter_table_nodes[thread_id]),
        common::ManagedPointer<catalog::CatalogAccessor>(accessors[thread_id]));
    if (res) thread_that_succeeds.compare_exchange_strong(orig_value, thread_id);
  };
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

  std::string new_col_name = col_names[thread_that_succeeds];
  EXPECT_TRUE(thread_that_succeeds != orig_value);

  catalog::Schema::Column new_col;

  for (int i = 0; i < num_threads; i++) {
    if (i != thread_that_succeeds) {
      EXPECT_EQ(accessors[i]->GetColumns(table_oid).size(), original_schema.GetColumns().size());
      auto &cur_schema = accessors[i]->GetSchema(table_oid);
      EXPECT_EQ(cur_schema.GetColumns().size(), original_schema.GetColumns().size());
      txn_manager_->Abort(txns[i]);
    } else {
      EXPECT_EQ(accessors[i]->GetColumns(table_oid).size(), original_schema.GetColumns().size() + 1);
      auto &cur_schema = accessors[i]->GetSchema(table_oid);
      EXPECT_EQ(cur_schema.GetColumns().size(), original_schema.GetColumns().size() + 1);
      EXPECT_NO_THROW(cur_schema.GetColumn(col_names[i]));
      new_col = cur_schema.GetColumn(col_names[i]);
      EXPECT_FALSE(new_col.Nullable());
      auto stored_default_val = new_col.StoredExpression();
      EXPECT_EQ(stored_default_val->GetReturnValueType(), type::TypeId::INTEGER);
      auto val = stored_default_val.CastManagedPointerTo<const parser::ConstantValueExpression>()->GetValue();
      EXPECT_EQ(type::TransientValuePeeker::PeekInteger(val), 15712);
      txn_manager_->Commit(txns[i], transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  // Drop the column added by the successful transaction
  txn_ = txn_manager_->BeginTransaction();
  accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);

  std::vector<std::unique_ptr<planner::AlterCmdBase>> cmds2;
  auto col_id = new_col.Oid();
  EXPECT_NE(col_id, catalog::INVALID_COLUMN_OID);
  auto drop_cmd = std::make_unique<planner::AlterPlanNode::DropColumnCmd>(new_col_name, false, false, col_id);
  cmds2.push_back(std::move(drop_cmd));
  auto alter_table_node_2 =
      alter_builder.SetTableOid(table_oid).SetCommands(std::move(cmds2)).SetColumnOIDs({col_id}).Build();

  EXPECT_TRUE(execution::sql::DDLExecutors::AlterTableExecutor(
      common::ManagedPointer<planner::AlterPlanNode>(alter_table_node_2),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size());
  auto schema_after_drop = accessor_->GetSchema(table_oid);
  EXPECT_EQ(schema_after_drop.GetColumns().size(), original_schema.GetColumns().size());

  for (auto const &c : schema_after_drop.GetColumns()) {
    EXPECT_NE(c.Oid(), col_id);
    EXPECT_NE(c.Name(), new_col_name);
  }
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::execution::sql::test
