#include "execution/sql/alter_executors.h"

#include "catalog/catalog_accessor.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {
bool AlterTableCmdExecutor::AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                      common::ManagedPointer<std::vector<catalog::Schema::Column>> cols,
                                      common::ManagedPointer<ChangeMap> change_map) {
  // Add the column
  auto add_col_cmd = cmd.CastManagedPointerTo<planner::AlterPlanNode::AddColumnCmd>();
  auto new_col = add_col_cmd->GetColumn();
  cols->push_back(new_col);

  // Record the change
  (*change_map)[new_col.Name()].push_back(ChangeType::Add);

  // TODO(XC): adding constraints
  return true;
}

bool AlterTableCmdExecutor::DropColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                       common::ManagedPointer<std::vector<catalog::Schema::Column>> cols,
                                       common::ManagedPointer<ChangeMap> change_map) {
  auto drop_col_cmd = cmd.CastManagedPointerTo<planner::AlterPlanNode::DropColumnCmd>();
  auto drop_col_oid = drop_col_cmd->GetColOid();
  if (drop_col_oid == catalog::INVALID_COLUMN_OID) {
    return drop_col_cmd->IsIfExist();
  }

  for (auto itr = cols->begin(); itr != cols->end(); ++itr) {
    if (itr->Oid() == drop_col_oid) {
      cols->erase(itr);
      break;
    }
  }

  // Record the change
  (*change_map)[drop_col_cmd->GetName()].push_back(ChangeType::DropNoCascade);

  return true;
}
}  // namespace terrier::execution::sql
