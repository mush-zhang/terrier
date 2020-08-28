
#pragma once

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "catalog/database_catalog.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier {
class StorageTestUtil;
class TpccPlanTest;
}  // namespace terrier

namespace terrier::catalog {

// A UNION of metadata structure for each of the constraints
// stored in the pg_constraint class instances
using PGConstraintMetadata = union PGConstraintMetadata { struct FKMetadata; };

// the metadata for FK
using FKMetadata = struct FKMetadata {
  table_oid_t confrelid_;  // the referenced table oid
  index_oid_t consrcindid_;
  std::vector<col_oid_t> fk_srcs_;  // the column indcies in the current table for foreign key
  std::vector<col_oid_t> fk_refs_;  // the column indicies in the parent table that are reference for the foreign key
  postgres::FKActionType update_action_;
  postgres::FKActionType delete_action_;
};

/**
 * The class datastructure for one pg_constraint instance
 * Including the attribute for characterizing a constraint on a table
 * Currently support NOT NULL, FOREIGN KEY, UNIQUE
 * Each pg_constraint
 *
 ********************* Multi Column Support *********************
 * This claos includes support for multi column senario:
 * CREATE TABLE example (
    a integer,
    b integer,
    c integer,
    UNIQUE (a, c)
);

CREATE TABLE t1 (
  a integer PRIMARY KEY,
  b integer,
  c integer,
  FOREIGN KEY (b, c) REFERENCES other_table (c1, c2)
);
 */
class PGConstraint {
 public:
  /**
   * Constructor going from pg_constraint projected row of the  into constraint class instance
   */
  PGConstraint(DatabaseCatalog *dbc, constraint_oid_t con_oid, std::string con_name, namespace_oid_t con_namespace_id,
               postgres::ConstraintType con_type, bool con_deferrable, bool con_deferred, bool con_validated,
               table_oid_t con_relid, index_oid_t con_index_id, const std::string &con_col_varchar)
      : dbc_(dbc),
        conoid_(con_oid),
        conname_(std::move(con_name)),
        connamespaceid_(con_namespace_id),
        contype_(con_type),
        condeferrable_(con_deferrable),
        condeferred_(con_deferred),
        convalidated_(con_validated),
        conrelid_(con_relid),
        conindid_(con_index_id) {
    FillConCol(con_col_varchar);
  }

  friend class DatabaseCatalog;
  /// pointer to the catalog
  catalog::DatabaseCatalog *dbc_;
  /// oid of the constraint
  constraint_oid_t conoid_;
  /// constraint name
  std::string conname_;
  /// OID of namespace containing constraint
  namespace_oid_t connamespaceid_;
  /// type of the constraint
  postgres::ConstraintType contype_;
  /// deferrable constraint?
  bool condeferrable_;
  /// deferred by default?
  bool condeferred_;
  /// Has the constraint been validated? Currently, can only be false for foreign keys
  bool convalidated_;
  /// table this constraint applies to
  table_oid_t conrelid_;
  /// index supporting this constraint
  index_oid_t conindid_;
  /// the column id that this index applies to
  std::vector<col_oid_t> concol_;
  /// pther metadata depending on the constraint type
  FKMetadata fk_metadata_;

  friend class Catalog;
  friend class postgres::Builder;
  friend class terrier::TpccPlanTest;

 private:
  // fill the columns that the constraint is effective on: this is for UNIQUE, PK, NOTNULL
  void FillConCol(const std::string &con_col_str) {
    std::vector<std::string> raw_oid_vec = SplitString(con_col_str, postgres::VARCHAR_ARRAY_DELIMITER);
    concol_.reserve(raw_oid_vec.size());
    for (std::string &col_oid : raw_oid_vec) {
      concol_.push_back(static_cast<col_oid_t>(stoi(col_oid)));
    }
  }

  // python style spliting a string into vector according to a delimiting char
  std::vector<std::string> SplitString(std::string str, char delimiter = ' ') {
    std::replace(str.begin(), str.end(), delimiter, ' ');
    std::istringstream buf(str);
    std::istream_iterator<std::string> beg(buf), end;
    std::vector<std::string> tokens(beg, end);  // done!
    return tokens;
  }
};
}  // namespace terrier::catalog