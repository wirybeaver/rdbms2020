#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    auto table_oid_ = next_table_oid_++;
    tables_[table_oid_] =  std::make_unique<TableMetadata>(
        schema, table_name,
        std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn), table_oid_);
    names_[table_name] = table_oid_;
    return tables_[table_oid_].get();
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    BUSTUB_ASSERT(names_.count(table_name) != 0, "Table name doesn't exit!");
    return GetTable(names_[table_name]);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    BUSTUB_ASSERT(tables_.count(table_oid) != 0, "Table oid doesn't exist");
    return tables_[table_oid].get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    BUSTUB_ASSERT(names_.count(table_name)!=0, "Table name not found, create the table before index");
    if (index_names_.count(table_name) == 0) {  // because we have make it in the CreateTable and we should create table before
      index_names_[table_name] = std::unordered_map<std::string, index_oid_t>();
    }
    auto& internal_index_names = index_names_[table_name];
    BUSTUB_ASSERT(internal_index_names.count(index_name)==0, "The index name within a table should be unique");

    index_oid_t index_oid = next_index_oid_++;
    auto *index_meta = new IndexMetadata(index_name, table_name, &schema, key_attrs);
    auto b_plus_tree_index = std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(index_meta, bpm_);
    auto index_info = std::make_unique<IndexInfo>(key_schema, index_name, std::move(b_plus_tree_index), index_oid,
                                                  table_name, keysize);
    auto table_meta = GetTable(table_name);
    auto tuple_iter = table_meta->table_->Begin(txn);
    auto end = table_meta->table_->End();

    while (tuple_iter != end) {
      auto key_sub_tuple = tuple_iter->KeyFromTuple(schema, key_schema, key_attrs);
      index_info->index_->InsertEntry(key_sub_tuple, tuple_iter->GetRid(), txn);
      ++tuple_iter;
    }

    internal_index_names[index_name] = index_oid;
    indexes_[index_oid] = std::move(index_info);
    return indexes_[index_oid].get();
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    return index_names_.count(table_name)==0 || index_names_[table_name].count(index_name)==0 ?
               nullptr : GetIndex(index_names_[table_name][index_name]);
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    return indexes_.count(index_oid) == 0? nullptr : indexes_[index_oid].get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> ans;
    if(index_names_.count(table_name) != 0) {
      for(auto& [_, index_oid] : index_names_[table_name]) {
        ans.push_back(indexes_[index_oid].get());
      }
    }
    return ans;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
