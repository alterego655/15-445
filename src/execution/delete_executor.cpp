//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  idxes_info = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

void DeleteExecutor::remove(const Tuple &tuple) {
  for (auto &inf : idxes_info) {
    std::vector<RID> rids;
    Tuple &tuple1 = const_cast<Tuple &>(tuple);
    Tuple key = tuple1.KeyFromTuple(table_info_->schema_, inf->key_schema_, inf->index_->GetKeyAttrs());
    inf->index_->ScanKey(key, &rids, GetExecutorContext()->GetTransaction());
    TableHeap *tableHeap = table_info_->table_.get();
    tableHeap->MarkDelete(rids[0], GetExecutorContext()->GetTransaction());
    inf->index_->DeleteEntry(key, key.GetRid(), GetExecutorContext()->GetTransaction());
    LOG_DEBUG("insert into index");
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    remove(*tuple);
    return true;
  }
  return false;
}

}  // namespace bustub
