//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  Catalog * catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  // idxes_info = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

void UpdateExecutor::update(const Tuple &tuple, RID *rid) {
  Tuple updated_tuple{GenerateUpdatedTuple(tuple)};
  LOG_DEBUG("insert into table");
  TableHeap *tableHeap = table_info_->table_.get();
  tableHeap->UpdateTuple(updated_tuple, *rid, GetExecutorContext()->GetTransaction());

  for (auto &inf : idxes_info) {
    LOG_DEBUG("insert into index");
    Tuple &tuple1 = const_cast<Tuple &>(tuple);
    Tuple key = tuple1.KeyFromTuple(table_info_->schema_, inf->key_schema_, inf->index_->GetKeyAttrs());
    inf->index_->DeleteEntry(key, key.GetRid(), GetExecutorContext()->GetTransaction());
    inf->index_->InsertEntry(key, *rid, GetExecutorContext()->GetTransaction());
  }
}
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  update(*tuple, rid);
  return true;
}
}  // namespace bustub
