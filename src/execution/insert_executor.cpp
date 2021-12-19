//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exe(std::move(child_executor)) {}

void InsertExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();

  table = catalog->GetTable(plan_->TableOid());
  LOG_DEBUG("Get table indexes");
  info = catalog->GetTableIndexes(catalog->GetTable(plan_->TableOid())->name_);

  if (!plan_->IsRawInsert()) {
    child_exe->Init();
  }
  if (plan_->IsRawInsert()) {
    itr = plan_->RawValues().begin();
  }
}

bool InsertExecutor::insert(const Tuple &tuple, RID *rid) {
  LOG_DEBUG("insert into table :%d", table->oid_);
  LOG_DEBUG("table's first page id is %d", table->table_.get()->GetFirstPageId());
  TableHeap *tableHeap = table->table_.get();
  bool inserted = tableHeap->InsertTuple(tuple, rid, GetExecutorContext()->GetTransaction());

  for (auto &inf : info) {
    LOG_DEBUG("insert into index");
    Tuple &tuple1 = const_cast<Tuple &>(tuple);
    Tuple key = tuple1.KeyFromTuple(table->schema_, inf->key_schema_, inf->index_->GetKeyAttrs());
    inf->index_->InsertEntry(key, *rid, GetExecutorContext()->GetTransaction());
  }
  return inserted;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!plan_->IsRawInsert()) {
    if (child_exe->Next(tuple, rid)) {
      return insert(*tuple, rid);
    }
    return false;
  }
  if (itr != plan_->RawValues().end()) {
    Tuple tuple1(*itr++, &table->schema_);
    return insert(tuple1, rid);
  }
  return false;
}

}  // namespace bustub
