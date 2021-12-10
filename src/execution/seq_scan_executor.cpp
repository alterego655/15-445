//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_id = plan_->GetTableOid();
  LOG_DEBUG("table id is %d",plan_->GetTableOid());
  table = exec_ctx_->GetCatalog()->GetTable(table_id)->table_.get();
  itr = table->Begin(exec_ctx_->GetTransaction());
}

std::vector<Value> SeqScanExecutor::GetValFromTuple(const Tuple *tuple, const Schema *schema) {
  std::vector<Value> res;
  for (auto &col : schema->GetColumns()) {
    Value val = tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
    res.push_back(val);
  }
  return res;
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (itr == table->End()) {
    return false;
  }
  *tuple = *itr;
  *rid = tuple->GetRid();
  if (plan_->GetPredicate() != nullptr) {
    while (itr != table->End()) {
      *tuple = *itr;
      if ((plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>())) {
        break;
      }
      itr++;
    }
    if (itr == table->End()) {
      return false;
    }
  }
  std::vector<Value> val = GetValFromTuple(tuple, plan_->OutputSchema());
  *tuple = Tuple(val, plan_->OutputSchema());
  *rid = tuple->GetRid();
  itr++;
  return true;
}
}  // namespace bustub
