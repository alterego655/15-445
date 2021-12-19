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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_id = plan_->GetTableOid();
  LOG_DEBUG("table id is %d", plan_->GetTableOid());
  table = exec_ctx_->GetCatalog()->GetTable(table_id)->table_.get();
  itr = table->Begin(exec_ctx_->GetTransaction());
  LOG_DEBUG("Iterator begins");
}

Tuple SeqScanExecutor::GenerateTuple(const Tuple &tuple) {
  std::vector<Value> res;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    Value val = col.GetExpr()->Evaluate(&tuple, GetOutputSchema());
    res.push_back(val);
  }
  return Tuple{res, GetOutputSchema()};
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // LOG_DEBUG("In here4 %d", ++count4);
  if (itr == table->End()) {
    return false;
  }
  *tuple = *itr;
  *rid = tuple->GetRid();
  // LOG_DEBUG("In here1 %d", ++count1);
  if (plan_->GetPredicate() != nullptr) {
    // LOG_DEBUG("In here 2");
    while (itr != table->End()) {
      *tuple = *itr;
      // LOG_DEBUG("In here 3");
      if ((plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>())) {
        // LOG_DEBUG("find the tuple 4");
        break;
      }
      itr++;
    }
    if (itr == table->End()) {
      // LOG_DEBUG("In here 5");
      return false;
    }
  }
  // LOG_DEBUG("In here2 %d", ++count2);
  *tuple = GenerateTuple(*tuple);
  itr++;
  // LOG_DEBUG("In here3 %d", ++count3);
  return true;
}

}  // namespace bustub
