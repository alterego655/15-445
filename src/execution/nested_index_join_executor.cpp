//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  inner_table = catalog->GetTable(plan_->GetInnerTableOid())->table_.get();
  idx =
      std::move(catalog->GetIndex(plan_->GetIndexName(), catalog->GetTable(plan_->GetInnerTableOid())->name_)->index_);
  b_plus_tree_idx = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(idx.get());
  child_executor_->Init();
}

Tuple NestIndexJoinExecutor::Combine(Tuple *tuple1, Tuple *tuple2) {
  std::vector<Value> result;
  for (auto const &col : GetOutputSchema()->GetColumns()) {
    Value val = col.GetExpr()->EvaluateJoin(tuple1, plan_->OuterTableSchema(), tuple2, plan_->InnerTableSchema());
    result.emplace_back(val);
  }
  return Tuple{result, GetOutputSchema()};
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!res.empty()) {
      RID right_rid = res.back();
      res.pop_back();
      Tuple right_tuple;
      inner_table->GetTuple(right_rid, &right_tuple, exec_ctx_->GetTransaction());
      *tuple = Combine(&left_tuple, &right_tuple);
      return true;
    }
    if (!child_executor_->Next(&left_tuple, rid)) {
      return false;
    }
    Tuple key = left_tuple.KeyFromTuple(*GetOutputSchema(), *idx->GetKeySchema(), idx->GetKeyAttrs());
    idx->ScanKey(key, &res, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
