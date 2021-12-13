//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)){}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  RID temp_rid;
  left = left_child_executor_->Next(&left_tuple, &temp_rid);
}

Tuple NestedLoopJoinExecutor::Combine(Tuple *tuple1, Tuple *tuple2) {
  std::vector<Value> res;
  for (auto const &col : GetOutputSchema()->GetColumns()) {
    Value val = col.GetExpr()->EvaluateJoin(tuple1, left_child_executor_->GetOutputSchema(),
                                            tuple2, right_child_executor_->GetOutputSchema());
    res.emplace_back(val);
  }
  return Tuple{res, GetOutputSchema()};
}


bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *left_schema = left_child_executor_->GetOutputSchema();
  const Schema *right_schema = right_child_executor_->GetOutputSchema();
  Tuple right_tuple;
  RID temp_rid;
  while (true) {
    if (!left) {
      return false;
    }
    if (!right_child_executor_->Next(&right_tuple, &temp_rid)) {
      right_child_executor_->Init();
      left = left_child_executor_->Next(&left_tuple, &temp_rid);
      continue;
    }

    if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
      *tuple = Combine(&left_tuple, &right_tuple);
      return true;
    }
  }


}

}  // namespace bustub
