//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }
Tuple AggregationExecutor::ConstructTuple() {
  std::vector<Value> res;
  assert(aht_iterator_ != aht_.End());
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    Value val = col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_);
    res.push_back(val);
  }
  return Tuple{res, GetOutputSchema()};
}
void AggregationExecutor::Init() {
  child_->Init();
  Tuple temp_tuple;
  RID temp_rid;
  while (child_->Next(&temp_tuple, &temp_rid)) {
    LOG_DEBUG("Insert here");
    aht_.InsertCombine(MakeKey(&temp_tuple), MakeVal(&temp_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    assert(aht_iterator_ != aht_.End());
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()
            ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
            .GetAs<bool>()) {
      *tuple = ConstructTuple();
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

}  // namespace bustub
