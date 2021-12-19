//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }
  /*
  std::vector<Value> GetValFromTuple(const Tuple *tuple, const Schema *schema) override {
    std::vector<Value> res;
    for (auto &col : schema->GetColumns()) {
      Value val = tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
      res.push_back(val);
    }
    return res;
  }
  */
  Tuple GenerateTuple(const Tuple &tuple);

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_{};
  TableHeap *table = nullptr;
  TableIterator itr = {nullptr, RID(), nullptr};
  /* int count1 = 0;
  int count2 = 0;
  int count3 = 0;
  int count4 = 0;
   */
};
}  // namespace bustub
