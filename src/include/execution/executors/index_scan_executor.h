//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

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

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  std::vector<Value> GetValFromTuple(const Tuple &tuple);

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  std::unique_ptr<Index> idx = nullptr;
  std::unique_ptr<TableHeap> table = nullptr;
  B_PLUS_TREE_INDEX_ITERATOR_TYPE idx_itr{nullptr, 0, nullptr};
};
}  // namespace bustub
