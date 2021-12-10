//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

std::vector<Value> IndexScanExecutor::GetValFromTuple(const Tuple *tuple, const Schema *schema) {
  std::vector<Value> res;
  for (auto &col : schema->GetColumns()) {
    Value val = tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
    res.push_back(val);
  }
  return res;
}

void IndexScanExecutor::Init() {
  auto idx_id = plan_->GetIndexOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  idx = std::move(catalog->GetIndex(idx_id)->index_);
  B_PLUS_TREE_INDEX_TYPE *b_plus_tree_idx = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(idx.get());
  idx_itr = b_plus_tree_idx->GetBeginIterator();
  auto table_name = idx->GetMetadata()->GetTableName();
  table = std::move(catalog->GetTable(table_name)->table_);
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (idx_itr.isEnd()) {
    return false;
  }
  *rid = (*idx_itr).second;
  table->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  if (plan_->GetPredicate() != nullptr) {
    while (!idx_itr.isEnd()) {
      *rid = (*idx_itr).second;
      table->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
      if ((plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>())) {
        break;
      }
      ++idx_itr;
    }
    if (idx_itr.isEnd()) {
      return false;
    }
  }
  std::vector<Value> val = GetValFromTuple(tuple, plan_->OutputSchema());
  *tuple = Tuple(val, plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++idx_itr;
  return true;
}


}  // namespace bustub
