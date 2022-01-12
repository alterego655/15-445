//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "common/logger.h"

#include <utility>
#include <vector>

namespace bustub {

std::vector<RID>::iterator LockManager::GetIterator(std::vector<RID> *rids, RID rid) {
  auto itr = rids->begin();
  while (itr != rids->end()) {
    if (itr.operator*() == rid) {
      break;
    }
    itr++;
  }
  return itr;
}

void LockManager::RemoveRid(std::unordered_map<txn_id_t, std::vector<RID>> *txn_to_rid, txn_id_t txnId, RID rid) {
  auto itr1 = GetIterator(txn_to_rid, txnId);
  if (itr1 == txn_to_rid->end()) {
    return;
  }
  auto itr2 = GetIterator(&itr1->second, rid);
  itr1->second.erase(itr2);
}

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator
    (std::list<LockRequest> *request_queue_, txn_id_t txnId) {
  auto itr = request_queue_->begin();
  while (itr != request_queue_->end()) {
    LOG_DEBUG("iterator's transaction id is %d", itr->txn_id_);
    if (itr->txn_id_ == txnId) {
      break;
      LOG_DEBUG("Break");
    }
    itr++;
  }
  return itr;

}

std::unordered_map<txn_id_t, std::vector<RID>>::iterator LockManager::GetIterator
    (std::unordered_map<txn_id_t, std::vector<RID>> *txn_to_rid, txn_id_t txnId) {
  auto itr = txn_to_rid->begin();
  while (itr != txn_to_rid->end()) {
    if (itr->first == txnId) {
      break;
    }
    itr++;
  }
  return itr;
}

void LockManager::AbortHandling(std::list<LockRequest> *request_queue_, txn_id_t txnId, RID rid) {
  LOG_DEBUG("in here to abort");
  // std::vector<RID> rids = txn_to_rid_[txnId];
  /*
  for (auto r : rids) {
    auto *reque = &lock_table_[r].request_queue_;
    auto itr = GetIterator(reque, txnId);
    reque->erase(itr);
    RemoveRid(&txn_to_rid_, txnId, rid);
  }
   */
  auto itr = GetIterator(request_queue_, txnId);
  request_queue_->erase(itr);
  LOG_DEBUG("removed");
  RemoveRid(&txn_to_rid_, txnId, rid);
  throw TransactionAbortException(txnId,AbortReason::DEADLOCK);
}
bool LockManager::CheckBeforeLocking(Transaction *txn, LockMode lockMode) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (lockMode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),
                                      AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLocking(txn, LockMode::SHARED)) {
    return false;
  }

  std::unique_lock<std::mutex> lk(latch_);
  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid),
                        std::forward_as_tuple());
  }

  auto *lrq = &lock_table_.at(rid);
  auto *req_que = &lrq->request_queue_;

  req_que->emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  LOG_DEBUG("request queue's size is %d", static_cast<int>(req_que->size()));
  LOG_DEBUG("Insert in here2");
  txn_to_rid_[txn->GetTransactionId()].push_back(rid);
  std::unique_lock<std::mutex> lkh(lrq->lock_);
  lk.unlock();
  auto do_it = [&]() {return CanGrant(*lrq) && !lrq->upgrading_; };
  while (!do_it()) {
    LOG_DEBUG("Waiting");
    lrq->cv_.wait(lkh, [txn] {return txn->GetState() ==
                                        TransactionState::ABORTED;});
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortHandling(req_que, txn->GetTransactionId(), rid);
  }
  txn->GetSharedLockSet()->emplace(rid);
  auto itr = GetIterator(req_que, txn->GetTransactionId());
  itr->granted_ = true;
  LOG_DEBUG("transaction %d is granted", itr->txn_id_);
  return true;
}

bool LockManager::CanGrant(const LockRequestQueue &lockRequestQueue) {
  auto *req_que = &lockRequestQueue.request_queue_;
  auto itr = req_que->begin();
  while (itr != req_que->end()) {
    if (itr->lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
    itr++;
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLocking(txn, LockMode::EXCLUSIVE)) {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid),
                        std::forward_as_tuple());
  }
  auto *lrq = &lock_table_.at(rid);
  auto *req_que = &lrq->request_queue_;
  req_que->emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn_to_rid_[txn->GetTransactionId()].push_back(rid);
  std::unique_lock<std::mutex> lkh(lrq->lock_);
  lk.unlock();
  LOG_DEBUG("IN HERE!!!");
  auto itr = GetIterator(req_que, txn->GetTransactionId());
  // auto do_it = [&]() { return ; };
  if (itr != req_que->begin()) {
    lrq->cv_.wait(lkh, [txn, req_que, itr]() -> bool { return txn->GetState() ==
                                        TransactionState::ABORTED || (req_que->begin() == itr);});
  }
  LOG_DEBUG("In here finally!");
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortHandling(req_que, txn->GetTransactionId(), rid);
  }

  LOG_DEBUG("In here3");
  txn->GetExclusiveLockSet()->emplace(rid);
  // auto itr2 = GetIterator(req_que, txn->GetTransactionId());
  itr->granted_ = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  LOG_DEBUG("In here");
  auto *lrq = &lock_table_.at(rid);
  auto *req_que = &lrq->request_queue_;
  if (lrq->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::UPGRADE_CONFLICT);
  }
  txn->GetSharedLockSet()->erase(rid);
  auto itr = GetIterator(req_que, txn->GetTransactionId());
  req_que->erase(itr);
  RemoveRid(&txn_to_rid_, txn->GetTransactionId(), rid);
  auto itr1 = req_que->begin();
  while (itr1 != req_que->end()) {
    if (!itr1->granted_) {
      break;
    }
    itr1++;
  }
  LOG_DEBUG("In here!!!");
  req_que->insert(itr1, {txn->GetTransactionId(), LockMode::EXCLUSIVE});
  txn_to_rid_[txn->GetTransactionId()].push_back(rid);

  for (const auto req : *req_que) {
    int grant = req.granted_ ? 1 : 0;
    LOG_DEBUG("req id is %d, is granted or not %d", req.txn_id_, grant);
  }
  std::unique_lock<std::mutex> lkh(lrq->lock_);
  lk.unlock();
  // auto itr2 = GetIterator(req_que, txn->GetTransactionId());
  auto itr2 = GetIterator(req_que, txn->GetTransactionId());
  if (itr2 != req_que->begin()) {
    LOG_DEBUG("is waiting...");
    lrq->cv_.wait(lkh, [txn, req_que, itr2]() -> bool {return txn->GetState() ==
                                        TransactionState::ABORTED || (req_que->begin() == itr2); });
  }
  LOG_DEBUG("Aquired!");
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortHandling(req_que, txn->GetTransactionId(), rid);
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  LOG_DEBUG("Upgrade succ");
  itr2->granted_ = true;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  auto *lrq = &lock_table_.at(rid);
  auto *req_que = &lrq->request_queue_;
  auto itr = GetIterator(req_que, txn->GetTransactionId());
  LOG_DEBUG("transaction id is %d", txn->GetTransactionId());
  LockMode mode = itr->lock_mode_;
  RemoveRid(&txn_to_rid_, txn->GetTransactionId(), rid);
  req_que->erase(itr);
  for (auto req : *req_que) {
    LOG_DEBUG("reque txn id is %d", req.txn_id_);
  }
  /*
  auto itr_table = lock_table_.begin();
  while (itr_table != lock_table_.end()) {
    auto reque = itr_table->second.request_queue_;
    for (auto req : reque) {
      LOG_DEBUG("Reque txn id is %d", req.txn_id_);
    }
    itr_table++;
  }
   */
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  if (txn->GetState() == TransactionState::GROWING &&
      !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
      mode == LockMode::SHARED)){
    txn->SetState(TransactionState::SHRINKING);
  }
  LOG_DEBUG("In here unlock!");
  // std::unique_lock<std::mutex> lkh(lrq->lock_);
  // lk.unlock();
  lrq->cv_.notify_all();
  LOG_DEBUG("notify in unlock!");
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto itr = waits_for_.find(t1);
  if (itr != waits_for_.end()) {
    itr->second.insert(t2);
    return;
  }
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto itr = waits_for_.find(t1);
  if (itr != waits_for_.end()) {
    LOG_DEBUG("t1 is %d, t2 is %d", t1, t2);
    itr->second.erase(t2);
  }
}

void LockManager::RemoveNode(txn_id_t txnId) {
  waits_for_.erase(txnId);
  auto itr = waits_for_.begin();
  while (itr != waits_for_.end()) {
    auto set = itr->second;
    if (set.count(txnId) > 0) {
      RemoveEdge(itr->first, txnId);
    }
    itr++;
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // std::vector<txn_id_t> vector;
  int num = waits_for_.size();
  std::map<txn_id_t, bool> marked;
  std::map<txn_id_t, bool> recStack;
  LOG_DEBUG("SIZE1 is %d", num);
  if (txns.empty()) {
    auto itr2 = waits_for_.begin();
    for (; itr2 != waits_for_.end(); itr2++) {
      txns.insert(itr2->first);
      auto sets = itr2->second;
      for (auto s : sets) {
        txns.insert(s);
      }
    }
  }

  LOG_DEBUG("SIZE %d", static_cast<int>(txns.size()));
  for (auto txn : txns) {
    recStack.insert({txn, false});
    marked.insert({txn, false});
  }

  for (auto txn : txns) {
    LOG_DEBUG("IN HERE2");
    if (dfs(txn, &marked, &recStack)) {
      auto itr1 = recStack.begin();
      int temp = INT_MIN;
      for (; itr1 != recStack.end(); ++itr1) {
        if (itr1->second) {
          temp = std::max(temp, itr1->first);
        }
      }
      *txn_id = temp;

      LOG_DEBUG("selected victim is %d", static_cast<int>(*txn_id));
      LOG_DEBUG("Stack size is %d", static_cast<int>(recStack.size()));
      txns.clear();
      return true;
    }
  }
  LOG_DEBUG("NO cycle");
  txns.clear();
  return false;
}

bool LockManager::dfs(int v, std::map<txn_id_t, bool> *marked, std::map<txn_id_t, bool> *recStack) {
  if(!marked->at(v))
  {
    // Mark the current node as visited and part of recursion stack
    marked->at(v) = true;
    LOG_DEBUG("marked at this position is true %d", v);
    recStack->at(v) = true;

    // Recur for all the vertices adjacent to this vertex


    auto neigbors = waits_for_[v];
    for (auto n : neigbors) {

      if (!marked->at(n) && dfs(n, marked, recStack)) {
        return true;
      }

      if (recStack->at(n)) {
        return true;
      }
    }
  }

  recStack->at(v) = false;
  LOG_DEBUG("stack at this position is false %d", v);
  return false;
}
/*
bool LockManager::dfs(std::map<txn_id_t, std::set<int> > *waits_for,
                      std::vector<int> *flags, int i) {
  LOG_DEBUG("In here 3");
  if (flags->at(i) == 1) {
    LOG_DEBUG("In here1");
    while (stk.top() != i) {
      txn_id_t txnId = stk.top();
      stk.pop();
      elements.emplace(txnId);
      if (stk.top() == i) {
        elements.emplace(i);
      }
    }
    while (!stk.empty()) {
      stk.pop();
    }
    return true;
  }
  if (flags->at(i) == -1) {
    return false;
  }
  flags[i].push_back(1);
  LOG_DEBUG("In here2");
  stk.push(i);
  for (int j : waits_for->at(i)) {

    if (dfs(waits_for, flags, j)) {
      return true;
    }
  }
  flags[i].push_back(-1);
  return false;
}
*/
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  auto itr = waits_for_.begin();
  while (itr != waits_for_.end()) {
    auto set = itr->second;
    for (auto s : set) {
      res.emplace_back(itr->first, s);
    }
    itr++;
  }
  return res;
}


void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      auto itr = lock_table_.begin();
      LOG_DEBUG("In here");
      while (itr != lock_table_.end()) {
        auto req_que = itr->second.request_queue_;
        auto itr7 = req_que.begin();
        for (; itr7 != req_que.end(); ++itr7) {
          int grant = itr7->granted_ ? 1 : 0;
          LOG_DEBUG("transaction %d is granted or not %d", itr7->txn_id_, grant);
        }
        for (const auto &req : req_que) {
          // txns.insert(req.txn_id_);
          req.granted_ ? granted.emplace(req.txn_id_) : waiting.emplace(req.txn_id_);
        }
        for (const auto &w : waiting) {
          for (const auto &g : granted) {
            AddEdge(w, g);
          }
        }
        waiting.clear();
        granted.clear();
        itr++;
      }
      LOG_DEBUG("graph's size is %d", static_cast<int>(waits_for_.size()));
      LOG_DEBUG("granting's size is %d", static_cast<int>(granted.size()));
      LOG_DEBUG("waiting's size is %d", static_cast<int>(waiting.size()));
      auto itr3 = granted.begin();
      for (; itr3 != granted.end(); ++itr3) {
        LOG_DEBUG("granted element is :%d", *itr3);
      }

      auto itr4 = waiting.begin();
      for (; itr4 != waiting.end(); ++itr4) {
        LOG_DEBUG("waiting element is %d", *itr4);
      }
      int count = 0;
      auto itr5 = waits_for_.begin();

      for (; itr5 != waits_for_.end(); ++itr5) {
        auto set = itr5->second;
        for (auto s : set) {
          LOG_DEBUG("wait graph %d's key, value is %d", count, s);
        }
        count++;
      }

      txn_id_t youngest;
      // bool cyclic = HasCycle(&youngest);
      LOG_DEBUG("In here1");
      while (HasCycle(&youngest)) {
        LOG_DEBUG("victim is %d", youngest);
        auto *txn = TransactionManager::GetTransaction(youngest);
        LOG_DEBUG("HAHAHA");
        LOG_DEBUG("txn_to_rid's size is %d", static_cast<int>(txn_to_rid_.size()));
        std::vector<RID> rids = txn_to_rid_[youngest];
        LOG_DEBUG("transaction id is %d", txn->GetTransactionId());
        txn->SetState(TransactionState::ABORTED);
        RemoveNode(youngest);
        for (auto rid : rids) {
          auto *lock_que = &lock_table_[rid];
          LOG_DEBUG("In here, notify");
          lock_que->cv_.notify_all();
        }
      }
      LOG_DEBUG("In here2");
      waits_for_.clear();
      waiting.clear();
      granted.clear();
      /*
      elements.clear();
      while (!stk.empty()) {
        stk.pop();
      }
       */
    }
  }
}


}  // namespace bustub
