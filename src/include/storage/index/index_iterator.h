//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables

  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage, int idx, BufferPoolManager *bufferPoolManager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return leaf_page == itr.leaf_page && cur_idx == itr.cur_idx; }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

 private:
  // add your own private member variables here
  void UnlockAndUnPin() {
    bufferPoolManager_->FetchPage(leaf_page->GetPageId())->RUnlatch();
    bufferPoolManager_->UnpinPage(leaf_page->GetPageId(), false);
    bufferPoolManager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page;
  int cur_idx;
  BufferPoolManager *bufferPoolManager_;
};

}  // namespace bustub
