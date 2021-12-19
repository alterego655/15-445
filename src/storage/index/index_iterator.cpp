/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage, int idx, BufferPoolManager *bufferPoolManager)
    : leaf_page(leafPage), cur_idx(idx), bufferPoolManager_(bufferPoolManager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page != nullptr) {
    UnlockAndUnPin();
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return leaf_page == nullptr || (leaf_page->GetNextPageId() == INVALID_PAGE_ID && cur_idx >= leaf_page->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_page->GetItem(cur_idx); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  cur_idx++;
  if (cur_idx == leaf_page->GetSize() && leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf_page->GetNextPageId();
    UnlockAndUnPin();
    Page *next_page = bufferPoolManager_->FetchPage(next_page_id);  // pined page
    next_page->RLatch();
    leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page);
    cur_idx = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
