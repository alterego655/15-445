//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include "common/logger.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockCount = 0;

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPage(key, false, TypeOfOp::READ, transaction);
  if (page == nullptr) {
    return false;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  RID temp_rid;
  bool ret = leaf_page->Lookup(key, &temp_rid, comparator_);
  if (ret) {
    result->emplace_back(temp_rid);
  }
  BreakFree(false, transaction, leaf_page->GetPageId());
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LockRootPageId(true);
  if (IsEmpty()) {
    StartNewTree(key, value);
    TryUnlockRootPageId(true);
    return true;
  }
  TryUnlockRootPageId(true);
  bool res = InsertIntoLeaf(key, value, transaction);
  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_root_id;
  Page *page = buffer_pool_manager_->NewPage(&new_root_id);
  assert(page != nullptr);
  LeafPage *root = reinterpret_cast<LeafPage *>(page->GetData());
  root->Init(new_root_id, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = new_root_id;
  UpdateRootPageId(1);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(new_root_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPage(key, false, TypeOfOp::INSERT, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType val;
  if (leaf_page->Lookup(key, &val, comparator_)) {
    BreakFree(true, transaction);
    return false;
  }
  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
    LeafPage *new_leaf = Split(leaf_page);
    InsertIntoParent(leaf_page, new_leaf->KeyAt(0), new_leaf, transaction);
  }
  BreakFree(true, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Warning!");
  }
  N *new_node;
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_page);
    new_leaf_node->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_page);
    new_internal_node->Init(new_page_id, internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    Page *page = buffer_pool_manager_->NewPage(&new_root_id);
    assert(page != nullptr);
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(page);
    new_root_page->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_id;
    UpdateRootPageId(0);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(page != nullptr);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  if (parent_page->GetSize() > parent_page->GetMaxSize()) {
    auto *new_parent_page = Split(parent_page);
    InsertIntoParent(parent_page, new_parent_page->KeyAt(0), new_parent_page, transaction);buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LockRootPageId(true);
  if (IsEmpty()) {
    TryUnlockRootPageId(true);
    return;
  }
  TryUnlockRootPageId(true);
  auto* leaf_page = FindLeafPage(key, false, TypeOfOp::REMOVE, transaction);
  LeafPage* ptr_to_leaf = reinterpret_cast<LeafPage *>(leaf_page);
  ptr_to_leaf->RemoveAndDeleteRecord(key, comparator_);
  if (ptr_to_leaf->GetSize() < ptr_to_leaf->GetMinSize()) {
    CoalesceOrRedistribute(ptr_to_leaf, transaction);
  }
  BreakFree(true, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    bool removeOldNode = AdjustRoot(node);
    if (removeOldNode) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return removeOldNode;
  }
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());

  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page);

  N *sibling;
  bool pre = FindSibling(node, sibling, transaction);

  if (sibling->GetSize() + node->GetSize() >= node->GetMaxSize()) {
    int node_idx_parent = parent->ValueIndex(node->GetPageId());
    Redistribute(sibling, node, node_idx_parent);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return false;
  }

  if (!pre) {
    int removed_idx = parent->ValueIndex(sibling->GetPageId());
    Coalesce(&node, &sibling, &parent, removed_idx, transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return false;
  }
  int removed_idx = parent->ValueIndex(node->GetPageId());
  Coalesce(&sibling, &node, &parent, removed_idx, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N* node, N* &sibling, Transaction *transaction) {
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  assert(parent_page != nullptr);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page);

  int node_idx = parent->ValueIndex(node->GetPageId());
  int sibling_idx = node_idx != 0 ? node_idx - 1 : node_idx + 1;

  page_id_t sibling_id = parent->ValueAt(sibling_idx);
  BPlusTreePage *sibling_page = CrabbingFetchPage(sibling_id, -1, transaction, TypeOfOp::REMOVE);
  sibling = reinterpret_cast<N*>(sibling_page);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  return sibling_idx == node_idx - 1;
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if (!(*node)->IsLeafPage()) {
    auto* neighbor_page = reinterpret_cast<InternalPage *>((*neighbor_node));
    auto* node_page = reinterpret_cast<InternalPage *>((*node));
    node_page->MoveAllTo(neighbor_page, (*parent)->KeyAt(index), buffer_pool_manager_);
    transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  } else {
    auto* neighbor_page = reinterpret_cast<LeafPage *>((*neighbor_node));
    auto* node_page = reinterpret_cast<LeafPage *>((*node));
    node_page->MoveAllTo(neighbor_page);
    neighbor_page->SetNextPageId(node_page->GetNextPageId());
    transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  }
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  auto* page_of_parent = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto* parent_page = reinterpret_cast<InternalPage *>(page_of_parent);

  if (!node->IsLeafPage()) {
    auto* neighbor_page = reinterpret_cast<InternalPage *>(neighbor_node);
    auto* node_page = reinterpret_cast<InternalPage *>(node);
    if (index == 0) {
      int idx = parent_page->ValueIndex(neighbor_page->GetPageId());
      KeyType middle_key = parent_page->KeyAt(index);
      neighbor_page->MoveFirstToEndOf(node_page, middle_key, buffer_pool_manager_);
      KeyType new_key = neighbor_page->KeyAt(0);
      parent_page->SetKeyAt(idx, new_key);
    } else {
      int idx = parent_page->ValueIndex(node_page->GetPageId());
      KeyType middle_key = parent_page->KeyAt(index);
      neighbor_page->MoveLastToFrontOf(node_page, middle_key, buffer_pool_manager_);
      KeyType new_key = node_page->KeyAt(0);
      parent_page->SetKeyAt(idx, new_key);
    }
  } else {
    auto* neighbor_page = reinterpret_cast<LeafPage *>(neighbor_node);
    auto* node_page = reinterpret_cast<LeafPage *>(node);
    if (index == 0) {
      neighbor_page->MoveFirstToEndOf(node_page);
      int idx = parent_page->ValueIndex(neighbor_page->GetPageId());
      KeyType new_key = neighbor_page->KeyAt(0);
      parent_page->SetKeyAt(idx, new_key);
    } else {
      neighbor_page->MoveLastToFrontOf(node_page);
      int idx = parent_page->ValueIndex(node_page->GetPageId());
      KeyType new_key = node_page->KeyAt(0);
      parent_page->SetKeyAt(idx, new_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 1) {
    InternalPage* old_root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_id = old_root_page->RemoveAndReturnOnlyChild();

    root_page_id_ = new_root_id;
    UpdateRootPageId(0);
    Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page);
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }
  if (old_root_node->GetSize() == 0) {
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the most left leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key{};
  Page *left_most_page = FindLeafPage(key, true);
  TryUnlockRootPageId(false);
  auto *left_most_leaf = reinterpret_cast<LeafPage *>(left_most_page);
  return INDEXITERATOR_TYPE(left_most_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 *
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *key_page = FindLeafPage(key);
  TryUnlockRootPageId(false);
  auto *key_leaf_page = reinterpret_cast<LeafPage *>(key_page);
  if (key_leaf_page == nullptr) {
    return INDEXITERATOR_TYPE(key_leaf_page, 0, buffer_pool_manager_);
  }
  int idx = key_leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(key_leaf_page, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  KeyType key{};
  Page *left_most_page = FindLeafPage(key, true);
  TryUnlockRootPageId(false);
  auto *leaf = reinterpret_cast<LeafPage *>(left_most_page);
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf->GetNextPageId();
    reinterpret_cast<Page *>(leaf)->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    next_page->RLatch();
    leaf = reinterpret_cast<LeafPage *>(next_page);
  }
  return INDEXITERATOR_TYPE(leaf, leaf->GetSize(), buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*xc
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
*/
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, TypeOfOp operation, Transaction *transaction) {
  bool exclusive = (operation != TypeOfOp::READ);
  LockRootPageId(exclusive);
  if (IsEmpty()) {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }

  auto *node = CrabbingFetchPage(root_page_id_, -1, transaction, operation);
  page_id_t child_page_id;

  for (page_id_t cur = root_page_id_; !node->IsLeafPage();
       node = CrabbingFetchPage(child_page_id, cur, transaction, operation), cur = child_page_id) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(node);
    child_page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
  }
  return reinterpret_cast<Page *>(node);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabbingFetchPage
    (page_id_t child_page_id, page_id_t parent_id, Transaction *transaction, TypeOfOp operation) {
  bool exclusive = operation != TypeOfOp::READ;
  auto page = buffer_pool_manager_->FetchPage(child_page_id);
  Lock(exclusive, page);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (parent_id > 0 && (!exclusive || node->SafeOrNot(operation))) {
    BreakFree(exclusive, transaction, parent_id);
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BreakFree(bool exclusive, Transaction *transaction, page_id_t parent_id) {
  TryUnlockRootPageId(exclusive);
  if (transaction == nullptr) {
    assert(parent_id >= 0 && !exclusive);
    Unlock(false, parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return;
  }

  for (auto *page : *transaction->GetPageSet()) {
    page_id_t page_id = page->GetPageId();
    Unlock(exclusive, page);
    buffer_pool_manager_->UnpinPage(page_id, exclusive);
    if (transaction->GetDeletedPageSet()->count(page_id) > 0) {
      buffer_pool_manager_->DeletePage(page_id);
      transaction->GetDeletedPageSet()->erase(page_id);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
