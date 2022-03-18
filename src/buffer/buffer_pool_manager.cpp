//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <common/logger.h>
#include <cassert>
#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

frame_id_t BufferPoolManager::GetReplaceblePage() {
  Page *R;
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    return frame_id;
  }
  if (replacer_->Size() > 0) {
    replacer_->Victim(&frame_id);
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
    R = GetTargetPage(frame_id);
    if (R->IsDirty()) {
      disk_manager_->WritePage(R->GetPageId(), R->GetData());
      R->SetPinCount(0);
    }
    page_table_.erase(R->GetPageId());
    return frame_id;
  }
  // LOG_DEBUG("--fall short of memory!\n");
  // PrintInfo();
  return INVALID_FRAME_ID;
}
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  assert(page_id != INVALID_PAGE_ID);
  Page *p;
  frame_id_t frame_id;
  frame_id = seekFrame(page_id);
  if (frame_id != INVALID_FRAME_ID) {
    p = GetTargetPage(frame_id);
    p->IncrePinCount();
    replacer_->Pin(frame_id);
    return p;
  }

  frame_id = GetReplaceblePage();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }
  p = GetTargetPage(frame_id);
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
  }
  page_table_.insert({page_id, frame_id});
  p->SetPageId(page_id);
  p->is_dirty_ = false;
  p->SetPinCount(1);
  disk_manager_->ReadPage(page_id, p->GetData());
  return p;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  assert(page_id != INVALID_PAGE_ID);
  frame_id_t frame_id = seekFrame(page_id);
  if (frame_id == INVALID_FRAME_ID) {
    return true;
  }
  Page *p = GetTargetPage(frame_id);
  if (p->pin_count_ < 0) {
    return false;
  }
  if (p->pin_count_ > 0) {
    p->pin_count_--;
  }
  if (p->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  p->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  assert(page_id != INVALID_PAGE_ID);
  frame_id_t frame_id = seekFrame(page_id);
  if (frame_id != INVALID_FRAME_ID) {
    Page *p = GetTargetPage(frame_id);
    disk_manager_->WritePage(page_id, p->GetData());
    p->is_dirty_ = false;
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = GetReplaceblePage();
  if (frame_id != INVALID_FRAME_ID) {
    Page *p = GetTargetPage(frame_id);
    page_id_t new_page_id = disk_manager_->AllocatePage();
    p->is_dirty_ = false;
    p->SetPageId(new_page_id);
    p->SetPinCount(1);
    page_table_.insert({new_page_id, frame_id});

    *page_id = new_page_id;
    return p;
  }

  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  assert(page_id != INVALID_PAGE_ID);
  frame_id_t frame_id = seekFrame(page_id);
  if (frame_id == INVALID_FRAME_ID) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  Page *p = GetTargetPage(frame_id);
  if (p->GetPinCount() > 0) {
    return false;
  }
  if (p->is_dirty_) {
    disk_manager_->WritePage(page_id, p->GetData());
  }
  p->is_dirty_ = false;
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  p->ResetMemory();
  p->SetPinCount(0);
  disk_manager_->DeallocatePage(page_id);
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    auto frame_id = seekFrame(i);
    Page *p = GetTargetPage(frame_id);
    disk_manager_->WritePage(i, p->GetData());
    p->is_dirty_ = false;
  }
}
}  // namespace bustub
