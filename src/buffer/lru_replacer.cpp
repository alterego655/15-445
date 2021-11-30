//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <climits>
#include <iterator>
#include <mutex>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mtx);
  if (lru_list.empty()) {
    *frame_id = INVALID_FRAME_ID;
    return false;
  }
  frame_id_t frame = lru_list.back();
  lru_map.erase(frame);
  lru_list.pop_back();
  *frame_id = frame;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mtx);
  if (lru_map.find(frame_id) != lru_map.end()) {
    auto ptr = lru_map[frame_id];
    lru_list.erase(ptr);
    lru_map.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mtx);
  if (lru_map.count(frame_id) != 0) {
    return;
  }
  lru_list.push_front(frame_id);
  lru_map[frame_id] = lru_list.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(mtx);
  return lru_list.size();
}

}  // namespace bustub
