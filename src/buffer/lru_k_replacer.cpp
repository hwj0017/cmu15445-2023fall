//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cassert>
#include <mutex>
#include "common/exception.h"

namespace bustub {
bool LRUKReplacer::Comparator::operator()(const LRUKNode *l, const LRUKNode *r) const {
  if (l->history_.size() == k_ && r->history_.size() < k_) return false;
  if (l->history_.size() < k_ && r->history_.size() == k_) return true;
  return l->history_.back() < r->history_.back();
}
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : evict_queue_(Comparator{k}), max_size_(num_frames), evictable_size_(0), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = evict_queue_.begin();
  if (it == evict_queue_.end()) return false;
  *frame_id = (*it)->fid_;
  evict_queue_.erase(it);
  node_store_.erase(*frame_id);
  --evictable_size_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  ++current_timestamp_;
  if (auto it = node_store_.find(frame_id); it != node_store_.end()) {
    auto &node = it->second;
    if (node.is_evictable_) {
      evict_queue_.erase(node.iter_);
      node.history_.push_front(current_timestamp_);
      if (node.history_.size() > k_) node.history_.pop_back();
      auto res = evict_queue_.emplace(&node);
      node.iter_ = std::move(res.first);
    } else {
      node.history_.push_front(current_timestamp_);
      if (node.history_.size() > k_) node.history_.pop_back();
    }
    return;
  }
  if (node_store_.size() < max_size_) {
    auto res = node_store_.emplace(frame_id, LRUKNode());
    auto &node = res.first->second;
    node.fid_ = frame_id;
    node.history_.push_front(current_timestamp_);
    node.is_evictable_ = true;
    auto newRes = evict_queue_.emplace(&node);
    node.iter_ = std::move(newRes.first);
    ++evictable_size_;
    return;
  }
  throw("error");
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (auto it = node_store_.find(frame_id); it != node_store_.end()) {
    auto &node = it->second;
    if (node.is_evictable_ != set_evictable) {
      node.is_evictable_ = set_evictable;
      // be evictable
      if (set_evictable) {
        auto res = evict_queue_.emplace(&node);
        node.iter_ = std::move(res.first);
        ++evictable_size_;
      } else {
        // be unevictable
        evict_queue_.erase(std::move(node.iter_));
        --evictable_size_;
      }
    }
    return;
  }
  throw("error");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (auto it = node_store_.find(frame_id); it != node_store_.end()) {
    auto &node = it->second;
    if (node.is_evictable_) {
      evict_queue_.erase(std::move(node.iter_));
      node_store_.erase(it);
      --evictable_size_;
    } else {
      throw("error");
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return evictable_size_; }

}  // namespace bustub
