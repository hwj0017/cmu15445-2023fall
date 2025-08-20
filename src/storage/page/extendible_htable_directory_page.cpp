//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
  for (auto &local_depth : local_depths_) {
    local_depth = 0;
  }
  for (auto &page_id : bucket_page_ids_) {
    page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (max_depth_ == sizeof(hash) * 8 || global_depth_ == 0) {
    return 0;
  }
  return hash & ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  auto low_bit = GetLocalDepthMask(bucket_idx) & bucket_idx;
  auto local_depth = local_depths_[bucket_idx];
  for (uint32_t i = 0; i < 1 << (global_depth_ - local_depth); ++i) {
    bucket_page_ids_[i << local_depth | low_bit] = bucket_page_id;
  }
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  assert(global_depth_ != 0);
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

// auto ExtendibleHTableDirectoryPage::GetMergeImageIndex(uint32_t bucket_idx) const -> uint32_t {
//   assert(global_depth_ != 0);
//   return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
// }
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  for (uint32_t i = 1 << global_depth_; i < 1 << (global_depth_ + 1); ++i) {
    uint32_t image_page_index = i ^ (1 << global_depth_);
    local_depths_[i] = local_depths_[image_page_index];
    bucket_page_ids_[i] = bucket_page_ids_[image_page_index];
  }
  ++global_depth_;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  --global_depth_;
  for (uint32_t i = 1 << global_depth_; i < 1 << (global_depth_ + 1); ++i) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < (1 << (global_depth_ - 1)); ++i) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  auto low_bit = GetLocalDepthMask(bucket_idx) & bucket_idx;
  auto local_depth = local_depths_[bucket_idx];
  for (uint32_t i = 0; i < 1 << (global_depth_ - local_depth); ++i) {
    ++local_depths_[i << local_depth | low_bit];
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  auto low_bit = GetLocalDepthMask(bucket_idx) & bucket_idx;
  auto local_depth = local_depths_[bucket_idx];
  for (uint32_t i = 0; i < 1 << (global_depth_ - local_depth); ++i) {
    --local_depths_[i << local_depth | low_bit];
  }
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth = local_depths_[bucket_idx];
  return (1 << local_depth) - 1;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepthMask() const -> uint32_t { return (1 << max_depth_) - 1; }
// auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }
}  // namespace bustub
