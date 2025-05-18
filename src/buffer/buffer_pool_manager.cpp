//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
void BufferPoolManager::FlushFrame(Page *page) {
  if (page->is_dirty_) {
    page->is_dirty_ = false;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
    future.get();
  }
}
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(mutex_);
  auto frame_id = 0;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else if (replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    page_table_.erase(page->page_id_);
    FlushFrame(pages_ + frame_id);
  } else {
    return nullptr;
  }
  *page_id = AllocatePage();

  page_table_.emplace(*page_id, frame_id);

  page->page_id_ = *page_id;
  // not mutex
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(mutex_);
  frame_id_t frame_id;
  Page *page = nullptr;
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    frame_id = it->second;
    page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    ++page->pin_count_;
    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  } else {
    page = pages_ + frame_id;
    page_table_.erase(page->page_id_);
    FlushFrame(page);
  }

  page_table_.emplace(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  page->ResetMemory();
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || pages_[it->second].pin_count_ == 0) {
    return false;
  }
  auto page = pages_ + it->second;

  --page->pin_count_;

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(it->second, true);
  }
  if (is_dirty) page->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto page = pages_ + it->second;
  FlushFrame(page);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [_, frame_id] : page_table_) {
    auto page = pages_ + frame_id;
    FlushFrame(page);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(mutex_);

  frame_id_t frame_id;

  if (auto it = page_table_.find(page_id); it == page_table_.end()) {
    return true;
  } else {
    frame_id = it->second;
    page_table_.erase(it);
  }

  auto page = pages_ + frame_id;
  if (page->pin_count_ > 0) return false;
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  FlushFrame(page);
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
