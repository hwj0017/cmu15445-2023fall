#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard guard{bpm_, page_};
  page_ = nullptr;
  return guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard guard{bpm_, page_};
  page_ = nullptr;
  return guard;
}
BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}
ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
