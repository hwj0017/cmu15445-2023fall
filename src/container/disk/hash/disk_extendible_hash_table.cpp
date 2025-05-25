//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  bpm_->NewPageGuarded(&header_page_id_);
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw Exception("Failed to create DiskExtendibleHashTable");
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  assert(guard);
  auto header_page = reinterpret_cast<const ExtendibleHTableHeaderPage *>(guard.GetData());
  auto directory_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(Hash(key)));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  guard = bpm_->FetchPageRead(directory_page_id);
  assert(guard);
  auto directory_page = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(guard.GetData());
  auto bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(Hash(key)));
  guard = bpm_->FetchPageRead(bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  assert(guard);
  auto bucket_page = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(guard.GetData());
  result->resize(1);
  return bucket_page->Lookup(key, result->at(0), cmp_);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto key_hash = Hash(key);
  int32_t directory_page_id;
  WritePageGuard directory_guard;
  ExtendibleHTableDirectoryPage *directory_page;
  // get header page
  {
    auto guard = bpm_->FetchPageWrite(header_page_id_);
    assert(bool(guard));
    auto header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(guard.GetDataMut());
    auto directory_page_index = header_page->HashToDirectoryIndex(key_hash);
    directory_page_id = header_page->GetDirectoryPageId(directory_page_index);
    if (directory_page_id == INVALID_PAGE_ID) {
      return InsertToNewDirectory(header_page, directory_page_index, key_hash, key, value);
    } else {
      directory_guard = bpm_->FetchPageWrite(directory_page_id);
      assert(directory_guard);
      directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());
    }
  }
  // directory_page is not empty
  auto bucket_page_index = directory_page->HashToBucketIndex(key_hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_index);
  assert(bucket_page_id != INVALID_PAGE_ID);
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  assert(bucket_guard);
  auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
  // is exist?
  if (bucket_page->Contain(key, cmp_)) {
    return false;
  }
  if (bucket_page->IsFull()) {
    directory_page->IncrLocalDepth(bucket_page_index);
    if (directory_page->GetLocalDepth(bucket_page_index) > directory_page->GetGlobalDepth()) {
      directory_page->IncrGlobalDepth();
    }
    uint32_t new_index = directory_page->HashToBucketIndex(key_hash);
    return InsertToNewBucket(directory_page, new_index, key, value);
  }
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  int32_t directory_page_id;
  auto directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());
  directory_page->Init(directory_max_depth_);
  if (!InsertToNewBucket(directory_page, 0, key, value)) {
    return false;
  };
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  int32_t bucket_page_id;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
  bucket_page->Init(bucket_max_size_);
  assert(bucket_page->Insert(key, value, cmp_));
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  if (bucket_idx == 0) {
    // first page
    return true;
  }
  auto image_page_index = directory->GetSplitImageIndex(bucket_idx);
  auto image_page_id = directory->GetBucketPageId(image_page_index);
  assert(image_page_id != INVALID_PAGE_ID);
  directory->SetBucketPageId(bucket_idx, image_page_id);

  auto image_guard = bpm_->FetchPageWrite(image_page_id);
  auto image_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(image_guard.GetDataMut());
  // merge
  auto local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
  for (uint32_t i = 0; i < image_page->Size();) {
    auto &[k, v] = image_page->EntryAt(i);
    if (Hash(k) & local_depth_mask) {
      bucket_page->Insert(k, v, cmp_);
      image_page->RemoveAt(i);
      continue;
    }
    ++i;
  }
  if (bucket_page->IsFull()) {
    directory->IncrLocalDepth(bucket_idx);
    InsertToNewBucket(directory, bucket_idx, key, value);
  }

  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket) {
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    auto &[k, v] = old_bucket->EntryAt(i);
    new_bucket->Insert(k, v, cmp_);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto key_hash = Hash(key);
  assert(header_page_id_ != INVALID_PAGE_ID);
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  assert(header_guard);
  auto header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_guard.GetDataMut());
  auto directory_index = header_page->HashToDirectoryIndex(key_hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  // get directory page and bucket page
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  assert(directory_guard);
  auto directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());
  auto bucket_page_index = directory_page->HashToBucketIndex(key_hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  assert(bucket_guard);
  auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
  // is exist?

  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  //
  if (directory_page->GetGlobalDepth() == 0) {
    if (bucket_page->IsEmpty()) {
      bucket_guard.Drop();
      directory_guard.Drop();
      header_page->SetDirectoryPageId(directory_index, INVALID_PAGE_ID);
      bpm_->DeletePage(bucket_page_id);
      bpm_->DeletePage(directory_page_id);
    }
    return true;
  }
  // global_depth!=0
  auto image_page_index = directory_page->GetSplitImageIndex(bucket_page_index);
  auto image_page_id = directory_page->GetBucketPageId(image_page_index);
  assert(image_page_id != INVALID_PAGE_ID);
  auto image_page_guard = bpm_->FetchPageWrite(image_page_id);
  auto image_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(image_page_guard.GetDataMut());
  if (bucket_page->Size() + image_page->Size() > bucket_max_size_) {
    return true;
  }
  if (bucket_page->Size() < image_page->Size()) {
    MigrateEntries(bucket_page, image_page);
    bucket_guard.Drop();
    bpm_->DeletePage(bucket_page_id);
    directory_page->DecrLocalDepth(bucket_page_index);
  } else {
    MigrateEntries(image_page, bucket_page);
    image_page_guard.Drop();
    bpm_->DeletePage(image_page_id);
    directory_page->DecrLocalDepth(image_page_index);
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
