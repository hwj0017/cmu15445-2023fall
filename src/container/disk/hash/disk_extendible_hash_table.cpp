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
  auto head_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw Exception("Failed to create DiskExtendibleHashTable");
  }
  auto header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(head_guard.GetDataMut());
  header_page->Init(header_max_depth);
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
  // header is droped
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
  V res;
  auto got_value = bucket_page->Lookup(key, res, cmp_);
  if (got_value) {
    result->emplace_back(std::move(res));
  }
  return got_value;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto key_hash = Hash(key);
  WritePageGuard bucket_guard;
  {
    WritePageGuard directory_guard;
    // get header page
    {
      auto header_guard = bpm_->FetchPageWrite(header_page_id_);
      assert(bool(header_guard));
      auto header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_guard.GetDataMut());
      auto directory_page_index = header_page->HashToDirectoryIndex(key_hash);
      auto directory_page_id = header_page->GetDirectoryPageId(directory_page_index);
      if (directory_page_id == INVALID_PAGE_ID) {
        directory_guard = CreateDirectory(header_page, directory_page_index);
        if (!directory_guard) {
          return false;
        }
        header_guard.Drop();
      } else {
        directory_guard = bpm_->FetchPageWrite(directory_page_id);
        assert(directory_guard);
      }
    }
    auto directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());
    auto bucket_page_index = directory_page->HashToBucketIndex(key_hash);
    auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_index);
    if (bucket_page_id == INVALID_PAGE_ID) {
      // first bucket in directory
      bucket_guard = CreateBucket(directory_page, bucket_page_index);
      if (!bucket_guard) {
        return false;
      }
    } else {
      bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    }
    assert(bucket_guard);
    auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
    // is exist?
    if (bucket_page->Contain(key, cmp_)) {
      return false;
    }

    if (bucket_page->IsFull()) {
      auto max_depth_mask = directory_page->GetMaxDepthMask();
      auto max_depth_index = bucket_page_index & max_depth_mask;
      bool can_insert = false;
      for (uint32_t i = 0; i < bucket_page->Size(); ++i) {
        if ((Hash(bucket_page->EntryAt(i).first) & max_depth_mask) != max_depth_index) {
          can_insert = true;
          break;
        }
      }
      if (can_insert) {
        while (bucket_page->IsFull()) {
          if (auto global_depth = directory_page->GetGlobalDepth();
              directory_page->GetLocalDepth(bucket_page_index) == global_depth) {
            if (global_depth == directory_max_depth_) {
              return false;
            }
            directory_page->IncrGlobalDepth();
          }
          directory_page->IncrLocalDepth(bucket_page_index);

          auto new_bucket_guard = CreateBucket(directory_page, bucket_page_index);

          auto new_bucket_page =
              reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_guard.GetDataMut());
          SplitBucket(bucket_page, new_bucket_page, directory_page->GetLocalDepthMask(bucket_page_index),
                      bucket_page_index);
          bucket_guard = std::move(new_bucket_guard);
          bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
        }
      } else {
        return false;
      }
    }
  }
  auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::CreateDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_index)
    -> WritePageGuard {
  int32_t directory_page_id;
  auto directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  if (directory_page_id != INVALID_PAGE_ID) {
    auto directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());
    directory_page->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_index, directory_page_id);
  }
  return directory_guard;
}
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::CreateBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_index)
    -> WritePageGuard {
  int32_t bucket_page_id;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  if (bucket_page_id != INVALID_PAGE_ID) {
    auto bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
    bucket_page->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_index, bucket_page_id);
    // local_depth is not set
  }
  return bucket_guard;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                    ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                    uint32_t local_depth_mask, uint32_t new_bucket_idx) {
  assert(old_bucket->IsFull());
  assert(new_bucket->IsEmpty());
  auto new_hash = new_bucket_idx & new_bucket_idx;
  for (uint32_t i = 0; i < old_bucket->Size(); ++i) {
    auto &[k, v] = old_bucket->EntryAt(i);
    if ((Hash(k) & local_depth_mask) == new_hash) {
      new_bucket->Insert(k, v, cmp_);
      old_bucket->RemoveAt(i);
      // not ++i
      continue;
    }
  }
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
  for (uint32_t index = 0; index < old_bucket->Size(); ++index) {
    auto &[k, v] = old_bucket->EntryAt(index);
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
  // maybe header_page can drop
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
  // is removed
  if (directory_page->GetGlobalDepth() == 0) {
    if (bucket_page->IsEmpty()) {
      bucket_guard.Drop();
      directory_guard.Drop();
      // not set local_depth
      header_page->SetDirectoryPageId(directory_index, INVALID_PAGE_ID);
      bpm_->DeletePage(bucket_page_id);
      bpm_->DeletePage(directory_page_id);
    }
    return true;
  }
  // global_depth!=0
  while (directory_page->GetGlobalDepth() > 0) {
    auto image_page_index = directory_page->GetSplitImageIndex(bucket_page_index);
    // length is not same
    if (directory_page->GetLocalDepth(image_page_index) != directory_page->GetLocalDepth(bucket_page_index)) {
      break;
    }
    auto image_page_id = directory_page->GetBucketPageId(image_page_index);
    assert(image_page_id != INVALID_PAGE_ID);
    auto image_page_guard = bpm_->FetchPageWrite(image_page_id);
    assert(image_page_guard);
    auto image_page = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(image_page_guard.GetDataMut());
    // not need to merge
    if (bucket_page->Size() + image_page->Size() > bucket_max_size_) {
      break;
    }
    // merge
    MigrateEntries(image_page, bucket_page);
    image_page_guard.Drop();
    bpm_->DeletePage(image_page_id);
    directory_page->SetBucketPageId(image_page_index, bucket_page_id);
    directory_page->DecrLocalDepth(bucket_page_index);
    directory_page->DecrLocalDepth(image_page_index);
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
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
