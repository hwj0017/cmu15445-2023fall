//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  //   throw NotImplementedException("SeqScanExecutor is not implemented");
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto iter = table_info->table_->MakeIterator();
  for (; !iter.IsEnd(); ++iter) {
    rids_.push_back(iter.GetRID());
  }
  index = 0;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_predicate = plan_->filter_predicate_;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto table_heap = table_info->table_.get();
  //   auto schema = table_info->schema_;
  for (; index < rids_.size(); ++index) {
    auto pair = table_heap->GetTuple(rids_[index]);
    auto meta = pair.first;
    *rid = rids_[index];
    *tuple = pair.second;
    if (!meta.is_deleted_ &&
        (filter_predicate == nullptr || filter_predicate->Evaluate(tuple, table_info->schema_).GetAs<bool>())) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
