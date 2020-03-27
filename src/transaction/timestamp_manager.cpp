#include "transaction/timestamp_manager.h"
#include <algorithm>
#include <vector>
#include <transaction/timestamp_manager.h>

namespace terrier::transaction {

timestamp_t TimestampManager::OldestTransactionStartTime() {
  std::vector<timestamp_t> mins;
  for (size_t i = 0; i < thread_count_; i++) {
    {
      common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_[i].latch_);
      const auto &oldest_txn = std::min_element(curr_running_txns_[i].cbegin(), curr_running_txns_[i].cend());
      if (oldest_txn != curr_running_txns_[i].end()) mins.push_back(*oldest_txn);
    }
  }
  const auto &oldest_txn = std::min_element(mins.cbegin(), mins.cend());
  timestamp_t result = (oldest_txn != mins.end()) ? *oldest_txn : time_.load();
  cached_oldest_txn_start_time_.store(result);  // Cache the timestamp
  return result;
}

timestamp_t TimestampManager::CachedOldestTransactionStartTime() { return cached_oldest_txn_start_time_.load(); }

void TimestampManager::RemoveTransaction(timestamp_t timestamp, worker_id_t worker_id) {
  const auto idx = static_cast<unsigned int>(worker_id) % thread_count_;
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_[idx].latch_);
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_[idx].erase(timestamp);
  TERRIER_ASSERT(ret == 1, "erased timestamp did not exist");
}

void TimestampManager::RemoveTransactions(const std::vector<terrier::transaction::timestamp_t> &timestamps, const std::vector<worker_id_t> &worker_ids) {
  for (size_t i = 0; i < timestamps.size(); i++) {
    const auto idx = static_cast<unsigned int>(worker_ids[i]) % thread_count_;
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_[idx].latch_);
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_[idx].erase(timestamps[i]);
    TERRIER_ASSERT(ret == 1, "erased timestamp did not exist");
  }
}

}  // namespace terrier::transaction
