#pragma once
#include "concurrent_queue.h"
#include "log_entry.h"
#include "rsm.h"
namespace kv {
class Channel : public raft::Rsm {
 public:
  static Channel* NewChannel(size_t capacity);

  Channel(size_t capacity);
  ~Channel() = default;

  void ApplyLogEntry(raft::LogEntry entry) override;
  raft::LogEntry Pop();

 private:
  ConcurrentQueue<raft::LogEntry> queue_;
};
}  // namespace kv
