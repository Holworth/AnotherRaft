#pragma once
#include <string>

#include "rsm.h"
#include "storage_engine.h"

namespace kv {
class KVRsm : public raft::Rsm {
 public:
  // Constructor: Opening an existed database or create a new one if the
  // specified database does not exist
  KVRsm(const std::string& dbname) : db_(StorageEngine::Default(dbname)) {}
  ~KVRsm() { delete db_; }

  KVRsm(const KVRsm&) = delete;
  KVRsm operator=(const KVRsm&) = delete;

  // Raft state machine apply interface override. The KV state machine parses
  // the apply operation type and Put a key-value pair into local key-value
  // store, or delete specified key
  void ApplyLogEntry(raft::LogEntry entry) override;

  bool Get(const std::string& key, std::string* value) { return db_->Get(key, value); }

 private:
  StorageEngine* db_;  // Use leveldb to perform KV operations
};
}  // namespace kv
