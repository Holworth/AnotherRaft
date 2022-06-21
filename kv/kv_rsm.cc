#include "kv_rsm.h"
#include "kv_format.h"
#include "type.h"
namespace kv {
void KVStateMachine::ApplyLogEntry(raft::LogEntry entry) {
  Request req;
  RawBytesToRequest(entry.CommandData().data(), &req);
  // Currently only deal with Put & Delete request
  switch (req.type) {
    case kPut: 
      db_->Put(req.key, req.value);
      break;
    case kDelete:
      db_->Delete(req.key);
      break;
    default:
      break;
  }
}
}
