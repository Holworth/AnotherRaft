#include "channel.h"
namespace kv {

Channel* Channel::NewChannel(size_t capacity) { return new Channel(capacity); }

Channel::Channel(size_t capacity) : queue_(capacity) {}

void Channel::ApplyLogEntry(raft::LogEntry entry) { queue_.Push(entry); }

raft::LogEntry Channel::Pop() { return queue_.Pop(); }
}  // namespace kv
