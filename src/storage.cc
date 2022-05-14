#include "storage.h"

#include <cassert>

#include "RCF/external/asio/asio/detail/descriptor_ops.hpp"
#include "serializer.h"

static inline size_t alignment(size_t size, size_t align) {
  return ((size - 1) / align + 1) * align;
}

namespace raft {
void PersistStorage::persistHeader() {
  if (!valid_header_) {
    return;
  }
  assert(file_ != nullptr);
  this->file_->seekp(0);
  this->file_->write(reinterpret_cast<char*>(&header_), kHeaderSize);
}

void PersistStorage::PersistEntries(raft_index_t lo, raft_index_t hi,
                                    const std::vector<LogEntry>& batch) {
  auto ser = Serializer::NewSerializer();
  auto check_raft_index = lo;
  for (const auto& ent : batch) {
    auto write_buf_size = alignment(ser.getSerializeSize(ent), 8);
    if (write_buf_size > this->buf_size) {
      delete[] this->buf_;
      buf_ = new char[write_buf_size];
    }
    ser.serialize_logentry_helper(&ent, this->buf_);

    this->file_->seekp(header_.write_off);
    this->file_->write(this->buf_, write_buf_size);
    header_.write_off += write_buf_size;
    header_.last_off = header_.write_off;

    assert(check_raft_index == ent.Index());
    check_raft_index++;

    if (ent.Index() > header_.lastLogIndex) {
      header_.lastLogIndex = ent.Index();
      header_.lastLogTerm = ent.Term();
    }
  }
  persistHeader();
}

// Note: This method should be called as less as possible since it scans through the
// whole log file and filter those invalid or old log entries
void PersistStorage::LogEntries(std::vector<LogEntry>* entries) {
  auto der = Serializer::NewSerializer();
  auto last_index = header_.lastLogIndex;
  entries->reserve(last_index);
  this->file_->seekg(kHeaderSize);

  auto read_off = kHeaderSize;

  // Read log entry one by one
  while (true) {
    if (read_off + sizeof(LogEntry) > header_.last_off) {
      break;
    }

    // TODO: We might parse multiple entries within one buffer read
    file_->seekg(read_off);
    file_->read(buf_, buf_size);

    LogEntry ent;
    auto next = der.deserialize_logentry_helper(buf_, &ent);
    read_off += (next - buf_);
    if (ent.Index() <= header_.lastLogIndex) {
      (*entries)[ent.Index()] = ent;
    }
  }
}

PersistStorage* PersistStorage::Open(const std::string& logname) {
  auto storage = new PersistStorage;
  storage->file_ =
      new std::fstream(logname, std::fstream::in | std::fstream::out | std::fstream::app);

  // Read meta data(Header) into memory
  storage->file_->read(reinterpret_cast<char*>(&storage->header_), kHeaderSize);
  if (storage->file_->gcount() < kHeaderSize) { // No data yet, do initialization
    storage->header_.lastLogIndex = 0;
    storage->header_.lastLogTerm = 0;
    storage->header_.write_off = kHeaderSize;
    storage->header_.read_off = kHeaderSize;
    storage->header_.last_off = kHeaderSize;
    storage->valid_header_ = true;
    storage->persistHeader();
  }
  return storage;
}

}  // namespace raft
