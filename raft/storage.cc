#include "storage.h"

#include <sys/fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <ios>

static inline size_t alignment(size_t size, size_t align) {
  return ((size - 1) / align + 1) * align;
}

namespace raft {
void PersistStorage::persistHeader() {
  assert(file_ != nullptr);
  if (file_->eof()) {
    file_->clear();
  }
  this->file_->seekp(0, std::ios::beg);
  this->file_->write(reinterpret_cast<char*>(&header_), kHeaderSize);
  // this->file_->flush();
  this->file_->sync();
}

void PersistStorage::PersistEntries(raft_index_t lo, raft_index_t hi,
                                    const std::vector<LogEntry>& batch) {
  auto ser = Serializer::NewSerializer();
  auto check_raft_index = lo;
  for (const auto& ent : batch) {
    // auto write_buf_size = alignment(ser.getSerializeSize(ent), 8);
    auto write_buf_size = ser.getSerializeSize(ent);
    if (this->buf_ == nullptr || write_buf_size > this->buf_size) {
      delete[] this->buf_;
      buf_ = new char[write_buf_size];
      this->buf_size = write_buf_size;
    }
    ser.serialize_logentry_helper(&ent, this->buf_);

    // Clear bad flags so that pointer can be moved
    if (file_->eof()) {
      file_->clear();
    }

    this->file_->seekp(header_.write_off);
    this->file_->write(this->buf_, write_buf_size);
    this->file_->sync();
    // printf("write data at %d, len %d\n", header_.write_off, write_buf_size);
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
  entries->clear();
  entries->resize(last_index + 1);
  this->file_->seekg(kHeaderSize);

  auto read_off = kHeaderSize;

  // Read log entry one by one
  while (true) {
    if (read_off + sizeof(LogEntry) > header_.last_off) {
      break;
    }

    // NOTE: For file stream, when the file pointer reaches the end of file, we
    // can not move the file pointer any more. Thus we have to call clear
    if (file_->eof()) {
      file_->clear();
    }

    // TODO: We might parse multiple entries within one buffer read
    file_->seekg(read_off, std::ios::beg);
    file_->read(buf_, buf_size);

    LogEntry ent;
    auto next = der.deserialize_logentry_helper(buf_, &ent);
    read_off += der.getSerializeSize(ent);
    if (ent.Index() <= header_.lastLogIndex) {
      (*entries)[ent.Index()] = ent;
    }
  }
}

PersistStorage* PersistStorage::Open(const std::string& logname) {
  auto storage = new PersistStorage;

  // Logfile not exist, create it first
  if (!std::filesystem::exists(logname)) {
    auto file_ptr = new std::fstream(logname, std::fstream::out);
    storage->file_ = file_ptr;
    storage->header_.lastLogIndex = 0;
    storage->header_.lastLogTerm = 0;
    storage->header_.write_off = kHeaderSize;
    storage->header_.read_off = kHeaderSize;
    storage->header_.last_off = kHeaderSize;

    file_ptr->seekp(0, std::ios_base::beg);
    file_ptr->write(reinterpret_cast<char*>(&storage->header_), kHeaderSize);
    file_ptr->flush();
    file_ptr->close();
    delete file_ptr;
  }

  storage->file_ =
      new std::fstream(logname, std::fstream::in | std::fstream::out | std::fstream::ate);

  // Read meta data(Header) into memory
  storage->file_->seekg(0, std::ios::beg);
  storage->file_->read(reinterpret_cast<char*>(&storage->header_), kHeaderSize);
  auto read_cnt = storage->file_->gcount();
  if (read_cnt < kHeaderSize) {  // No data yet, do initialization
    storage->persistHeader();
  }

  // Create internal buffer
  storage->buf_ = new char[kInitBufSize];
  storage->buf_size = kInitBufSize;

  return storage;
}

FileStorage* FileStorage::Open(const std::string& filename) {
  int fd = ::open(filename.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    return nullptr;
  }
  auto ret = new FileStorage();
  ret->AllocateNewInternalBuffer(FileStorage::kInitBufSize);
  ret->fd_ = fd;
  if (auto size = ::read(fd, &(ret->header_), kHeaderSize) < kHeaderSize) {
    // No complete header
    ret->InitializeHeaderOnCreation();
    ret->PersistHeader();
  }
  return ret;
}

void FileStorage::Close(FileStorage* file) { delete file; }

void FileStorage::PersistEntries(raft_index_t lo, raft_index_t hi,
                                 const std::vector<LogEntry>& batch) {
  if (lo > hi) {
    return;
  }
  auto ser = Serializer::NewSerializer();
  auto check_raft_index = lo;
  for (const auto& ent : batch) {
    auto write_size = ser.getSerializeSize(ent);
    if (!this->buf_ || write_size > this->buf_size_) {
      AllocateNewInternalBuffer(write_size);
    }
    ser.serialize_logentry_helper(&ent, this->buf_);
    Append(this->buf_, write_size);

    assert(check_raft_index == ent.Index());
    check_raft_index++;

    MaybeUpdateLastIndexAndTerm(ent.Index(), ent.Term());
  }
  PersistHeader();
}

void FileStorage::LogEntries(std::vector<LogEntry>* entries) {
  auto der = Serializer::NewSerializer();
  auto last_index = header_.lastLogIndex;
  entries->clear();
  entries->resize(last_index + 1);

  auto read_off = kHeaderSize;
  // Read log entry one by one
  while (true) {
    if (read_off + sizeof(LogEntry) > header_.last_off) {
      break;
    }
    auto off = lseek(fd_, read_off, SEEK_SET);
    ::read(fd_, buf_, this->buf_size_);

    LogEntry ent;
    auto next = der.deserialize_logentry_helper(buf_, &ent);
    read_off += der.getSerializeSize(ent);
    if (ent.Index() <= header_.lastLogIndex) {
      (*entries)[ent.Index()] = ent;
    }
  }
}

}  // namespace raft
