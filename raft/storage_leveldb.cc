#include "storage_leveldb.h"

#include <unistd.h>

#include <cstring>
namespace raft {

constexpr const int kOpenBaseFlags = O_CLOEXEC;

WritableFile* WritableFile::OpenWritableFile(const std::string& filename) {
  auto ret = new WritableFile();
  auto fd =
      ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
  ret->fd_ = fd;
  ret->filename_ = filename;
  ret->pos_ = 0;
  return ret;
}

void WritableFile::Append(const char* data, size_t size) {
  auto copy_size = std::min(size, kWritableFileBufferSize - pos_);
  std::memcpy(buf_ + pos_, data, copy_size);
  pos_ += copy_size;
  data += copy_size;
  size -= copy_size;
  if (size == 0) {
    return;
  }
  FlushBuffer();
  if (size < kWritableFileBufferSize) {
    std::memcpy(buf_, data, size);
    pos_ += size;
    return;
  } else {
    WriteUnbuffered(data, size);
  }
}

void WritableFile::WriteUnbuffered(const char* data, size_t size) {
  while (size > 0) {
    ssize_t write_result = ::write(fd_, data, size);
    if (write_result < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return;
    }
    data += write_result;
    size -= write_result;
  }
}

void WritableFile::FlushBuffer() {
  WriteUnbuffered(buf_, pos_);
  pos_ = 0;
  return;
}

void WritableFile::Sync() {
  FlushBuffer();
  SyncFd(fd_, filename_);
}

void WritableFile::Flush() {
  FlushBuffer();
}

void WritableFile::SyncFd(int fd, const std::string& filename) { ::fsync(fd); }

StorageLevelDB* StorageLevelDB::Open(const std::string& logname) {
  auto storage = new StorageLevelDB();
  storage->logfile_ = WritableFile::OpenWritableFile(logname);
  storage->buf_ = nullptr;
  storage->bufsize_ = 0;
  return storage;
}

void StorageLevelDB::PersistEntries(raft_index_t lo, raft_index_t hi,
                                    const std::vector<LogEntry>& batch) {
  auto ser = Serializer::NewSerializer();
  auto check_raft_index = lo;
  for (const auto& ent : batch) {
    auto write_buf_size = ser.getSerializeSize(ent);
    if (write_buf_size > bufsize_ || !buf_) {
      AllocateNewInternalBuffer(write_buf_size);
    }
    ser.serialize_logentry_helper(&ent, this->buf_);
    logfile_->Append(this->buf_, write_buf_size);
  }
  logfile_->Sync();
}

}  // namespace raft
