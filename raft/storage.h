#pragma once
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "log_entry.h"
#include "raft_type.h"
#include "serializer.h"

namespace raft {

// Storage is an interface used for retriving (and store) persisted raft status
// including current term, voteFor, persisted log entries
class Storage {
 public:
  struct PersistRaftState {
    // Indicating if this state is valid, for example, when creating
    // a persister on first bootstrap, there is no valid raft state yet
    bool valid;
    raft_term_t persisted_term;
    raft_node_id_t persisted_vote_for;
  };

 public:
  // LastIndex returns the raft index of last persisted log entries, if
  // there is no valid log entry, returns 0
  virtual raft_index_t LastIndex() const = 0;

  // Return the persisted raft state. Mark valid field as false if there
  // is no valid raft state
  virtual PersistRaftState PersistState() const = 0;
  virtual void PersistState(const PersistRaftState& state) = 0;

  // Get all persisted but not discarded log entries and store them in a temporary
  // vector
  virtual void LogEntries(std::vector<LogEntry>* entries) = 0;

  // Persist log entries within specified range to storage, there might be old version
  // of data that has been persisted in storage, if so, old version of these entries
  // would be ignored when reading. However, upon reading, this may require us to scan
  // the whole log file
  virtual void PersistEntries(raft_index_t lo, raft_index_t hi,
                              const std::vector<LogEntry>& batch) = 0;

  // Persist the last index attribute would ignore any entries with higher index in
  // the next reading. It can be used when an entry deleting occurs
  virtual void SetLastIndex(raft_index_t raft_index) = 0;

  virtual ~Storage() = default;
};

// This class is only for unit test, it is used for simulating the behaviour
// of a persister by place them simply in memory. The test module may feel
// free to inherit this class and override corresponding methods
class MemStorage : public Storage {
 public:
  raft_index_t LastIndex() const override {
    if (!persisted_entries_.size()) {
      return 0;
    } else {
      return (persisted_entries_.end() - 1)->Index();
    }
  }

  PersistRaftState PersistState() const override {
    return {true, persisted_term_, persisted_vote_for_};
  }

  void PersistState(const PersistRaftState& state) override {}

  void LogEntries(std::vector<LogEntry>* entries) override {
    *entries = persisted_entries_;
  }

  // Should do nothing
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry>& batch) override {}

  void SetLastIndex(raft_index_t raft_index) override {}

 protected:
  raft_term_t persisted_term_;
  raft_node_id_t persisted_vote_for_;
  std::vector<LogEntry> persisted_entries_;
};

class FileStorage : public Storage {
 public:
  // Create or open an existed log file
  static FileStorage* Open(const std::string& name);
  static void Close(FileStorage* file);

  ~FileStorage() {
    delete[] buf_;
    ::close(fd_);
  }

  raft_index_t LastIndex() const override { return header_.lastLogIndex; };

  PersistRaftState PersistState() const override {
    return PersistRaftState{has_header_, header_.currentTerm, header_.voteFor};
  }

  void PersistState(const PersistRaftState& state) override {
    if (!state.valid) {
      return;
    }
    header_.voteFor = state.persisted_vote_for;
    header_.currentTerm = state.persisted_term;
    has_header_ = true;
    PersistHeader();
  };

  void LogEntries(std::vector<LogEntry>* entries) override;
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry>& batch) override;

  void SetLastIndex(raft_index_t raft_index) override {
    header_.lastLogIndex = raft_index;
    PersistHeader();
  };

 private:
  void AllocateNewInternalBuffer(size_t size) {
    if (this->buf_) {
      delete[] buf_;
    }

    this->buf_ = new char[size + 20];
    this->buf_size_ = size;
  }

  void PersistHeader() {
    lseek(fd_, 0, SEEK_SET);
    ::write(fd_, &header_, kHeaderSize);
    SyncFd(fd_);
  }

  // Append data of specified slice to the file
  void Append(const char* data, size_t size) {
    lseek(fd_, 0, SEEK_END);
    while (size > 0) {
      auto write_size = ::write(fd_, data, size);
      size -= write_size;
      data += write_size;
    }
  };

  void SyncFd(int fd) { ::fsync(fd); }

 private:
  struct Header {
    raft_node_id_t voteFor;
    raft_term_t currentTerm;
    raft_index_t lastLogIndex;
    raft_term_t lastLogTerm;
    size_t write_off;  // Move file write pointer to off position for next write
    size_t read_off;
    size_t last_off;  // Basically the file size, used to check if reaches the end
  };
  bool has_header_;

  Header header_;
  int fd_;
  char* buf_;
  size_t buf_size_;

  static constexpr size_t kInitBufSize = 16 * 1024 * 1024;  // 16MB
  static const size_t kHeaderSize = sizeof(Header);
};

// A storage implementation that persist log entries and raft state into log file
// to achive persistence. Note that each PersistStorage instance only creates one
// file and the maximum storage capacity depends on the maximum size of a file the
// operating system supports
class PersistStorage : public Storage {
 public:
  // Open an existed log file or create one, if it does not exist. Returns nullptr to
  // indicate that any error occured
  static PersistStorage* Open(const std::string& logname);

  ~PersistStorage() {
    persistHeader();
    this->file_->close();
    delete this->file_;
  }

 public:
  raft_index_t LastIndex() const { return header_.lastLogIndex; };

  PersistRaftState PersistState() const {
    return PersistRaftState{true, header_.currentTerm, header_.voteFor};
  }

  void PersistState(const PersistRaftState& state) {
    if (!state.valid) {
      return;
    }
    header_.voteFor = state.persisted_vote_for;
    header_.currentTerm = state.persisted_term;
    persistHeader();
  };

  void LogEntries(std::vector<LogEntry>* entries);
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry>& batch);

  void SetLastIndex(raft_index_t raft_index) {
    header_.lastLogIndex = raft_index;
    persistHeader();
  };

 private:
  void persistHeader();

 private:
  // The header occupies the first a few bytes of storage of the logfile, for recording
  // any necessary meta information about the whole log file
  struct Header {
    raft_node_id_t voteFor;
    raft_term_t currentTerm;
    raft_index_t lastLogIndex;
    raft_term_t lastLogTerm;
    size_t write_off;  // Move file write pointer to off position for next write
    size_t read_off;
    size_t last_off;  // Basically the file size, used to check if reaches the end
  };
  Header header_;
  std::fstream* file_;
  static const size_t kHeaderSize = sizeof(Header);
  char* buf_;  // Internal buffer for read and write
  size_t buf_size;
  static const size_t kInitBufSize = 1024 * 1024;
};

}  // namespace raft
