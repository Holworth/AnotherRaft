#include "storage.h"
namespace raft {
class WritableFile {
  static constexpr int kWritableFileBufferSize = 4 * 1024;

 public:
  static WritableFile* OpenWritableFile(const std::string&);

  void Append(const char* data, size_t size);
  void Sync();
  void Flush();

 private:
  void FlushBuffer();
  void WriteUnbuffered(const char* data, size_t size);
  void SyncFd(int fd, const std::string& filename);

 private:
  int fd_;
  std::string filename_;
  char buf_[kWritableFileBufferSize];
  size_t pos_;
};

class StorageLevelDB : public Storage {
 public:
  static StorageLevelDB* Open(const std::string& logname);

  raft_index_t LastIndex() const {
    // TODO
    return 0;
  };

  PersistRaftState PersistState() const {
    // TODO
    return PersistRaftState{true, 0, 0};
  }

  void PersistState(const PersistRaftState& state){
      // TODO
  };

  void LogEntries(std::vector<LogEntry>* entries) {
    // TODO
  }
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry>& batch);

  void SetLastIndex(raft_index_t raft_index){
      // TODO
  };

 private:
  WritableFile* logfile_;
  char* buf_;
  size_t bufsize_;
};

}  // namespace raft
