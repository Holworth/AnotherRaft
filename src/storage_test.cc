#include "storage.h"

#include <filesystem>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

static const std::string kStorageTestFileName = "./test.log";
class StorageTest : public ::testing::Test {
  static const size_t kMaxDataSize = 16 * 1024;

 public:
  // Remove created log file in case that created log affect next test
  void Clear() { std::filesystem::remove(kStorageTestFileName); }

  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    auto rand_data = new char[rand_size];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }

  auto GenerateRandomLogEntry(raft_index_t raft_index, raft_term_t raft_term,
                              raft_entry_type type, bool generate_data) -> LogEntry {
    LogEntry ent;
    ent.SetTerm(raft_term);
    ent.SetIndex(raft_index);
    ent.SetSequence(rand());
    ent.SetType(type);
    if (generate_data) {
      switch (ent.Type()) {
        case raft::kNormal:
          ent.SetCommandData(GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
          break;
        case raft::kFragments:
          ent.SetNotEncodedSlice(GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
          ent.SetFragmentSlice(GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
          break;
        case raft::kTypeMax:
          assert(false);
      }
    }
    return ent;
  }

  // Generate a datasets containing log entries with index of range [1, raft_index]
  // Term are set to be 1 for simplicity
  auto GenerateRandomEntrySets(raft_index_t raft_index)
      -> std::unordered_map<raft_index_t, LogEntry> {
    std::unordered_map<raft_index_t, LogEntry> logs;
    for (raft_index_t i = 1; i <= raft_index; ++i) {
      auto type = static_cast<raft_entry_type>(i % raft::kTypeMax);
      logs.insert({i, GenerateRandomLogEntry(i, 1, type, true)});
    }
    return logs;
  }

 private:
  Storage* storage_;
};

TEST_F(StorageTest, TestPersistRaftState) {
  Clear();  // Clear existed files so that it won't affect current status
  const int kTestRun = 100;
  for (int i = 1; i <= kTestRun; ++i) {
    // Open storage and write some thing, then close it
    auto storage = PersistStorage::Open(kStorageTestFileName);
    storage->PersistState(Storage::PersistRaftState{true, static_cast<raft_term_t>(i),
                                                    static_cast<raft_node_id_t>(i)});
    delete storage;

    storage = PersistStorage::Open(kStorageTestFileName);
    auto state = storage->PersistState();
    EXPECT_TRUE(state.valid);
    EXPECT_EQ(state.persisted_term, i);
    EXPECT_EQ(state.persisted_vote_for, i);
  }
  Clear();
}

TEST_F(StorageTest, TestPersistLogEntries) {
  Clear();
  const size_t kPutCnt = 10000;
  auto sets = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets[i]);
  }
  auto storage = PersistStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  // Reopen
  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  // Get all entries
  std::vector<LogEntry> read_ents;
  storage->LogEntries(&read_ents);
  EXPECT_EQ(read_ents.size() - 1, kPutCnt);
  // Check recovered data are equal
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    ASSERT_EQ(read_ents[i], sets[i]);
  }

  // Modify raft state after check log entries are ok
  storage->PersistState(Storage::PersistRaftState{true, 1, 1});
  delete storage;

  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  auto state = storage->PersistState();
  EXPECT_TRUE(state.valid);
  EXPECT_EQ(state.persisted_term, 1);
  EXPECT_EQ(state.persisted_vote_for, 1);

  Clear();
}

TEST_F(StorageTest, TestOverwriteLogEntries) {
  Clear();
  const size_t kPutCnt = 10000;

  // Stage1: Persist and check all old entries 
  auto sets1 = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets1[i]);
  }

  auto storage = PersistStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  // Reopen
  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  // Get all entries
  std::vector<LogEntry> read_ents;
  storage->LogEntries(&read_ents);
  EXPECT_EQ(read_ents.size() - 1, kPutCnt);
  // Check recovered data are equal
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    ASSERT_EQ(read_ents[i], sets1[i]);
  }

  // Stage2: Generate another data of sets and persist and them
  auto sets2 = GenerateRandomEntrySets(kPutCnt);
  entries.clear();
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets2[i]);
  }
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  // Check new entries 
  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  // Get all entries
  storage->LogEntries(&read_ents);
  EXPECT_EQ(read_ents.size() - 1, kPutCnt);
  // Check recovered data are equal
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    ASSERT_EQ(read_ents[i], sets2[i]);
  }
  Clear();
}

TEST_F(StorageTest, TestDeleteEntries) {
  Clear();
  const raft_index_t kPutCnt = 10;
  const raft_index_t kLastIndex = kPutCnt / 2;
  auto sets = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets[i]);
  }

  auto storage = PersistStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  std::vector<LogEntry> read_ents;

  // Check all entries
  storage->LogEntries(&read_ents);
  EXPECT_EQ(read_ents.size() - 1, kPutCnt);
  // Check recovered data are equal
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    ASSERT_EQ(read_ents[i], sets[i]);
  }

  // Discard log entries with higher index
  storage->SetLastIndex(kLastIndex);
  delete storage;

  storage = PersistStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kLastIndex);

  // Check all entries
  storage->LogEntries(&read_ents);
  EXPECT_EQ(read_ents.size() - 1, kLastIndex);
  // Check recovered data are equal
  for (raft_index_t i = 1; i <= kLastIndex; ++i) {
    ASSERT_EQ(read_ents[i], sets[i]);
  }
  Clear();
}

}  // namespace raft