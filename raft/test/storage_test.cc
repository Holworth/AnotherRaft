#include "storage.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

static const std::string kStorageTestFileName = "/mnt/ssd1/test.log";
class StorageTest : public ::testing::Test {
  static const size_t kMaxDataSize = 16 * 1024;

 public:
  // Remove created log file in case that created log affect next test
  void Clear() {
    // std::filesystem::remove(kStorageTestFileName);
    remove(kStorageTestFileName.c_str());
  }

  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    int rand_size = 0;
    if (max_len == min_len) {
      rand_size = max_len;
    } else {
      auto rand_size = rand() % (max_len - min_len) + min_len;
    }
    // printf("[Generate Random Data Slice: size=%d]\n", rand_size);
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

TEST_F(StorageTest, DISABLED_TestPersistRaftState) {
  Clear();  // Clear existed files so that it won't affect current status
  const int kTestRun = 100;
  for (int i = 1; i <= kTestRun; ++i) {
    // Open storage and write some thing, then close it
    auto storage = FileStorage::Open(kStorageTestFileName);
    storage->PersistState(Storage::PersistRaftState{true, static_cast<raft_term_t>(i),
                                                    static_cast<raft_node_id_t>(i)});
    delete storage;

    storage = FileStorage::Open(kStorageTestFileName);
    auto state = storage->PersistState();
    ASSERT_TRUE(state.valid);
    ASSERT_EQ(state.persisted_term, i);
    ASSERT_EQ(state.persisted_vote_for, i);
  }
  Clear();
}

TEST_F(StorageTest, DISABLED_TestPersistLogEntries) {
  Clear();
  const size_t kPutCnt = 10000;
  auto sets = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets[i]);
  }
  auto storage = FileStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  // Reopen
  storage = FileStorage::Open(kStorageTestFileName);
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

  storage = FileStorage::Open(kStorageTestFileName);
  EXPECT_EQ(storage->LastIndex(), kPutCnt);

  auto state = storage->PersistState();
  EXPECT_TRUE(state.valid);
  EXPECT_EQ(state.persisted_term, 1);
  EXPECT_EQ(state.persisted_vote_for, 1);

  Clear();
}

TEST_F(StorageTest, DISABLED_TestOverwriteLogEntries) {
  Clear();
  const size_t kPutCnt = 10000;

  // Stage1: Persist and check all old entries
  auto sets1 = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets1[i]);
  }

  auto storage = FileStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  // Reopen
  storage = FileStorage::Open(kStorageTestFileName);
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
  storage = FileStorage::Open(kStorageTestFileName);
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
//
TEST_F(StorageTest, DISABLED_TestDeleteEntries) {
  Clear();
  const raft_index_t kPutCnt = 10000;
  const raft_index_t kLastIndex = kPutCnt / 2;
  auto sets = GenerateRandomEntrySets(kPutCnt);
  std::vector<LogEntry> entries;
  for (raft_index_t i = 1; i <= kPutCnt; ++i) {
    entries.push_back(sets[i]);
  }

  auto storage = FileStorage::Open(kStorageTestFileName);
  storage->PersistEntries(1, kPutCnt, entries);
  delete storage;

  storage = FileStorage::Open(kStorageTestFileName);
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

  storage = FileStorage::Open(kStorageTestFileName);
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

TEST_F(StorageTest, TestPersistencePerformance) {
  Clear();
  const int kPutCnt = 100;
  const size_t kSize = 2 * 1024 * 1024;  // 2MB
  auto storage = FileStorage::Open(kStorageTestFileName);
  std::vector<uint64_t> latency;
  for (int i = 1; i <= kPutCnt; ++i) {
    LogEntry ent;
    ent.SetIndex(static_cast<raft_index_t>(i));
    ent.SetTerm(1);
    ent.SetType(kNormal);
    ent.SetCommandData(GenerateRandomSlice(kSize, kSize));

    std::vector<LogEntry> sets = {ent};
    auto start = std::chrono::high_resolution_clock::now();
    storage->PersistEntries(i, i, sets);
    auto end = std::chrono::high_resolution_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    latency.push_back(dura.count());
  }

  // Deal with collected data
  uint64_t latency_sum = 0;
  std::for_each(latency.begin(), latency.end(),
                [&latency_sum](uint64_t n) { latency_sum += n; });
  printf("[Average Persistence Latency = %llu us]\n", latency_sum / latency.size());
  printf("[Max     Persistence Latency = %llu us]\n",
         *std::max_element(latency.begin(), latency.end()));
  std::sort(latency.begin(), latency.end());
  std::reverse(latency.begin(), latency.end());
  uint64_t top_latency_sum = 0;
  int top_cnt = latency.size() / 10;
  std::for_each(latency.begin(), latency.begin() + top_cnt,
                [&top_latency_sum](uint64_t n) { top_latency_sum += n; });
  printf("[Top10 average Latency = %llu us]\n", top_latency_sum / top_cnt);

  delete storage;
  Clear();
}

TEST_F(StorageTest, TestPersistRaftStatePerformance) {
  Clear();
  const int kPutCnt = 1000000;
  auto storage = FileStorage::Open("/mnt/ssd1/test.log");
  std::vector<uint64_t> latency;
  latency.reserve(kPutCnt);

  for (int i = 1; i <= kPutCnt; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    storage->PersistState(
        Storage::PersistRaftState{true, static_cast<raft_term_t>(i), 0});
    auto end = std::chrono::high_resolution_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    latency.push_back(dura.count());
  }

  // Deal with collected data
  uint64_t latency_sum = 0;
  std::for_each(latency.begin(), latency.end(),
                [&latency_sum](uint64_t n) { latency_sum += n; });
  printf("[Average Persistence Latency = %llu us]\n", latency_sum / latency.size());
  printf("[Max     Persistence Latency = %llu us]\n",
         *std::max_element(latency.begin(), latency.end()));
  std::sort(latency.begin(), latency.end());
  std::reverse(latency.begin(), latency.end());
  uint64_t top_latency_sum = 0;
  int top_cnt = latency.size() / 10;
  std::for_each(latency.begin(), latency.begin() + top_cnt,
                [&top_latency_sum](uint64_t n) { top_latency_sum += n; });
  printf("[Top10 average Latency = %llu us]\n", top_latency_sum / top_cnt);

  Clear();
}

}  // namespace raft
