#include "kv_rsm.h"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "kv_format.h"
#include "log_entry.h"
#include "raft_type.h"
#include "type.h"
namespace kv {
class KVRsmTest : public ::testing::Test {
 public:
  void GenerateRandomDataSet(int value_size, int ent_cnt) {
    for (int i = 1; i <= ent_cnt; ++i) {
      dataset_.insert({std::to_string(i), GenerateRandomKeyValue(value_size)});
    }
  }

 public:
  std::string GenerateRandomKeyValue(int value_size) {
    std::string val;
    val.resize(value_size);

    // Fill in with random value
    for (int i = 0; i < value_size; ++i) {
      val[i] = rand() % 256;
    }
    return val;
  }

  void StartTest(const std::string& dbname) { kv_ = new KVStateMachine(dbname); }

  void ClearTestContext(const std::string& dbname) {
    delete kv_;
    std::filesystem::remove_all(dbname);
  }

  void Put(const std::string& key, const std::string& value) {
    DealWithRequest(kPut, key, value, nullptr);
  }

  void Delete(const std::string& key) {
    DealWithRequest(kDelete, key, std::string(""), nullptr); }

  bool Get(const std::string& key, std::string* value) {
    return DealWithRequest(kGet, key, "", value);
  }

  bool DealWithRequest(RequestType type, const std::string& key, const std::string& value,
                       std::string* ret) {
    if (type == kGet && ret != nullptr) {
      auto find = kv_->Get(key, ret);
      if (!find) {
        *ret = "";
        return false;
      }
      return true;
    }
    Request req = Request{type, 0, 0, key, value};

    auto size = GetRawBytesSizeForRequest(req);
    auto data = new char[size];
    RequestToRawBytes(req, data);

    raft::LogEntry ent;
    ent.SetType(raft::kNormal);
    ent.SetCommandData(raft::Slice(data, size));
    ent.SetStartOffset(size);

    kv_->ApplyLogEntry(ent);

    delete[] data;
    return true;
  }

 public:
  KVStateMachine* kv_;
  std::map<std::string, std::string> dataset_;
};

TEST_F(KVRsmTest, TestSimplePutAndGet) {
  StartTest("./tmprsmtest");
  auto expect_val = GenerateRandomKeyValue(64);
  std::string value;
  Put(std::to_string(1), expect_val);
  ASSERT_TRUE(Get(std::to_string(1), &value));
  ASSERT_EQ(value, expect_val);
  ClearTestContext("./tmprsmtest");
}

TEST_F(KVRsmTest, TestABatchOfWrite) {
  StartTest("./tmprsmtest");
  GenerateRandomDataSet(4 * 1024, 100000);
  std::string value;
  for (const auto&[key, value] : dataset_) {
    Put(key, value);
    // std::printf("Put %s done\n", key.c_str());
  }
  for (const auto&[key, expect_value] : dataset_) {
    ASSERT_TRUE(Get(key, &value));
    ASSERT_EQ(value, expect_value);
  }
  ClearTestContext("./tmprsmtest");
}

TEST_F(KVRsmTest, TestOverWriteValues) {
  StartTest("./tmprsmtest");
  std::string value;

  // Phase1: Insert some key-value pairs
  GenerateRandomDataSet(4 * 1024, 100000);
  for (const auto&[key, expect_value] : dataset_) {
    Put(key, expect_value);
  }
  for (const auto&[key, expect_value] : dataset_) {
    ASSERT_TRUE(Get(key, &value));
    ASSERT_EQ(value, expect_value);
  }

  // Phase2: Regenerate the dataset and insert new values
  GenerateRandomDataSet(8 * 1024, 100000);
  for (const auto&[key, expect_value] : dataset_) {
    Put(key, expect_value);
  }
  for (const auto&[key, expect_value] : dataset_) {
    ASSERT_TRUE(Get(key, &value));
    ASSERT_EQ(value, expect_value);
  }
  ClearTestContext("./tmprsmtest");
}

TEST_F(KVRsmTest, TestSimpleDeleteEntries) {
  StartTest("./tmprsmtest");
  std::string value;
  auto expect_value = GenerateRandomKeyValue(64);

  Put(std::to_string(1), expect_value);
  ASSERT_TRUE(Get(std::to_string(1), &value));

  // Delete this entry
  Delete(std::to_string(1));
  ASSERT_FALSE(Get(std::to_string(1), &value));

  ClearTestContext("./tmprsmtest");
}

TEST_F(KVRsmTest, TestRandomlyDeleteEntries) {
  StartTest("./tmprsmtest");

  std::string value;

  GenerateRandomDataSet(4 * 1024, 100000);

  // Phase1: Put all values into kv
  for (const auto&[key, expect_value] : dataset_) {
    Put(key, expect_value);
  }
  for (const auto&[key, expect_value] : dataset_) {
    ASSERT_TRUE(Get(key, &value));
    ASSERT_EQ(value, expect_value);
  }

  int cnt = 0;
  std::vector<std::string> removed_keys;
  // Phase2: Randomly remove some kv entries
  for (const auto& [key, _] : dataset_) {
    if (++cnt % 10 == 1) {
      Delete(key);
      removed_keys.push_back(key);
    }
  }

  // Can not find removekeys 
  for (const auto& key : removed_keys) {
    ASSERT_FALSE(Get(key, &value));
  }

  // Others can be find with exactly the same value
  cnt = 0;
  for (const auto& [key, expect_value] : dataset_) {
    if (++cnt % 10 != 1) {
      ASSERT_TRUE(Get(key, &value));
      ASSERT_EQ(expect_value, value);
    }
  }
  
  ClearTestContext("./tmprsmtest");
}


}  // namespace kv
