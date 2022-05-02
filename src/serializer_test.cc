#include "RCF/RCF.hpp"
#include "log_entry.h"
#include "serializer.h"
#include "gtest/gtest.h"

#include <cstring>
#include <random>

// Register RPC call function
RCF_BEGIN(I_EchoService, "I_EchoService")
RCF_METHOD_R1(RCF::ByteBuffer, Echo, const RCF::ByteBuffer &)
RCF_END()

namespace raft {

class EchoService {
public:
  RCF::ByteBuffer Echo(const RCF::ByteBuffer &receive) { return receive; }
};

class SerializerTest : public ::testing::Test {
public:
  template <typename T> void LaunchServerThread() {
    auto server_work = [this]() {
      RCF::RcfInit rcfInit;
      RCF::RcfServer server(RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));
      EchoService echoServrice;
      server.bind<I_EchoService>(echoServrice);
      server.start();
      // Wait until this echo service is executed at least once
      std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));
    };
    auto thread = std::thread(server_work);
    thread.detach();
  }

  template <typename T, typename Cmp>
  void LaunchClientThread(const T &ent, Cmp &cmp) {
    auto client_work = [this, &ent, &cmp]() {
      RCF::RcfInit rcfInit;
      RcfClient<I_EchoService> client(
          RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));

      auto serializer = Serializer::NewSerializer();

      RCF::ByteBuffer buffer(serializer.getSerializeSize(ent));
      serializer.Serialize(&ent, &buffer);

      RCF::ByteBuffer returned = client.Echo(buffer);
      T parse;
      serializer.Deserialize(&returned, &parse);
      ASSERT_TRUE(cmp(ent, parse));
    };
    auto thread = std::thread(client_work);
    thread.join();
  }

  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    auto rand_data = new char[rand_size];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }

  auto GenerateRandomLogEntry(bool generate_data, raft_entry_type type)
      -> LogEntry {
    LogEntry ent;
    ent.SetTerm(rand());
    ent.SetIndex(rand());
    ent.SetSequence(rand());
    ent.SetType(type);
    if (generate_data) {
      switch (ent.Type()) {
      case raft::kNormal:
        ent.SetCommandData(GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        break;
      case raft::kFragments:
        ent.SetNotEncodedSlice(
            GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        ent.SetFragmentSlice(
            GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        break;
      case raft::kTypeMax:
        assert(false);
      }
    }
    return ent;
  }

  void WaitServerExit() {
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime * 2));
  }

private:
  const std::string kLocalTestIp = "127.0.0.1";
  const int kLocalTestPort = 50001;
  const int kMaxDataSize = 1024 * 1024;
  const int kSleepTime = 1000;
};

TEST_F(SerializerTest, TestNoDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(false, kNormal);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent, cmp);
  WaitServerExit();
}

TEST_F(SerializerTest, TestCompleteCommandDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(true, kNormal);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent, cmp);
  WaitServerExit();
}

TEST_F(SerializerTest, TestFragmentDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(true, kFragments);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent, cmp);
  WaitServerExit();
}

TEST_F(SerializerTest, TestSerializeRequestVoteArgs) {
  RequestVoteArgs args;
  auto cmp = [](const RequestVoteArgs &a, const RequestVoteArgs &b) -> bool {
    return std::memcmp(&a, &b, sizeof(RequestVoteArgs)) == 0;
  };
  LaunchServerThread<RequestVoteArgs>();
  LaunchClientThread<RequestVoteArgs>(args, cmp);
  WaitServerExit();
}

TEST_F(SerializerTest, TestSerializeRequestVoteReply) {
  RequestVoteReply reply;
  auto cmp = [](const RequestVoteReply &a, const RequestVoteReply &b) -> bool {
    return std::memcmp(&a, &b, sizeof(RequestVoteReply)) == 0;
  };
  LaunchServerThread<RequestVoteReply>();
  LaunchClientThread<RequestVoteReply>(reply, cmp);
  WaitServerExit();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}
} // namespace raft
//
