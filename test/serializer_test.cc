#include "RCF/RCF.hpp"
#include "log_entry.h"
#include "gtest/gtest.h"

#include <cstring>
#include <random>

// Register RPC call function
RCF_BEGIN(I_EchoService, "I_EchoService")
RCF_METHOD_R1(raft::LogEntry, Echo, const raft::LogEntry &)
RCF_END()

namespace raft {

template <typename T> class EchoService {
public:
  T Echo(const T &receive) { return receive; }
};

class SerializerTest : public ::testing::Test {
public:
  template <typename T> void LaunchServerThread() {
    auto server_work = [this]() {
      RCF::RcfInit rcfInit;
      RCF::RcfServer server(RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));
      EchoService<T> echoServrice;
      server.bind<I_EchoService>(echoServrice);
      server.start();
      // Wait until this echo service is executed at least once
      std::this_thread::sleep_for(std::chrono::seconds(kSleepTime));
    };
    auto thread = std::thread(server_work);
    thread.detach();
  }

  template <typename T> void LaunchClientThread(const T &entry) {
    auto client_work = [this, &entry]() {
      RCF::RcfInit rcfInit;
      RcfClient<I_EchoService> client(
          RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));
      auto returned = client.Echo(entry);
      ASSERT_EQ(returned, entry);
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
    std::this_thread::sleep_for(std::chrono::seconds(kSleepTime));
  }

private:
  const std::string kLocalTestIp = "127.0.0.1";
  const int kLocalTestPort = 50001;
  const int kMaxDataSize = 512 * 1024;
  const int kSleepTime = 1;
};

TEST_F(SerializerTest, DISABLED_TestNoDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(false, kNormal);
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent);
  WaitServerExit();
}

TEST_F(SerializerTest, DISABLED_TestCompleteCommandDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(true, kNormal);
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent);
  WaitServerExit();
}

TEST_F(SerializerTest, TestFragmentDataLogEntryTransfer) {
  LogEntry ent = GenerateRandomLogEntry(true, kFragments);
  LaunchServerThread<LogEntry>();
  LaunchClientThread<LogEntry>(ent);
  WaitServerExit();
}
} // namespace raft

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
