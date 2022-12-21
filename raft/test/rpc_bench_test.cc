#include <memory>
#include <string>
#include <thread>

#include "RCF/ThreadLibrary.hpp"
#include "log_entry.h"
#include "raft_struct.h"
#include "rcf_rpc.h"
#include "rpc.h"

namespace raft {
static const int kRPCBenchTestPort = 50001;

auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
  int rand_size = (min_len == max_len) ? min_len : rand() % (max_len - min_len) + min_len;
  auto rand_data = new char[rand_size];
  for (decltype(rand_size) i = 0; i < rand_size; ++i) {
    rand_data[i] = rand();
  }
  return Slice(rand_data, rand_size);
}

auto GenerateRandomLogEntry(int min_len, int max_len) -> LogEntry {
  LogEntry ent;
  ent.SetTerm(rand());
  ent.SetIndex(rand());
  ent.SetType(kNormal);
  ent.SetCommandData(GenerateRandomSlice(min_len, max_len));
  return ent;
}

auto GenerateRandomAppendEntryArgs(int len, int ent_cnt) -> AppendEntriesArgs {
  AppendEntriesArgs args;
  args.term = 0;
  args.prev_log_term = 0;
  args.prev_log_index = 0;
  args.leader_id = 0;
  args.leader_commit = 0;
  args.entry_cnt = ent_cnt;
  args.entries.reserve(ent_cnt);
  for (int i = 0; i < args.entry_cnt; ++i) {
    args.entries.push_back(GenerateRandomLogEntry(len, len));
  }
  return args;
}

// The server will send "cnt" AppendEntries RPC call to remote server
// and evaluate the processing time
void RunRPCClient(std::string ip, int size, int cnt) {
  rpc::NetAddress net;
  net.ip = ip;
  net.port = kRPCBenchTestPort;
  auto rpc_client = std::make_shared<rpc::RCFRpcClient>(net, 0);

  for (int i = 1; i <= cnt; ++i) {
    auto arg = GenerateRandomAppendEntryArgs(size, 1);
    rpc_client->sendMessage(arg);
    RCF::sleepMs(10);
    printf("\e[?25l");
    printf("Already Done: %5d / %5d\r", i, cnt);
  }
  puts("");

  auto filename = std::string("results.txt") + std::to_string(size);
  rpc_client->Dump(filename);
}

void RunRPCServer(std::string ip) {
  rpc::NetAddress net;
  net.ip = ip;
  net.port = kRPCBenchTestPort;
  auto rpc_server = std::make_shared<rpc::RCFRpcServer>(net);
  rpc_server->Start();
  std::cout << "[Input to exit]:";
  char c;
  std::cin >> c;
}

}  // namespace raft

int main(int argc, char* argv[]) {
  auto type = std::atoi(argv[1]);
  auto ip = std::string(argv[2]);
  auto size = std::atoi(argv[3]);
  auto cnt = std::atoi(argv[4]);

  if (type == 0) {
    raft::RunRPCServer(ip);
  } else {
    raft::RunRPCClient(ip, size, cnt);
  }
}
