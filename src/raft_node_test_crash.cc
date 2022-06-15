#include <gtest/gtest.h>
#include <unordered_map>

#include "raft_node_test.h"
#include "raft_type.h"
namespace raft {
class RaftNodeBasicTest : public RaftNodeTest {
 public:
  static NetConfig ConstructNetConfig(int server_num) {
    std::string default_ip = "127.0.0.1";
    uint16_t init_port = 50001;
    NetConfig ret;
    for (uint16_t i = 0; i < server_num; ++i) {
      ret.insert({i, {default_ip, static_cast<uint16_t>(init_port + i)}});
    }
    return ret;
  }
};
class RaftNodeCrashTest : public RaftNodeTest {
 public:
  using StorageConfig = std::unordered_map<raft_node_id_t, std::string>;
};
}  // namespace raft
