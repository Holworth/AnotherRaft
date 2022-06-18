#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

#include "raft_node.h"
#include "raft_type.h"
#include "rpc.h"

using namespace raft;
struct CommandLineParser {
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

 public:
  NetConfig GetNetConfig() const { return net_config; }
  raft_node_id_t GetNodeId() const { return node_id; }
  int CommandSize() const { return cmd_size; }

  void Parse(int argc, char* argv[]) {
    int curr_index = 1;
    switch (std::string(argv[curr_index]).at(1)) {
      case 'n': 
        curr_index = ParseNetConfig(argc, argv, curr_index + 1);
        break;
      case 'i': 
        curr_index = ParseNodeId(argc, argv, curr_index + 1);
        break;
      case 'b': 
        curr_index = ParseCommandSize(argc, argv, curr_index + 1);
        break;
      default: 
        std::cerr << "Invalid argument " << std::string(argv[curr_index]) << std::endl;
        exit(1);
    }
  };

 private:
  int ParseNetConfig(int argc, char* argv[], int curr_index) {
    int id = 0;
    std::string net_addr;
    while (net_addr = std::string(argv[curr_index]), net_addr[0] != '-') {
      net_config.insert({
          id, StringToNetAddress(net_addr)
      });
      ++id;
      ++curr_index;
    }
    return curr_index;
  }

  int ParseNodeId(int argc, char* argv[], int curr_index) {
    node_id = std::stoi(std::string(argv[curr_index]));
    return curr_index + 1;
  }

  int ParseCommandSize(int argc, char* argv[], int curr_index) {
    node_id = std::stoi(std::string(argv[curr_index]));
    return curr_index + 1;
  }

  rpc::NetAddress StringToNetAddress(const std::string& net_addr) {
    auto specifier = net_addr.find(":");
    auto port_str = net_addr.substr(specifier + 1, net_addr.size() - specifier - 1);
    return rpc::NetAddress{net_addr.substr(0, specifier),
                           static_cast<uint16_t>(std::stoi(port_str, nullptr))};
  }

 private:
  NetConfig net_config;
  raft_node_id_t node_id;
  int cmd_size;
};

int main(int argc, char* argv[]) {}
