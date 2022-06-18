#pragma once

#include <cctype>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <unordered_map>
#include <cstdarg>

#include "raft_node.h"
#include "raft_type.h"
#include "rpc.h"

using namespace raft;
struct CommandLineParser {
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

 public:
  NetConfig GetNetConfig() const { return net_config; }
  raft_node_id_t GetNodeId() const { return node_id; }
  int GetCommandSize() const { return cmd_size; }

  bool Parse(int argc, char* argv[]) {
    int curr_index = 1;
    while (curr_index < argc) {
      switch (std::string(argv[curr_index]).at(1)) {
        case 'n':
          if (!ParseNetConfig(argc, argv, ++curr_index)) {
            return false;
          }
          break;
        case 'i':
          if (!ParseNodeId(argc, argv, ++curr_index)) {
            return false;
          }
          break;
        case 'b':
          if (!ParseCommandSize(argc, argv, ++curr_index)) {
            return false;
          }
          break;
        default:
          Error("Invalid argument: %s", argv[curr_index]);
          return false;
      }
    }
    return true;
  };

  void ErrorMsg() {
    std::cout << buf << std::endl;
  }

 private:
  bool ParseNetConfig(int argc, char* argv[], int& curr_index) {
    int id = 0;
    std::string net_addr;
    while (net_addr = std::string(argv[curr_index]), net_addr[0] != '-') {
      net_config.insert({id, StringToNetAddress(net_addr)});
      ++id;
      ++curr_index;
    }
    return true;
  }

  bool ParseNodeId(int argc, char* argv[], int& curr_index) {
    node_id = std::stoi(std::string(argv[curr_index]));
    curr_index += 1;
    return true;
  }

  bool ParseCommandSize(int argc, char* argv[], int& curr_index) {
    // Support simplied notation, e.g. "4K, 16M"
    auto size_str = std::string(argv[curr_index]);
    if (std::isdigit(size_str.back())) {
      cmd_size = std::stoi(size_str);
    } else {
      auto num = std::stoi(size_str.substr(0, size_str.size() - 1));
      switch (size_str.back()) {
        case 'K':
          cmd_size = num * 1024;
          break;
        case 'M':
          cmd_size = num * 1024 * 1024;
          break;
        default:
          Error("Invalid argument: %s", argv[curr_index]);
          return false;
      }
    }
    curr_index += 1;
    return true;
  }

  rpc::NetAddress StringToNetAddress(const std::string& net_addr) {
    auto specifier = net_addr.find(":");
    auto port_str = net_addr.substr(specifier + 1, net_addr.size() - specifier - 1);
    return rpc::NetAddress{net_addr.substr(0, specifier),
                           static_cast<uint16_t>(std::stoi(port_str, nullptr))};
  }

  void Error(const char* fmt, ...) {
    va_list vaList;
    va_start(vaList, fmt);
    vsprintf(buf, fmt, vaList);
    va_end(vaList);
  }
  

 private:
  NetConfig net_config;
  raft_node_id_t node_id;
  int cmd_size = 1024;  // Default size
  char buf[512]{};
};
