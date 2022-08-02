#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "config.h"
#include "kv_node.h"
#include "rpc.h"

auto ParseNetAddress(const std::string& net_addr) -> raft::rpc::NetAddress {
  auto specifier = net_addr.find(":");
  auto port_str = net_addr.substr(specifier + 1, net_addr.size() - specifier - 1);
  return raft::rpc::NetAddress{net_addr.substr(0, specifier),
                               static_cast<uint16_t>(std::stoi(port_str, nullptr))};
}

auto ParseCommandSize(const std::string& size_str) -> int {
  if (std::isdigit(size_str.back())) {
    return std::stoi(size_str);
  }
  auto num = std::stoi(size_str.substr(0, size_str.size() - 1));
  switch (size_str.back()) {
    case 'K':
    case 'k':
      return num * 1024;
    case 'M':
    case 'm':
      return num * 1024 * 1024;
    case 'G':
      return num * 1024 * 1024 * 1024;
    default:
      return 0;
  }
}

// This is an interface for parsing cluster configurations of config file
auto ParseConfigurationFile(const std::string& filename) -> kv::KvClusterConfig {
  std::ifstream cfg(filename);
  kv::KvClusterConfig cluster_cfg;

  std::string node_id;
  std::string raft_rpc_addr;
  std::string kv_rpc_addr;
  std::string logname;
  std::string dbname;

  while (cfg >> node_id >> raft_rpc_addr >> kv_rpc_addr >> logname >> dbname) {
    kv::KvServiceNodeConfig cfg;
    cfg.id = std::stoi(node_id);
    cfg.raft_rpc_addr = ParseNetAddress(raft_rpc_addr);
    auto addr = ParseNetAddress(kv_rpc_addr);
    cfg.kv_rpc_addr = kv::rpc::NetAddress{addr.ip, addr.port};
    // cfg.raft_log_filename = logname;
    // Disable persistence for now
    cfg.raft_log_filename = "";
    cfg.kv_dbname = dbname;

    cluster_cfg.insert({cfg.id, cfg});
  }

  return cluster_cfg;
}
