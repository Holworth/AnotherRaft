#include <RCF/RCF.hpp>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <memory>
#include <thread>
#include <atomic>

#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RcfFwd.hpp"
#include "RCF/RcfMethodGen.hpp"
#include "RCF/RcfServer.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "RCF/UdpEndpoint.hpp"
#include "rcf_rpc.h"
#include "rpc.h"
#include "util.h"

// #define DEBUG 1

RCF_BEGIN(I_CounterService, "I_CounterService")
RCF_METHOD_R1(RCF::ByteBuffer, Add, const RCF::ByteBuffer&)
RCF_END(I_CounterService)

class CounterService {
 public:
  RCF::ByteBuffer Add(const RCF::ByteBuffer& buf) {
    int a = *reinterpret_cast<int*>(buf.getPtr());
    int ret_val = val_->fetch_add(a);

    RCF::ByteBuffer ret(sizeof(int));
    *reinterpret_cast<int*>(ret.getPtr()) = ret_val;
    return ret;
  }

  void SetState(std::atomic<int>* val) { val_ = val; }

 private:
  std::atomic<int>* val_;
};

class AdderClient {
  using AdderServicePtr = std::shared_ptr<RcfClient<I_CounterService>>;

 public:
  AdderClient(const raft::rpc::NetAddress& address)
      : rcf_init_(), address_(address), adder_complete_cnt_(0) {}

 public:
  void CallAdd(int increment) {
    std::cout << "Call add with argument " << increment << std::endl;

    AdderServicePtr client_ptr(
        new RcfClient<I_CounterService>(RCF::TcpEndpoint(address_.ip, address_.port)));

    RCF::ByteBuffer arg_buf(sizeof(int));
    *reinterpret_cast<int*>(arg_buf.getPtr()) = increment;

    RCF::Future<RCF::ByteBuffer> ret;
    auto cmp_callback = [=]() {
      onAdderComplete(ret, client_ptr, &(this->adder_complete_cnt_));
    };

    ret = client_ptr->Add(RCF::AsyncTwoway(cmp_callback), arg_buf);
  }

 private:
  static void onAdderComplete(RCF::Future<RCF::ByteBuffer> reply, AdderServicePtr ptr,
                              int* complete_cnt) {
    auto reply_buf = *reply;
    int reply_val = *reinterpret_cast<int*>(reply_buf.getPtr());
    std::cout << "reply value = " << reply_val << std::endl;
    (*complete_cnt) += 1;
  }

 public:
  int AddCallCompleteCount() const { return adder_complete_cnt_; }

 private:
  RCF::RcfInit rcf_init_;
  int adder_complete_cnt_;
  raft::rpc::NetAddress address_;
};

class AdderServer {
 public:
  AdderServer(const raft::rpc::NetAddress& addr)
      : rcf_init_(), rcf_server_(RCF::TcpEndpoint(addr.ip, addr.port)) {}
  void Start() {
    rcf_server_.bind<I_CounterService>(rcf_service_);
    rcf_server_.start();
  }
  void SetState(std::atomic<int>* val) { rcf_service_.SetState(val); }

 private:
  RCF::RcfInit rcf_init_;
  RCF::RcfServer rcf_server_;
  CounterService rcf_service_;
};

int main(int argc, char* argv[]) {
  std::vector<raft::rpc::NetAddress> addr = {
      raft::rpc::NetAddress{"127.0.0.1", 50001},
      raft::rpc::NetAddress{"127.0.0.1", 50002},
      raft::rpc::NetAddress{"127.0.0.1", 50003},
  };
  int myid = atoi(argv[1]);

  std::atomic<int> val(0);

  AdderServer server(addr[myid]);
  std::vector<AdderClient*> clients;
  for (decltype(addr.size()) i = 0; i < addr.size(); ++i) {
    if (i != myid) {
      clients.push_back(new AdderClient(addr[i]));
    }
  }

  server.SetState(&val);
  server.Start();

  std::cout << "Server starts" << std::endl;

  // Sleep until all servers have been booted
  std::this_thread::sleep_for(std::chrono::seconds(5));
  const int kAddCnt = 1000;


  // In debug mod, only the server marked as id 0 is able to send RPC call to other 
  // servers. Other servers would spin for receiving request and execute it, so that
  // we can verify if one single machine works well
#ifdef DEBUG
  if (myid == 0) {
    for (int run = 1; run <= kAddCnt; ++run) {
      for (auto& client : clients) {
        client->CallAdd(1);
      }
    }
  } else {
    for (;;)
      ;
  }
  std::this_thread::sleep_for(std::chrono::seconds(kAddCnt / 100));
  assert(val.load() == 0);
  for (auto& client : clients) {
    assert(client->AddCallCompleteCount() == kAddCnt);
  }
#else
  for (int run = 1; run <= kAddCnt; ++run) {
    for (auto& client : clients) {
      client->CallAdd(1);
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::this_thread::sleep_for(std::chrono::seconds(kAddCnt / 100));
  assert(val.load() == clients.size() * kAddCnt);
  for (auto& client : clients) {
    assert(client->AddCallCompleteCount() == kAddCnt);
  }
#endif
  printf("[TEST PASS]\n");
}
