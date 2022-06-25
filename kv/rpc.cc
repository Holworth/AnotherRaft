#include "rpc.h"

#include "RCF/Exception.hpp"
#include "kv_server.h"
#include "type.h"
#include "util.h"

namespace kv {
namespace rpc {
// Response KvServerRPCService::DealWithRequest(const Request &req) {
//   Response resp;
//   server_->DealWithRequest(&req, &resp);
//   return resp;
// }

Response KvServerRPCClient::DealWithRequest(const Request& request) {
  try {
    auto resp = client_stub_.DealWithRequest(request);
    return resp;
  } catch (RCF::Exception& e) {
    LOG(raft::util::kRaft, "KvServerRPC Client %d RCP failed(%s)", id_,
        e.getErrorMessage().c_str());
    Response resp;
    resp.err = kRPCCallFailed;
    return resp;
  }
}
}  // namespace rpc
}  // namespace kv
