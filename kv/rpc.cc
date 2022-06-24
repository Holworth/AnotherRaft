#include "rpc.h"

#include "kv_server.h"
#include "type.h"

namespace kv {
namespace rpc {
// Response KvServerRPCService::DealWithRequest(const Request &req) {
//   Response resp;
//   server_->DealWithRequest(&req, &resp);
//   return resp;
// }

Response KvServerRPCClient::DealWithRequest(const Request& request) {
  return client_stub_.DealWithRequest(request);
}
}  // namespace rpc
}  // namespace kv
