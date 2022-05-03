#include "RCF/RCF.hpp"
#include <iostream>
// Define the I_PrintService RCF interface.
RCF_BEGIN(I_PrintService, "I_PrintService")
RCF_METHOD_V1(void, Print, const std::string &)
RCF_END(I_PrintService)
int main() {
  try {
    // Initialize RCF.
    RCF::RcfInit rcfInit;
    std::cout << "Calling the I_PrintService Print() method." << std::endl;

    // Instantiate a RCF client.
    RcfClient<I_PrintService> client(RCF::TcpEndpoint("127.0.0.1", 50001));
    // Connect to the server and call the Print() method.
    std::string str("Hello, World");
    str.resize(512 * 1024);
    client.Print(str);
  } catch (const RCF::Exception &e) {
    std::cout << "Error: " << e.getErrorMessage() << std::endl;
  }
  return 0;
}
