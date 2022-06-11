#include <iostream>

#include "RCF/Exception.hpp"
#include "RCF/RCF.hpp"
// Define the I_PrintService RCF interface.
RCF_BEGIN(I_PrintService, "I_PrintService")
RCF_METHOD_V1(void, Print, const std::string &)
RCF_END(I_PrintService)
int main() {
  // Initialize RCF.
  RCF::RcfInit rcfInit;
  std::cout << "Calling the I_PrintService Print() method." << std::endl;

  // Instantiate a RCF client.
  RcfClient<I_PrintService> client(RCF::TcpEndpoint("127.0.0.1", 50001));

  std::string str;
  while (true) {
    std::cout << "Input your message" << std::endl;
    std::cin >> str;
    try {
      client.Print(str);
    } catch (const RCF::Exception &e) {
      std::cout << "Error: " << e.getErrorMessage() << std::endl;
    }
  }
  return 0;
}
