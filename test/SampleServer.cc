#include "RCF/RCF.hpp"
#include <iostream>
// Define the I_PrintService RCF interface.
RCF_BEGIN(I_PrintService, "I_PrintService")
RCF_METHOD_V1(void, Print, const std::string &)
RCF_END(I_PrintService)
// Server implementation of the I_PrintService RCF interface.
class PrintService {
public:
  void Print(const std::string &s) {
    std::cout << "I_PrintService service: " << s << std::endl;
  }
};
int main() {
  try {
    // Initialize RCF.
    RCF::RcfInit rcfInit;
    // Instantiate a RCF server.
    RCF::RcfServer server(RCF::TcpEndpoint("127.0.0.1", 50001));
    // Bind the I_PrintService interface.
    PrintService printService;
    server.bind<I_PrintService>(printService);
    // Start the server.
    server.start();
    std::cout << "Press Enter to exit..." << std::endl;
    std::cin.get();
  } catch (const RCF::Exception &e) {
    std::cout << "Error: " << e.getErrorMessage() << std::endl;
  }
  return 0;
}
