#include <RCF/RCF.hpp>
#include <deque>
#include <iostream>
// Define RCF interface.
RCF_BEGIN(I_PrintService, "I_PrintService")
RCF_METHOD_R1(RCF::ByteBuffer, Print, const RCF::ByteBuffer &)
RCF_END(I_PrintService)
// This class maintains a dedicated thread for processing of Print() calls.
class PrintService {
 public:
  RCF::ByteBuffer Print(const RCF::ByteBuffer &msg) {
    std::cout << "recv buffer has size " << msg.getLength();
    return msg;
  }
};
int main() {
  try {
    RCF::RcfInit rcfInit;
    RCF::RcfServer server(RCF::TcpEndpoint("127.0.0.1", 50001));
    PrintService printService;
    server.bind<I_PrintService>(printService);
    server.start();
    server.stop();
    {
      server.bind<I_PrintService>(printService);
      server.start();
    }
    std::cout << "Press Enter to exit..." << std::endl;
    std::cin.get();
  } catch (const RCF::Exception &e) {
    std::cout << "Error: " << e.getErrorMessage() << std::endl;
  }
  return 0;
}
