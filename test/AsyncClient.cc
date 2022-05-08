#include <RCF/RCF.hpp>
#include <iostream>

#include "RCF/ByteBuffer.hpp"
#include "RCF/ThreadLibrary.hpp"
// Define RCF interface.
RCF_BEGIN(I_PrintService, "I_PrintService")
RCF_METHOD_R1(RCF::ByteBuffer, Print, const RCF::ByteBuffer &)
RCF_END(I_PrintService)
typedef std::shared_ptr<RcfClient<I_PrintService> > PrintServicePtr;
// Remote call completion handler.
void onPrintCompleted(PrintServicePtr client, RCF::Future<RCF::ByteBuffer> fRet) {
  std::unique_ptr<RCF::Exception> ePtr = fRet.getAsyncException();
  if (ePtr.get()) {
    std::cout << "Print() returned an exception: " << ePtr->getErrorMessage()
              << std::endl;
  } else {
    std::cout << "Print() returned: " << (*fRet).getLength() << std::endl;
  }
}
int main() {
  try {
    RCF::RcfInit rcfInit;
    {
      // Asynchronous call with completion callback.
      std::cout << std::endl;
      std::cout << "Asynchronous call with completion callback:" << std::endl;

        PrintServicePtr clientPtr(
            new RcfClient<I_PrintService>(RCF::TcpEndpoint("127.0.0.1", 50001)));
      for (int i = 0; i < 10; ++i) {
        RCF::Future<RCF::ByteBuffer> fRet;
        auto onCompletion = [=]() { onPrintCompleted(clientPtr, fRet); };
        fRet =
            clientPtr->Print(RCF::AsyncTwoway(onCompletion), RCF::ByteBuffer(512 * 1024));
        // RCF::sleepMs(10);
      }
      // clientPtr goes out of scope here, but a reference to it is still held in
      // onCompletion, and will be passed to the completion handler when the call
      // completes.
    }
    RCF::sleepMs(1000);
    std::cout << std::endl;
    std::cout << "Press Enter to exit..." << std::endl;
    std::cin.get();
  } catch (const RCF::Exception &e) {
    std::cout << "Error: " << e.getErrorMessage() << std::endl;
  }
  return 0;
}
