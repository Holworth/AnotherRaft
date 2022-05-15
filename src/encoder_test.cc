#include "gtest/gtest.h"
#include "encoder.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {
class EncoderTest : public ::testing::Test {
  public:
  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    auto rand_data = new char[rand_size];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }
};


TEST_F(EncoderTest, TestSimpleEncodingDecoding) {
  const int kTestK = 2;
  const int kTestM = 1;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);
  LogEntry ent;
  ent.SetIndex(kTestRaftIndex);
  ent.SetTerm(kTestRaftTerm);
  ent.SetType(kNormal);
  ent.SetCommandData(GenerateRandomSlice(512, 1024));
  ent.SetStartOffset(0);

  Stripe stripe;

  // Encoding
  encoder->EncodeEntry(ent, &stripe);
  ASSERT_EQ(stripe.GetTerm(), kTestRaftTerm);
  ASSERT_EQ(stripe.GetIndex(), kTestRaftIndex);
  ASSERT_EQ(stripe.GetK(), kTestK);
  ASSERT_EQ(stripe.GetN(), kTestK + kTestM);

  // Decoding
  LogEntry recover_ent;
  encoder->DecodeEntry(&stripe, &recover_ent);
  ASSERT_EQ(recover_ent, ent);
}
  
}
