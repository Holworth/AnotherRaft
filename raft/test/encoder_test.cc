#include "encoder.h"
#include <cstdlib>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {
class EncoderTest : public ::testing::Test {
 public:
  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    // Add 16 so that the data can be accessed
    auto rand_data = new char[rand_size + 16];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }

  auto GenerateLogEntry(raft_index_t raft_index, raft_term_t raft_term) -> LogEntry {
    LogEntry ent;
    ent.SetIndex(raft_index);
    ent.SetTerm(raft_term);
    ent.SetType(kNormal);
    ent.SetStartOffset(0);
    ent.SetCommandData(GenerateRandomSlice(512, 1024));
    ent.SetFragId(0);
    ent.SetK(0);
    ent.SetN(0);
    ent.SetSequence(0);
    return ent;
  }
};

TEST_F(EncoderTest, TestSimpleEncodingDecoding) {
  const int kTestK = 2;
  const int kTestM = 1;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);
  LogEntry ent = GenerateLogEntry(kTestRaftIndex, kTestRaftTerm);

  // Encoding
  Stripe stripe;
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

TEST_F(EncoderTest, TestDecodingAfterRemovingOneFragment) {
  const int kTestK = 2;
  const int kTestM = 1;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);

  LogEntry ent = GenerateLogEntry(kTestRaftIndex, kTestRaftTerm);

  // Encoding
  Stripe stripe;
  encoder->EncodeEntry(ent, &stripe);

  ASSERT_EQ(stripe.GetTerm(), kTestRaftTerm);
  ASSERT_EQ(stripe.GetIndex(), kTestRaftIndex);
  ASSERT_EQ(stripe.GetK(), kTestK);
  ASSERT_EQ(stripe.GetN(), kTestK + kTestM);

  // Drop one fragment
  stripe.Remove(0);

  // Decoding
  LogEntry recover_ent;
  encoder->DecodeEntry(&stripe, &recover_ent);
  ASSERT_EQ(recover_ent, ent);
}

TEST_F(EncoderTest, TestEncodingDecodingWithNonZeroOffset) {
  const int kTestK = 9;
  const int kTestM = 9;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);
  LogEntry ent = GenerateLogEntry(kTestRaftIndex, kTestRaftTerm);
  ent.SetStartOffset(rand() % ent.CommandLength());

  for (int i = 0; i < kTestK + kTestM; ++i) {
    // Encoding
    Stripe stripe;
    encoder->EncodeEntry(ent, &stripe);
    ASSERT_EQ(stripe.GetTerm(), kTestRaftTerm);
    ASSERT_EQ(stripe.GetIndex(), kTestRaftIndex);
    ASSERT_EQ(stripe.GetK(), kTestK);
    ASSERT_EQ(stripe.GetN(), kTestK + kTestM);

    // Drop one fragment
    stripe.Remove(i);

    // Decoding
    LogEntry recover_ent;
    encoder->DecodeEntry(&stripe, &recover_ent);
    ASSERT_EQ(recover_ent, ent);
  }
}

TEST_F(EncoderTest, TestDecodingWithExactlyKFragments) {
  const int kTestK = 9;
  const int kTestM = 9;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);
  LogEntry ent = GenerateLogEntry(kTestRaftIndex, kTestRaftTerm);
  ent.SetStartOffset(rand() % ent.CommandLength());

  const int kTestRun = kTestK + kTestM;
  for (int i = 0; i < kTestRun; ++i) {
    Stripe stripe;
    encoder->EncodeEntry(ent, &stripe);

    ASSERT_EQ(stripe.GetTerm(), kTestRaftTerm);
    ASSERT_EQ(stripe.GetIndex(), kTestRaftIndex);
    ASSERT_EQ(stripe.GetK(), kTestK);
    ASSERT_EQ(stripe.GetN(), kTestK + kTestM);

    // Remove until there is only k entry
    while (stripe.FragmentCount() > kTestK) {
      stripe.Remove(rand() % (kTestK + kTestM));
    }
    LogEntry recover_ent;
    auto stat = encoder->DecodeEntry(&stripe, &recover_ent);
    ASSERT_TRUE(stat);
    ASSERT_EQ(recover_ent, ent);
  }
} 

TEST_F(EncoderTest, TestDecodeFailWithLessThanKFragments) {
  const int kTestK = 9;
  const int kTestM = 9;
  const raft_index_t kTestRaftIndex = 1;
  const raft_term_t kTestRaftTerm = 1;

  auto encoder = new Encoder(kTestK, kTestM);
  LogEntry ent = GenerateLogEntry(kTestRaftIndex, kTestRaftTerm);
  ent.SetStartOffset(rand() % ent.CommandLength());

  const int kTestRun = kTestK + kTestM;
  for (int i = 0; i < kTestRun; ++i) {
    Stripe stripe;
    encoder->EncodeEntry(ent, &stripe);

    ASSERT_EQ(stripe.GetTerm(), kTestRaftTerm);
    ASSERT_EQ(stripe.GetIndex(), kTestRaftIndex);
    ASSERT_EQ(stripe.GetK(), kTestK);
    ASSERT_EQ(stripe.GetN(), kTestK + kTestM);

    // Remove until there is only k entry
    while (stripe.FragmentCount() > kTestK - 1) {
      stripe.Remove(rand() % (kTestK + kTestM));
    }
    LogEntry recover_ent;
    auto stat = encoder->DecodeEntry(&stripe, &recover_ent);
    ASSERT_FALSE(stat);
  }
}

}  // namespace raft
