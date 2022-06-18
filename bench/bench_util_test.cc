#include "bench_util.h"
#include <algorithm>

#include "gtest/gtest.h"
#include "util.h"

TEST(CommandLineParserTest, TestSimpleParsing) {
  CommandLineParser parser;
  char* args[] = {
      "./", "-n", "127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003", "-i", "1",
      "-b", "4K"};
  CommandLineParser::NetConfig net_config = {{2, {std::string("127.0.0.1"), 50003}},
                                             {1, {std::string("127.0.0.1"), 50002}},
                                             {0, {std::string("127.0.0.1"), 50001}}};
  ASSERT_TRUE(parser.Parse(9, args));
  auto config = parser.GetNetConfig();
  ASSERT_EQ(config, net_config);
  ASSERT_EQ(parser.GetNodeId(), 1);
  ASSERT_EQ(parser.GetCommandSize(), 4096);
}

TEST(CommandLineParserTest, TestDefaultValue) {
  CommandLineParser parser;
  char* args[] = {
      "./", "-n", "127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003", "-i", "1"};
  CommandLineParser::NetConfig net_config = {{2, {std::string("127.0.0.1"), 50003}},
                                             {1, {std::string("127.0.0.1"), 50002}},
                                             {0, {std::string("127.0.0.1"), 50001}}};

  ASSERT_TRUE(parser.Parse(7, args));
  ASSERT_EQ(parser.GetNetConfig(), net_config);
  ASSERT_EQ(parser.GetNodeId(), 1);
  ASSERT_EQ(parser.GetCommandSize(), 1024);
}

TEST(CommandLineParserTest, TestFailParser) {
  CommandLineParser parser;
  char* args[] = {
      "./", "-s", "127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003", "-i", "1"};
  ASSERT_FALSE(parser.Parse(7, args));
}
