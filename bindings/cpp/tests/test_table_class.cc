#include "gtest/gtest.h"
#include "setup.hh"
#include "table.hh"

TEST_F(SCRCAPITest, TestTableUnequalColumns)
{
    Table table;
    table.add_column<long>("a",{1,2,3});
    EXPECT_ANY_THROW(table.add_column<int>("b", {1,2,3,4}));
}

TEST_F(SCRCAPITest, TestTableOverwriteColumns)
{
    Table table;
    table.add_column<long>("a",{1,2,3});
    EXPECT_THROW(table.add_column<int>("a", {1,2,3,4}), invalid_argument);
}

TEST_F(SCRCAPITest, TestTableColumnTypes)
{
  Table table;

  table.add_column<long>("a",{1,2,3});

  EXPECT_EQ(table.get_column<long>("a"), vector<long>({1,2,3}));
  EXPECT_THROW(table.get_column<double>("a"), invalid_argument); 
}