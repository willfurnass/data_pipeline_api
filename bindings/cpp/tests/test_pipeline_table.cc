#include "gtest/gtest.h"
#include "setup.hh"
#include "table.hh"

TEST_F(SCRCAPITest, TestReadTable)
{
  Table table = pDataPipeline_->read_table("object", "example-table");

  EXPECT_EQ(table.get_column<long>("a"), vector<long>({1,2}));
  EXPECT_EQ(table.get_column<long>("b"),  vector<long>({3,4}));
}

TEST_F(SCRCAPITest, TestWriteTable)
{
    Table table;
    const std::vector<std::string> _alpha = {"A", "B", "C", "D", "E", "F"};
    const std::vector<double> _numero = {0.5, 2.2, 3.4, 4.6, 5.2, 6.1};
    const std::vector<int> _id = {0,1,2,3,4,5};
    table.add_column("ALPHA", _alpha);
    table.add_column("NUMERO", _numero);
    table.add_column("ID", _id);
    EXPECT_NO_FATAL_FAILURE(pDataPipeline_->write_table("output-table", "example-table", table));
}