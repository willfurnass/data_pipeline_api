#include "gtest/gtest.h"
#include "setup.hh"

TEST_F(SCRCAPITest, TestWriteEstimate)
{
    EXPECT_NO_THROW(pDataPipeline_->write_estimate("output-parameter", "example-estimate", 1.0));
}

TEST_F(SCRCAPITest, TestReadEstimate)
{
    EXPECT_EQ(pDataPipeline_->read_estimate("parameter", "example-estimate"),  1);
    EXPECT_EQ(pDataPipeline_->read_estimate("parameter", "example-distribution"), 2);
    EXPECT_EQ(pDataPipeline_->read_estimate("parameter", "example-samples"), 2);
}