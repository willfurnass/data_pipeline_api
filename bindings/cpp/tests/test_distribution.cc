#include "gtest/gtest.h"
#include "distributions.hh"
#include "setup.hh"

TEST_F(SCRCAPITest, TestWriteDistribution)
{
    EXPECT_NO_THROW(pDataPipeline_->write_distribution("output-parameter", "example-distribution", Gamma(10, 10)));
    EXPECT_NO_FATAL_FAILURE(pDataPipeline_->read_distribution("output-parameter", "example-distribution").getParameter("k") == 10);
    EXPECT_NO_FATAL_FAILURE(pDataPipeline_->read_distribution("output-parameter", "example-distribution").getParameter("theta") == 10);
}