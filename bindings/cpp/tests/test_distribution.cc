#include "gtest/gtest.h"
#include "distributions.hh"
#include "setup.hh"

TEST_F(SCRCAPITest, TestWriteDistribution)
{
    EXPECT_NO_THROW(pDataPipeline_->write_distribution("output-parameter", "example-distribution", Gamma(10, 10)));
    EXPECT_NO_FATAL_FAILURE(pDataPipeline_->read_distribution("output-parameter", "example-distribution").getParameter("k") == 10);
    EXPECT_NO_FATAL_FAILURE(pDataPipeline_->read_distribution("output-parameter", "example-distribution").getParameter("theta") == 10);
}

TEST_F(SCRCAPITest, TestReadDistribution)
{
  EXPECT_THROW(pDataPipeline_->read_distribution("parameter", "example-estimate"),
                pybind11::error_already_set);
  EXPECT_EQ(pDataPipeline_->read_distribution("parameter", "example-distribution").name, "gamma");
  EXPECT_EQ(pDataPipeline_->read_distribution("parameter", "example-distribution").getParameter("k"), 1);
  EXPECT_EQ(pDataPipeline_->read_distribution("parameter", "example-distribution").getParameter("theta"), 2);
  EXPECT_THROW(pDataPipeline_->read_distribution("parameter", "example-samples"),
                  pybind11::error_already_set);
}