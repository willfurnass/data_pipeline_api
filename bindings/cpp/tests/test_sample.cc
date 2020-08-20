#include "gtest/gtest.h"
#include "setup.hh"

TEST_F(SCRCAPITest, TestReadSample)
{
  ASSERT_THROW(pDataPipeline_->read_sample("parameter", "example-estimate"), pybind11::error_already_set);
  pybind11::module::import("numpy.random").attr("seed")(0);
  ASSERT_THROW(pDataPipeline_->read_sample("parameter", "example-distribution"), pybind11::error_already_set);
  EXPECT_EQ(pDataPipeline_->read_sample("parameter", "example-samples"), vector<double>({1,2,3}));
}

TEST_F(SCRCAPITest, TestWriteSamples)
{
    EXPECT_NO_THROW(pDataPipeline_->write_samples("output-parameter", "example-samples", vector<int>{1,2,3}));
}