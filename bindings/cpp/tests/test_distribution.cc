#include "gtest/gtest.h"
#include "distributions.hh"
#include "setup.hh"

TEST_F(SCRCAPITest, TestWriteDistribution)
{
  const std::string CONFIG_FILE = std::string(ROOTDIR)+"/bindings/cpp/tests/config.yaml";
  const std::string uri = std::string(GIT_URL);
  const std::string version = std::string(VERSION);
  DataPipeline* dp_test = new DataPipeline(CONFIG_FILE, uri, version);
  EXPECT_NO_THROW(dp_test->write_distribution("output-parameter", "example-distribution", Gamma(10, 10)));
  EXPECT_EQ(dp_test->read_distribution("output-parameter", "example-distribution").getParameter("k"), 10);
  EXPECT_EQ(dp_test->read_distribution("output-parameter", "example-distribution").getParameter("theta"), 10);
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