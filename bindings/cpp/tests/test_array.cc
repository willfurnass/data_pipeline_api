#include "gtest/gtest.h"
#include "setup.hh"

TEST_F(SCRCAPITest, TestReadArray)
{
    Array<double> array = pDataPipeline_->read_array<double>("object", "example-array");
    EXPECT_EQ(array(0), 1);
    EXPECT_EQ(array(1), 2);
    EXPECT_EQ(array(2), 3);
}

TEST_F(SCRCAPITest, TestWriteArrayDouble)
{
    Array<double> array(1,{3});

    array(0) = 1.;
    array(1) = 2.;
    array(2) = 3.;

    EXPECT_NO_THROW(pDataPipeline_->write_array<double>("output-object", "example-array", array));
}

TEST_F(SCRCAPITest, TestWriteArrayInt)
{
    Array<int> array(1,{3});

    array(0) = 1;
    array(1) = 2;
    array(2) = 3;

    EXPECT_NO_THROW(pDataPipeline_->write_array<int>("output-object", "example-array", array));
}

TEST_F(SCRCAPITest, TestWriteArrayFloat)
{
    Array<float> array(1,{3});

    array(0) = 1.;
    array(1) = 2.;
    array(2) = 3.;

    EXPECT_NO_THROW(pDataPipeline_->write_array<float>("output-object", "example-array", array));
}