#include "gtest/gtest.h"
#include "distributions.hh"
#include "setup.hh"

#include <sstream>

TEST_F(SCRCAPITest, TestDistributionBuilding)
{
    auto args_d = rand_args<double>(10, 10);
    auto args_i = rand_args<int>(10, 10);
    EXPECT_NO_FATAL_FAILURE(Gamma(args_d[0],args_d[1]));
    EXPECT_NO_FATAL_FAILURE(Poisson(args_i[0]));
    EXPECT_NO_FATAL_FAILURE(Multinomial(3, {args_d[7], args_d[8], args_d[9]}));
    EXPECT_NO_FATAL_FAILURE(Binomial(args_i[4], args_i[5]));
    EXPECT_NO_FATAL_FAILURE(Uniform(args_d[2], args_d[2]+4));
    EXPECT_NO_FATAL_FAILURE(Beta(args_d[3], args_d[4]));
    EXPECT_NO_FATAL_FAILURE(Normal(args_d[5], args_d[6]));
}

TEST_F(SCRCAPITest, TestDistributionPrint)
{
   Distribution _dis = todis_gamma(Gamma(10,10));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
}