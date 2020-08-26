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
   _dis = todis_poisson(Poisson(10));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
   _dis = todis_multinomial(Multinomial(3, {4,5,6}));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
   _dis = todis_binomial(Binomial(3, 7));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
   _dis = todis_uniform(Uniform(3, 7));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
   _dis = todis_beta(Beta(3, 7));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
   _dis = todis_normal(Normal(3, 7));
   EXPECT_NO_FATAL_FAILURE(std::cout << _dis << std::endl);
}