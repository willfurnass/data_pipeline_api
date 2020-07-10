#include "catch.hpp"

#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

#include "array.hh"

using namespace std;

const string CONFIG_FILE = "../../tests/data/config.yaml";

TEST_CASE("read_estimate", "[read_estimate]") {
  DataPipeline dp(CONFIG_FILE);
  REQUIRE(dp.read_estimate("parameter", "example-estimate") == 1);
  REQUIRE(dp.read_estimate("parameter", "example-distribution") == 2);
  REQUIRE(dp.read_estimate("parameter", "example-samples") == 2);
}

TEST_CASE("read_sample", "[read_sample]") {
  DataPipeline dp(CONFIG_FILE);

  REQUIRE(dp.read_sample("parameter", "example-estimate") == 1);
  pybind11::module::import("numpy.random").attr("seed")(0);
  REQUIRE(dp.read_sample("parameter", "example-distribution") == 1.59174901632622);
  REQUIRE(dp.read_sample("parameter", "example-samples") == 2);
}
