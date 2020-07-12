#include "catch.hpp"

#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

#include "array.hh"

using namespace std;

const string CONFIG_FILE = "../../tests/data/config.yaml";

TEST_CASE("read_estimate") {
  DataPipeline dp(CONFIG_FILE);
  REQUIRE(dp.read_estimate("parameter", "example-estimate") == 1);
  REQUIRE(dp.read_estimate("parameter", "example-distribution") == 2);
  REQUIRE(dp.read_estimate("parameter", "example-samples") == 2);
}

TEST_CASE("read_sample") {
  DataPipeline dp(CONFIG_FILE);

  REQUIRE(dp.read_sample("parameter", "example-estimate") == 1);
  pybind11::module::import("numpy.random").attr("seed")(0);
  REQUIRE(dp.read_sample("parameter", "example-distribution") == 1.59174901632622);
  REQUIRE(dp.read_sample("parameter", "example-samples") == 2);
}

TEST_CASE("read_table") {
  DataPipeline dp(CONFIG_FILE);
  Table table = dp.read_table("object", "example-table");

  REQUIRE(table.get_column<long>("a") == vector<long>{1,2});
  REQUIRE(table.get_column<long>("b") == vector<long>{3,4});
}

TEST_CASE("table::get_column/types") {
  Table table;

  table.add_column<long>("a",{1,2,3});

  REQUIRE(table.get_column<long>("a") == vector<long>{1,2,3});
  REQUIRE_THROWS_AS(table.get_column<double>("a"), invalid_argument);
}
