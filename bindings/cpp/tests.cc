#include "catch.hpp"

#include <iostream>

#include "pybind11/embed.h"

#include "table.hh"
#include "datapipeline.hh"

#include "array.hh"
#include "gitversion.hh"

using namespace std;

const string CONFIG_FILE = "../../tests/data/config.yaml";

const string uri = GIT_URL;

#define INIT_DP DataPipeline dp(CONFIG_FILE, uri, GIT_VERSION)

TEST_CASE("write_estimate", "[!shouldfail]") {
  INIT_DP;
  CHECK_NOTHROW(dp.write_estimate("parameter", "example-estimate", 1.0));
}

TEST_CASE("read_estimate") {
  INIT_DP;
  CHECK(dp.read_estimate("parameter", "example-estimate") == 1);
  CHECK(dp.read_estimate("parameter", "example-distribution") == 2);
  CHECK(dp.read_estimate("parameter", "example-samples") == 2);
}

TEST_CASE("write_distribution", "[!shouldfail]") {
  INIT_DP;
  Distribution dist;
  CHECK_NOTHROW(dp.write_distribution("parameter", "example-distribution", dist));
}

TEST_CASE("read_distribution","[!shouldfail]") {
  INIT_DP;

  CHECK_THROWS_AS(dp.read_distribution("parameter", "example-estimate"),
                  pybind11::error_already_set);
  CHECK(dp.read_distribution("parameter", "example-distribution").name == "gamma");
  CHECK(dp.read_distribution("parameter", "example-distribution").params["shape"] == 1);
  CHECK(dp.read_distribution("parameter", "example-distribution").params["scale"] == 2);
  CHECK_THROWS_AS(dp.read_distribution("parameter", "example-samples"),
                  pybind11::error_already_set);
}

TEST_CASE("read_sample") {
  INIT_DP;

  CHECK(dp.read_sample("parameter", "example-estimate") == 1);
  pybind11::module::import("numpy.random").attr("seed")(0);
  CHECK(dp.read_sample("parameter", "example-distribution") == 1.59174901632622);
  CHECK(dp.read_sample("parameter", "example-samples") == 2);
}

TEST_CASE("write_samples","[!shouldfail]") {
  INIT_DP;
  CHECK_NOTHROW(dp.write_samples("parameter", "example-samples", vector<double>{1,2,3}));
}

TEST_CASE("read_table") {
  INIT_DP;
  Table table = dp.read_table("object", "example-table");

  CHECK(table.get_column<long>("a") == vector<long>{1,2});
  CHECK(table.get_column<long>("b") == vector<long>{3,4});
}

TEST_CASE("table::get_column/types") {
  INIT_DP;
  Table table;

  table.add_column<long>("a",{1,2,3});

  CHECK(table.get_column<long>("a") == vector<long>{1,2,3});
  CHECK_THROWS_AS(table.get_column<double>("a"), invalid_argument);
}

TEST_CASE("read_array") {
  INIT_DP;

  Array<double> array = dp.read_array("object", "example-array");

  CHECK(array(0) == 1);
  CHECK(array(1) == 2);
  CHECK(array(2) == 3);
}

TEST_CASE("write_array") {
  INIT_DP;

  Array<double> array(1,{3});

  array(0) = 1;
  array(1) = 2;
  array(2) = 3;

  // Get error "'memoryview' object has no attribute 'dtype'". 
  CHECK_NOTHROW(dp.write_array("object", "example-array", array));
}
