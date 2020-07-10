#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "pybind11/embed.h"

int main( int argc, char* argv[] ) {

  pybind11::scoped_interpreter guard{};

  int result = Catch::Session().run( argc, argv );

  return result;
}

void touch_test_catch();

void dummy()
{
  touch_test_catch();
}
