#pragma once

#define DATA_USE_TOML 0 // todo: define this in cmake file or commandline option
#define DATA_USE_PYTHON 1
#if DATA_USE_TOML
#include "toml11/toml.hpp"
using DT = toml::value;
#elif DATA_USE_PYTHON
#include "pybind11/pybind11.h"
using DT = pybind11::object;
#else
#include "json.hpp"
using DT = nlohmann::json;
// can also just use double as the only scalar type
#endif

#include <string>
#include <map>

/**
 * implemenation of Standardised data type API
 * */

namespace data
{
    /**
 * `template<typename T> class Parameter` is not preferred, 
 * because mixed type container, i.e. `std::map<std::string, Parameter>` is desired. 
 * Meanwhile, some C++ object can hold value of dynamic types with type info accessible, 
 * such as `nlohmann::json` or `toml::value`
 * */
    struct Parameter
    {
        //  as dict key
        std::string type; /// c++ class name, e.g. Estimation, Sample, Distribution
        std::string name; // short name,
        std::string desc; // description/ doc string

        /// this feature, `unit for any parameter`,  is put on hold
        std::string unit; // empty if it is a non-unit
    };

    /// the relationship between Parameter and ParameterFile?
    /// is ParameterFile has only one parameter? or more than one?
    /// typedef Parameter ParameterFile;

    /// these class can be further subclass by specific model
    struct Estimation : public Parameter
    {
        // ctor() set type="Estimation"
        DT value; // T any type supported by json, can be std::vector<ET>
    };

    struct Distribution : public Parameter
    {
    public:
        // ctor() set type="Distribution"
        double scale;
        double shape;
    };

    /// sample classes
    struct Sample : public Parameter
    {
    public:
        // ctor() set type="Samples"
        std::vector<DT> samples;
    };

} // namespace data