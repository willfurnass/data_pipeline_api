#pragma once

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
#include "pybind11/operators.h"

#include <string>
#include <map>
#include <iostream>
#include <algorithm>

using namespace std;
using namespace pybind11::literals;

typedef map<string, double> params;
typedef map<string, std::vector<double>> arr_params;

class Distribution
{
    private:
        params _parameters;
        arr_params _array_parameters;
    public:
        string name;
        Distribution() {}
        Distribution(string name, params parameters, arr_params arr_parameters=arr_params()) : _parameters(parameters), name(name){}
        friend ostream& operator<<(ostream& os, const Distribution d)
        {
            os << "Distribution('" << d.name << "', ";
            for(auto p : d._parameters)
            {
                os << p.first << "=";
                os << p.second;
                os << ", ";
            }
            
            for(auto p : d._array_parameters)
            {
                os << p.first << "= [";
                for(auto i : p.second)
                {
                    os << i;
                    os << ", ";
                }
                os << "]";
            }

            os << ")";

            return os;
        }
        double getParameter(string param_name);
        vector<double> getArrayParameter(string param_name);
};

PYBIND11_EXPORT pybind11::object Gamma(double k, double theta);

PYBIND11_EXPORT pybind11::object Normal(double mu, double sigma);

PYBIND11_EXPORT pybind11::object Poisson(double lambda);

PYBIND11_EXPORT pybind11::object Multinomial(double n, vector<double> p);

PYBIND11_EXPORT pybind11::object Uniform(double a, double b);

PYBIND11_EXPORT pybind11::object Beta(double alpha, double beta);

PYBIND11_EXPORT pybind11::object Binomial(int n, double p);

Distribution todis_gamma(pybind11::object d_py);

Distribution todis_normal(pybind11::object d_py);

Distribution todis_uniform(pybind11::object d_py);

Distribution todis_poisson(pybind11::object d_py);

Distribution todis_exponential(pybind11::object d_py);

Distribution todis_beta(pybind11::object d_py);

Distribution todis_binomial(pybind11::object d_py);

Distribution todis_multinomial(pybind11::object d_py);

const Distribution get_distribution(pybind11::object d_py);