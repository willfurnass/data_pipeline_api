#include "distributions.hh"

using namespace pybind11::literals;

double Distribution::getParameter(string param_name)
{
    if(_parameters.find(param_name) == _parameters.end())
    {
        throw runtime_error("Parameter '"+param_name+"' not found.");
    }
    
    return _parameters[param_name];
}

vector<double> Distribution::getArrayParameter(string param_name)
        {
            if(_array_parameters.find(param_name) == _array_parameters.end())
            {
                throw runtime_error("Parameter '"+param_name+"' not found.");
            }
            return _array_parameters[param_name];
        }

Distribution todis_gamma(pybind11::object d_py)
{
    const string name = "gamma";
    const double k = d_py.attr("args").cast<vector<double>>()[0];
    const double theta = d_py.attr("kwds").cast<map<string, double>>()["scale"];

    return Distribution(name, {{"k", k}, {"theta", theta}});
}

Distribution todis_normal(pybind11::object d_py)
{
    const string name = "normal";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"mu", args[0]}, {"sigma", args[1]}});
}

Distribution todis_uniform(pybind11::object d_py)
{
    const string name = "uniform";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"a", args[0]}, {"b", args[0]+args[1]}});
}

Distribution todis_poisson(pybind11::object d_py)
{
    const string name = "poisson";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"lambda", args[0]}});
}

Distribution todis_exponential(pybind11::object d_py)
{
    const string name = "exponential";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"lambda", 1./args[0]}});
}

Distribution todis_beta(pybind11::object d_py)
{
    const string name = "beta";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"alpha", args[0]}, {"beta", args[1]}});
}

Distribution todis_binomial(pybind11::object d_py)
{
    const string name = "binomial";
    const vector<double> args = d_py.attr("args").cast<vector<double>>();

    return Distribution(name, {{"n", args[0]}, {"p", args[1]}});
}

Distribution todis_multinomial(pybind11::object d_py)
{
    const string name = "multinomial";
    const double n = d_py.attr("n").cast<double>();
    const std::vector<double> p = d_py.attr("p").cast<vector<double>>();

    return Distribution(name, {{"n", n}}, {{"p", p}});
}

const Distribution get_distribution(pybind11::object d_py)
{
    const std::string name = pybind11::str(d_py.attr("dist").attr("name"));

    if(name == "gamma")
    {
        return todis_gamma(d_py);
    }
    else if(name == "norm")
    {
        return todis_normal(d_py);
    }
    else if(name == "uniform")
    {
        return todis_uniform(d_py);
    }
    else if(name == "poisson")
    {
        return todis_poisson(d_py);
    }
    else if(name == "binom")
    {
        return todis_poisson(d_py);
    }
    else if(name == "beta")
    {
        return todis_beta(d_py);
    }
    else if(name == "expon")
    {
        return todis_exponential(d_py);
    }
    else if(name == "multinomial")
    {
        return todis_multinomial(d_py);
    }
    else
    {
        throw runtime_error("Not implemented");
    }

    return Distribution();
}

pybind11::object Gamma(double k, double theta)
{
    pybind11::object gamma = pybind11::module::import("scipy").attr("stats").attr("gamma");
    return gamma(k, "scale"_a=theta);
}

pybind11::object Normal(double mu, double sigma)
{
    pybind11::object norm = pybind11::module::import("scipy").attr("stats").attr("norm");
    return norm(mu, sigma);
}

pybind11::object Poisson(double lambda)
{
    pybind11::object poisson = pybind11::module::import("scipy").attr("stats").attr("poisson");
    return poisson(lambda);
}

pybind11::object Multinomial(double n, std::vector<double> p)
{
    pybind11::object multinom = pybind11::module::import("scipy").attr("stats").attr("multinomial");
    return multinom(n, p);
}

pybind11::object Uniform(double a, double b)
{
    pybind11::object uniform = pybind11::module::import("scipy").attr("stats").attr("uniform");
    return uniform(a, b-a);
}

pybind11::object Beta(double alpha, double beta)
{
    pybind11::object _beta = pybind11::module::import("scipy").attr("stats").attr("beta");
    return _beta(alpha, beta);
}

pybind11::object Binomial(int n, double p)
{
    pybind11::object binom = pybind11::module::import("scipy").attr("stats").attr("binom");
    return binom(n, p);
}

pybind11::object Exponential(double lambda)
{
    pybind11::object expon = pybind11::module::import("scipy").attr("stats").attr("expon");
    return expon("scale"_a=1./lambda);
}