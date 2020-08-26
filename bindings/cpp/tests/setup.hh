#include "gtest/gtest.h"
#include "datapipeline.hh"
#include "array.hh"
#include "distributions.hh"

#include <random>
#include <ctime>

#include "pybind11/embed.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
#include "pybind11/operators.h"


class SCRCAPITest : public ::testing::Test
{
    public:
        static void SetUpTestSuite()
        {
            pybind11::initialize_interpreter();
        }

        static void TearDownTestSuite()
        {
            pybind11::finalize_interpreter();
        }
    protected:
        const std::string CONFIG_FILE = std::string(ROOTDIR)+"/tests/data/config.yaml";
        const std::string uri = std::string(GIT_URL);
        const std::string version = std::string(VERSION);
        DataPipeline* pDataPipeline_;

        template<typename T>
        std::vector<T> rand_args(int range=100, int n_args=10)
        {
            srand(time(NULL));
            range *= 10;
            std::vector<T> _rand = {};

            for(int i{0}; i < n_args; ++i) _rand.push_back(static_cast<T>(0.1*(rand()/static_cast<double>(RAND_MAX))*range));

            return _rand;
        }

        void SetUp() override
        {
            pDataPipeline_ = new DataPipeline(CONFIG_FILE, uri, version);
        }
        void TearDown() override
        {
            delete pDataPipeline_;
        }

};