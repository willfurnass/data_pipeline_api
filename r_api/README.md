# Using the API in R

Using the API within R requires the `reticulate` package which acts as a wrapper for Python allowing direct use of the pipeline.
When using reticulate you can also specify which Python interpreter to use, in this example we use the version obtained when running `which python` within bash:

```R
library(reticulate)

python_version <- system("which python", intern=TRUE)
use_python(python_version)
```

The modules and attributes are accessed in a manner identical to python with a simple switch from `.` to `$`:

```R
StandardAPI <- import("data_pipeline_api.standard_api")$StandardAPI

api_from_config <- StandardAPI$from_config(config_path, git_uri, version)

```

the API is then used as normal:

```R
api_from_config$read_distribution("example", "example-distribution")
```

We can write an R table directly to a Pandas dataframe by importing the Pandas module:

```R
# DataFrame class for conversion to Python-friendly type
pandas_df <- import("pandas")$DataFrame

pd_table <- pandas_df(my_table)

api_from_config$write_table("example-out", "example-table" , pd_table)
```

Reticulate is able to convert to and from numpy arrays, as well as other data types. For more information see the documentation [here](https://rstudio.github.io/reticulate/).