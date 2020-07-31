from logging import basicConfig
from pathlib import Path
import numpy as np
from data_pipeline_api.standard_api import StandardAPI, Issue


basicConfig(
    level="DEBUG",
    format="%(asctime)s %(filename)s:%(lineno)s %(levelname)s - %(message)s",
)

CONFIG_PATH = Path(__file__).parent.parent / "tests" / "data" / "config.yaml"

with StandardAPI.from_config(CONFIG_PATH, "uri", "git_sha") as api:
    api.write_estimate(
        "output-parameter",
        "example-estimate-from-estimate",
        api.read_estimate("parameter", "example-estimate"),
    )
    api.write_estimate(
        "output-parameter",
        "example-estimate-from-distribution",
        api.read_estimate("parameter", "example-distribution"),
        issues=[Issue("test issue", 3)]
    )
    api.write_estimate(
        "output-parameter",
        "example-estimate-from-samples",
        api.read_estimate("parameter", "example-samples"),
    )

    # api.read_distribution("parameter", "example-estimate")  # expected to fail
    api.write_distribution(
        "output-parameter",
        "example-distribution",
        api.read_distribution("parameter", "example-distribution"),
    )
    # api.read_distribution("parameter", "example-samples")  # expected to fail

    # api.read_samples("parameter", "example-estimate")  # expected to fail
    # api.read_samples("parameter", "example-distribution")  # expected to fail
    api.write_samples(
        "output-parameter",
        "example-samples-from-samples",
        np.array(api.read_samples("parameter", "example-samples")),
    )

    api.write_table(
        "output-object", "example-table", api.read_table("object", "example-table")
    )
    api.write_array(
        "output-object", "example-array", api.read_array("object", "example-array")
    )
