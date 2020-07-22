from pathlib import Path
import pandas as pd
from data_pipeline_api.data_processing_api import DataProcessingAPI

config_filename = Path(__file__).parent / "data_processing_config.yaml"
uri = "data_processing_uri"
git_sha = "data_processing_git_sha"
with DataProcessingAPI(config_filename, uri=uri, git_sha=git_sha) as api:
    estimate = api.read_estimate(
        "human/infection/SARS-CoV-2/latent-period", "latent-period"
    )
    print(f"estimate is {estimate}")

    with api.read_external_object("external_object.csv") as file:
        csv = pd.read_csv(file)
        print(f"csv is {csv}")
