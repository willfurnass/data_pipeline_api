# download
Currently parses config file as specified by the File API document, downloads the data to the file system (with some caveats around URIs) and writes the metadata.yaml file. 
 
```
python -m data_pipeline_api.registry.download --help
Usage: download.py [OPTIONS]

Options:
  --config TEXT         Path to the yaml config file.  [required]
  --data-registry TEXT  URL of the data registry API. Defaults to
                        DATA_REGISTRY_URL env variable followed by
                        http://data.scrc.uk/api/.

  --token TEXT          github personal access token. Defaults to
                        DATA_REGISTRY_ACCESS_TOKEN env if not passed.Personal
                        access tokens can be created from
                        https://github.com/settings/tokens, only
                        admin:org/read:org permissions are required.

  --help                Show this message and exit.

```

```
python -m data_pipeline.registry.download --config data_pipeline_api/registry/example_configs/simple_network_sim_config.yaml
```

# upload
Does not yet parse the access.yaml for upload, but will parse an upload config yaml file of slightly arbitrary definition to write to the data registry.

Sections are:
* reference - items to lookup in the data registry
* patch - items to patch in the data registry
* post - items to post to the data registry

```
python -m data_pipeline_api.registry.upload --help
Usage: upload.py [OPTIONS]

Options:
  --config TEXT         Path to the yaml config file.  [required]
  --data-registry TEXT  URL of the data registry API. Defaults to
                        DATA_REGISTRY_URL env variable followed by
                        http://data.scrc.uk/api/.

  --token TEXT          github personal access token. Defaults to
                        DATA_REGISTRY_ACCESS_TOKEN env if not passed.Personal
                        access tokens can be created from
                        https://github.com/settings/tokens, only
                        admin:org/read:org permissions are required.

  --help                Show this message and exit.
```

```
python -m data_pipeline.registry.upload --config data_pipeline_api/registry/example_configs/simple_network_sim_upload.yaml
```