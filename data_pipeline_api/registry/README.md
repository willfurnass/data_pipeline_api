# download
Currently parses config file as specified by the File API document, downloads the data to the file system (with some caveats around URIs) and writes the metadata.yaml file. 
 
```
python -m data_pipeline_api.registry.download --help
Usage: download.py [OPTIONS]

Options:
  --config TEXT         Path to the yaml config file.  [required]
  --data-registry TEXT  URL of the data registry API. Defaults to
                        DATA_REGISTRY_URL env variable followed by
                        https://data.scrc.uk/api/.

  --token TEXT          data registry access token. Defaults to
                        DATA_REGISTRY_ACCESS_TOKEN env if not passed. access
                        tokens can be created from the data registry's get-
                        token end point


  --help                Show this message and exit.

```

```
python -m data_pipeline.registry.download --config examples/example_configs/simple_network_sim_config.yaml

data_registry_download --config examples/example_configs/simple_network_sim_config.yaml
```
        'data_registry_upload = data_pipeline_api.registry.access_upload:upload_model_run_cli',
        'data_registry_post = data_pipeline_api.registry.upload:upload_cli',
# upload from access.yaml
```
python -m registry.access_upload --help
Usage: access_upload.py [OPTIONS]

Options:
  --config TEXT                   Path to the access yaml file.  [required]
  -u, --remote-uri TEXT           URI to the root of the storage  [required]
  -o, --remote-option <TEXT TEXT>...
                                  (key, value) pairs that are passed to the
                                  remote storage, e.g. credentials

  --remote-uri-override TEXT      URI to the root of the storage to post in
                                  the registry required if the URI to use for
                                  download from the registry is different from
                                  that used to upload the item

  --accessibility TEXT            accessibility of the data, defaults to
                                  public

  --data-registry TEXT            URL of the data registry API. Defaults to
                                  DATA_REGISTRY_URL env variable followed by
                                  https://data.scrc.uk/api/.

  --token TEXT                    data registry access token. Defaults to
                                  DATA_REGISTRY_ACCESS_TOKEN env if not
                                  passed. access tokens can be created from
                                  the data registry's get-token end point

  --help                          Show this message and exit.
```

```
data_registry_upload --config access.yaml -remote-uri ftp://someftp/ -o username myusername -o password mypassword 
```

# upload
Parses an upload config yaml file of slightly arbitrary definition to write to the data registry.

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
                        https://data.scrc.uk/api/.

  --token TEXT          data registry access token. Defaults to
                        DATA_REGISTRY_ACCESS_TOKEN env if not passed. access
                        tokens can be created from the data registry's get-
                        token end point


  --help                Show this message and exit.
```

```
python -m data_pipeline.registry.upload --config examples/example_configs/simple_network_sim_upload.yaml

data_registry_post --config examples/example_configs/simple_network_sim_upload.yaml
```
