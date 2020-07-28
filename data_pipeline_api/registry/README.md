# Data Pipeline Registry Interactions
  * [Downloading](#downloading)
    + [Download Script](#download-script)
    + [`Downloader` object](#-downloader--object)
  * [Uploading](#uploading)
    + [Upload Script (from access.yaml)](#uploading-from-accessyaml)
    + [Arbitrary Upload](#arbitrary-upload)
    + [Uploading a data product](#uploading-a-data-product)


<a name="downloading"></a>
## Downloading 
<a name="download-script"></a>
### Download Script
Parses the config file as specified by the API, downloads the data to the file system and writes the metadata.yaml file. 

Accessed via a cli:
```bash
pipeline_download --help
Usage: pipeline_download [OPTIONS]

Options:
  --config PATH         Path to the yaml config file.  [required]
  --data-registry TEXT  URL of the data registry API. Defaults to DATA_REGISTRY_URL env variable followed by https://data.scrc.uk/api/.
  --token TEXT          data registry access token. Defaults to DATA_REGISTRY_ACCESS_TOKEN env if not passed. access tokens can be created from the data
                        registry's get-token end point

  --help                Show this message and exit
```
or as a python module:
```bash
python -m data_pipeline_api.registry.download --help
```
<a name="-downloader--object"></a>
### `Downloader` object
The `Downloader` object can be used within python to download data products or external objects from the registry, it
can be useful for checking the availability of data outside of the context of the configuration. As with the configuration
it accepts wildcard globbing of fields where applicable (e.g. namespace, data_product name etc).

```python
from data_pipeline_api.registry.downloader import Downloader

downloader = Downloader(data_directory=".")
downloader.add_data_product(namespace="SCRC", data_product="human/*")
downloader.add_data_product(namespace="SCRC", data_product="*", component="array", version="0.1.0")
downloader.add_external_object(doi_or_unique_name="doi://10.1016/j.ijid.2020.03.007")
downloader.download()
``` 
<a name="uploading"></a>
## Uploading
<a name="uploading-from-accessyaml"></a>
### Upload Script (from access.yaml)
Parses the access yaml file created by the API, uploads the outputs and configurations to the remote-uri (or text-file table),
and writes the data to the registry.

Accessed via a cli:
```bash
pipeline_upload --help
Usage: pipeline_upload [OPTIONS]

Options:
  --config PATH                   Path to the access yaml file.  [required]
  --model-config PATH             Path to the model config yaml file.  [required]
  --submission-script PATH        Path to the submission script file.  [required]
  -u, --remote-uri TEXT           URI to the root of the storage  [required]
  -o, --remote-option <TEXT TEXT>...
                                  (key, value) pairs that are passed to the remote storage, e.g. credentials
  --remote-uri-override TEXT      URI to the root of the storage to post in the registry required if the URI to use for download from the registry is
                                  different from that used to upload the item

  --data-registry TEXT            URL of the data registry API. Defaults to DATA_REGISTRY_URL env variable followed by https://data.scrc.uk/api/.
  --token TEXT                    data registry access token. Defaults to DATA_REGISTRY_ACCESS_TOKEN env if not passed. access tokens can be created from
                                  the data registry's get-token end point

  --text-file-table / --no-text-file-table
                                  Whether to upload the model-config and submission-script to the text_file table of the data registry (default), or to
                                  the remote-uri

  --help                          Show this message and exit.
```
or as a python module:
```bash
python -m data_pipeline_api.registry.access_upload --help
```
<a name="arbitrary-upload"></a>
### Arbitrary Upload 
Parses an upload config yaml file of slightly arbitrary definition to write to the data registry.

Sections are:
* patch - items to patch in the data registry
* post - items to post to the data registry

e.g. to upload a `storage_root` and `storage_location` you could write a config file like this:
```yaml
post:
  - &storage_root_ref
    target: 'storage_root'
    data:
      name:  'some_name'
      root: 'https://some_root_url/'

  - 
    target: 'storage_location'
    data:
      path: 'some/path/from/root'
      hash: 'somehashvalue'
      storage_root: *storage_root_ref
```

Accessed via a cli:
```bash
pipeline_post --help
Usage: pipeline_post [OPTIONS]

Options:
  --config TEXT         Path to the yaml config file.  [required]
  --data-registry TEXT  URL of the data registry API. Defaults to DATA_REGISTRY_URL env variable followed by https://data.scrc.uk/api/.
  --token TEXT          data registry access token. Defaults to DATA_REGISTRY_ACCESS_TOKEN env if not passed. access tokens can be created from the data
                        registry's get-token end point

  --help                Show this message and exit.
```
or as a python module:
```bash
python -m data_pipeline_api.registry.upload --help
```
<a name="uploading-a-data-product"></a>
### Uploading a data product
This is a convenience script to help uploading new data products from python.

Accessed via a cli:
```bash
pipeline_upload_data_product --help
Usage: pipeline_upload_data_product [OPTIONS]

Options:
  --data-product-path PATH        Path to the data product on the local filesystem to upload to storage  [required]
  --namespace TEXT                namespace of the data product that's being uploaded, defaults to SCRC
  --storage-root-name TEXT        Name of the storage root being uploaded to, defaults to the remote-uri arg
  --storage-location-path TEXT    Path to upload the file to on remote storage, if not provided no path is used, i.e. the file is uploaded to the root of
                                  remote-uri

  --accessibility INTEGER         Accessibility of the data product, 0: public, 1: private. Defaults to 1.
  --data-product-name TEXT        name of the data product to be uploaded  [required]
  --data-product-description TEXT
                                  free text description of the data product
  --data-product-version TEXT     semver version of the data product. If not provided defaults to 0.1.0 if this is the first version of the data product,
                                  else increments the minor version of the existing data product.  [required]

  --component <TEXT TEXT>...      component (name, description) pairs that are part of this data product, if not provided defaults to data product name
  --data-registry TEXT            URL of the data registry API. Defaults to DATA_REGISTRY_URL env variable followed by https://data.scrc.uk/api/.
  --token TEXT                    data registry access token. Defaults to DATA_REGISTRY_ACCESS_TOKEN env if not passed. access tokens can be created from
                                  the data registry's get-token end point

  -u, --remote-uri TEXT           URI to the root of the remote storage, defaults to --root arg
  -o, --remote-option <TEXT TEXT>...
                                  (key, value) pairs that are passed to the remote storage, e.g. credentials
  --remote-uri-override TEXT      URI to the root of the storage to post in the registry. Required if the uri to use for download from the registry is
                                  different from that used to upload the item

  --help                          Show this message and exit.
```
or as a python module:
```bash
python -m data_pipeline_api.registry.upload_data_product --help
```