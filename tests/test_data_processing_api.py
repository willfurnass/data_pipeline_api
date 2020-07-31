from data_pipeline_api.data_processing_api import DataProcessingAPI


def test_read_external_object_no_component(tmp_path):
    with open(tmp_path / "config.yaml", "w") as config_file:
        config_file.write("""
data_directory: .
access_log: False
fail_on_hash_mismatch: False
        """)
    with open(tmp_path / "metadata.yaml", "w") as metadata_file:
        metadata_file.write("""
- doi_or_unique_name: doi
  title: title
  filename: data.txt
        """)
    with open(tmp_path / "data.txt", "w") as data_file:
        data_file.write("hello world")
    with DataProcessingAPI.from_config(tmp_path / "config.yaml", "uri", "sha") as api:
        with api.read_external_object("doi", "title") as file:
            assert file.read().decode() == "hello world"


def test_read_external_object_with_component(tmp_path):
    with open(tmp_path / "config.yaml", "w") as config_file:
        config_file.write("""
data_directory: .
access_log: False
fail_on_hash_mismatch: False
        """)
    with open(tmp_path / "metadata.yaml", "w") as metadata_file:
        metadata_file.write("""
- doi_or_unique_name: doi
  title: title
  component: component
  filename: data.txt
        """)
    with open(tmp_path / "data.txt", "w") as data_file:
        data_file.write("hello world")
    with DataProcessingAPI.from_config(tmp_path / "config.yaml", "uri", "sha") as api:
        with api.read_external_object("doi", "title", "component") as file:
            assert file.read().decode() == "hello world"
