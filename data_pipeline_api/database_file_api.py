from logging import getLogger
from io import IOBase
import __main__
from data_pipeline_api.file_api import FileAPI, RunMetadata
from data_pipeline_api.registry.download import download_from_configs
from data_pipeline_api.registry.access_upload import upload_model_run
from data_pipeline_api.registry.utils import get_access_token, get_remote_options

logger = getLogger(__name__)


class DatabaseFileAPI(FileAPI):
    """The DatabaseFileAPI extends the FileAPI and provides direct access to the
    database, but still mediated by the file system.
    """

    RUN_METADATA_NEEDED_FOR_DOWNLOAD = {RunMetadata.data_registry_url}
    RUN_METADATA_NEEDED_FOR_UPLOAD = {
        RunMetadata.data_directory,
        RunMetadata.run_id,
        RunMetadata.open_timestamp,
        RunMetadata.git_sha,
        RunMetadata.git_repo,
        RunMetadata.remote_uri,
        RunMetadata.description,
    }

    def _has_run_metadata(self, keys):
        missing_keys = {key for key in keys if key not in self._run_metadata}
        if missing_keys:
            logger.warning("missing %s", missing_keys)
            return False
        return True

    def open_for_read(self, **call_metadata) -> IOBase:
        """Attempt a read, and if it fails, attempt a download and try the read again.
        """
        try:
            return super().open_for_read(**call_metadata)
        except (KeyError, FileNotFoundError):
            if self._has_run_metadata(DatabaseFileAPI.RUN_METADATA_NEEDED_FOR_DOWNLOAD):
                download_from_configs(
                    self._run_metadata,
                    [{"where": self.get_read_metadata(call_metadata)}],
                    get_access_token(),
                    self._root,
                )
                self.load_metadata_store()
                return super().open_for_read(**call_metadata)
            raise

    def close(self):
        """Close as normal, then attempt to upload the results to the database.
        """
        super().close()
        if self._has_run_metadata(DatabaseFileAPI.RUN_METADATA_NEEDED_FOR_UPLOAD):
            upload_model_run(
                config_filename=self._access_log_path,
                model_config_filename=self._config_filename,
                submission_script_filename=self._run_metadata.get(
                    RunMetadata.submission_script, __main__.__file__
                ),
                remote_options=get_remote_options(),
                token=get_access_token(),
            )
