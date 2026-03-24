# imports
import hashlib
import json
import mimetypes
import threading
from io import BytesIO

import girder_client
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry

from ..data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from ..utilities.config import RUN_CONST

# Dynamically retrieve version instead of relying on version.py
try:
    from importlib.metadata import version  # Standard in Python 3.8+
except ImportError:
    from importlib_metadata import version  # Backport for Python < 3.8

try:
    openmsistream_version = version("openmsistream")
except Exception:
    openmsistream_version = "unknown"


class GirderClientWithSession(girder_client.GirderClient):
    def __init__(
        self,
        host=None,
        port=None,
        apiRoot=None,
        scheme=None,
        apiUrl=None,
        apiKey=None,
        token=None,
        session=None,
        cacheSettings=None,
        progressReporterCls=None,
    ):
        super().__init__(
            host=host,
            port=port,
            apiRoot=apiRoot,
            scheme=scheme,
            apiUrl=apiUrl,
            cacheSettings=cacheSettings,
            progressReporterCls=progressReporterCls,
        )

        if token:
            self.setToken(token)

        if apiKey:
            self.authenticate(apiKey=apiKey)

        self._session = session

    def existing_resource(self, datafile, folder_id):
        try:
            hashsum = datafile.check_file_hash
            resp = self.get(f"hashsum/sha512/{hashsum}")
            resp.raise_for_status()  # if hashsum_download is not installed it will throw 404
            for fobj in resp:
                item = self.get(f"item/{fobj['itemId']}")
                if not item["folderId"] == folder_id:
                    continue
                return fobj, item
        except HTTPError:
            # fallback to checking for existing file by name
            for item in self.listItem(folder_id, name=datafile.filename):
                return next(self.listFile(item["_id"]), None), item
        return None, None

    def replace_existing_file(self, datafile, file_obj, mimetype=None):
        if datafile.full_filepath and datafile.full_filepath.is_file():
            size = datafile.full_filepath.stat().st_size
            with open(datafile.full_filepath, "rb") as stream:
                self.uploadFileContents(file_obj["_id"], stream, size)
        else:
            data = datafile.bytestring
            self.uploadFileContents(file_obj["_id"], BytesIO(data), len(data))
        return {"itemId": file_obj["itemId"]}

    def upload_new_file(self, parent_id, datafile, mimetype=None):
        if datafile.full_filepath and datafile.full_filepath.is_file():
            return self.uploadFileToFolder(
                parent_id, datafile.full_filepath, mimeType=mimetype
            )
        # in memory handling
        data = datafile.bytestring
        return self.uploadStreamToFolder(
            parent_id,
            BytesIO(data),
            datafile.filename,
            len(data),
            mimeType=mimetype,
        )


class GirderUploadStreamProcessor(DataFileStreamProcessor):
    """
    A stream processor to reconstruct data files read as messages from a topic, hold them
    in memory or on disk, and upload them to a Girder instance when all of their messages
    have been received

    :param girder_api_url: URL of the REST API endpoint for the Girder instance to which
        files should be uploaded
    :type girder_api_url: str
    :param girder_api_key: API key for interacting with the Girder instance
    :type girder_api_key: str
    :param config_file: Path to the config file to use in defining the Broker connection
        and Consumers
    :type config_file: :class:`pathlib.Path`
    :param topic_name: Name of the topic to which the Consumers should be subscribed
    :type topic_name: str
    :param girder_root_folder_ID: ID of an existing Girder Folder relative to which
        files should be uploaded. Additional Folders will be created within this root
        Folder to replicate the original Producer-side subdirectory structure.
    :type girder_root_folder_ID: str
    :param collection_name: Name of the Girder Collection to which files should be
        uploaded. Only used if `girder_root_folder_id` is not given.
    :type collection_name: str
    :param girder_root_folder_path: Path to the root Folder within which files should be
        uploaded. Additional Folders will be created within this root Folder to replicate
        the original Producer-side subdirectory structure. This argument is only used if
        `girder_root_folder_id` is not provided. If a `collection_name` is given but this
        argument is not, a Folder named after the topic will be created within the
        Collection.
    :type girder_root_folder_path: str
    :param metadata: If this argument is given, an extra metadata field with
        the given value will be added to uploaded Files and Folders.
    :type metadata: str (JSON-serializable)
    :param filepath_regex: If given, only messages associated with files whose paths match
        this regex will be consumed
    :type filepath_regex: :type filepath_regex: :func:`re.compile` or None, optional
    """

    def __init__(
        self,
        girder_api_url,
        girder_api_key,
        config_file,
        topic_name,
        *,
        girder_root_folder_id=None,
        collection_name=None,
        girder_root_folder_path=None,
        metadata=None,
        replace_existing=False,
        **other_kwargs,
    ):
        super().__init__(config_file, topic_name, **other_kwargs)
        self._thread_local = threading.local()
        self.retry_strategy = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
        )
        self.http_adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.girder_api_url = girder_api_url
        self.girder_api_key = girder_api_key
        # set some minimal amount of metadata fields
        self.minimal_metadata_dict = {
            "OpenMSIStreamVersion": openmsistream_version,
            "KafkaTopic": topic_name,
        }
        if metadata:
            try:
                self.minimal_metadata_dict.update(json.loads(metadata))
            except Exception as exc:
                errmsg = (
                    "ERROR: failed to parse the given metadata as JSON. "
                    "Exception will be re-raised."
                )
                self.logger.error(errmsg, exc_info=exc, reraise=True)
                raise ValueError(errmsg) from exc
        self.replace_existing = replace_existing
        # if a root folder ID was given, just use that
        if girder_root_folder_id:
            self.__root_folder_id = girder_root_folder_id
        # otherwise, figure it out from the given collection name and/or root folder path
        else:
            # get or create the collection with the given name, and save its ID
            if not collection_name:
                collection_name = RUN_CONST.DEFAULT_COLLECTION_NAME
            collection_id = self.__init_collection(collection_name)
            # get or create the root folder and save its ID
            root_folder_path = girder_root_folder_path
            if not root_folder_path:
                root_folder_path = f"{collection_name}/{topic_name}"
            self.__root_folder_id = self.__init_root_folder(
                root_folder_path, collection_id, collection_name
            )

    @property
    def session(self):
        if not hasattr(self._thread_local, "session"):
            session = requests.Session()
            session.mount("http://", self.http_adapter)
            session.mount("https://", self.http_adapter)
            session.headers.update(
                {
                    "User-Agent": (
                        f"OpenMSIStream/{openmsistream_version} "
                        f"({self.__class__.__name__}; "
                        f"{self.minimal_metadata_dict['KafkaTopic']}) "
                        f"girder-client/{girder_client.__version__} "
                        f"python-requests/{requests.__version__}"
                    ),
                }
            )
            self._thread_local.session = session
        return self._thread_local.session

    @property
    def girder_client(self):
        if not hasattr(self._thread_local, "girder_client"):
            try:
                self._thread_local.girder_client = GirderClientWithSession(
                    apiUrl=self.girder_api_url,
                    apiKey=self.girder_api_key,
                    session=self.session,
                )
            except Exception as exc:
                errmsg = (
                    "ERROR: failed to authenticate to the Girder instance at "
                    f" {self.girder_api_url}. Exception will be re-raised."
                )
                self.logger.error(errmsg, exc_info=exc, reraise=True)
        return self._thread_local.girder_client

    def _process_downloaded_data_file(self, datafile, lock):
        """
        Upload a fully-reconstructed file to the Girder instance, creating Folders as
        necessary to preserve original subdirectory structure. Also adds metadata to Files
        and Folders listing the version of OpenMSIStream that's running and the name of
        the topic from which files are being consumed.

        :param datafile: A :class:`~DownloadDataFile` object that has received
            all of its messages from the topic
        :type datafile: :class:`~DownloadDataFile`
        :param lock: Acquiring this :class:`threading.Lock` object ensures that only one instance
            of :func:`~_process_downloaded_data_file` is running at once
        :type lock: :class:`threading.Lock`

        :return: None if upload was successful, a caught Exception otherwise
        """
        # NOTE: we are not using lock since GirderClientWithSession should be thread-safe,
        # but we are keeping it as an argument in case we want to add any non-thread-safe
        # code in the future
        # Currently, concurrency is handled by Girder server side. If too many requests are
        # sent at once, the retry strategy defined in the session will handle retrying them
        # with backoff.
        return self.__process_downloaded_data_file(datafile, metadata=None)

    @staticmethod
    def __get_checksum(datafile, alg="sha256"):
        checksum = hashlib.new(alg)
        if datafile.full_filepath and datafile.full_filepath.is_file():
            with open(datafile.full_filepath, "rb") as f:
                while True:
                    data = f.read(8192)
                    if not data:
                        break
                    checksum.update(data)
        else:
            checksum.update(datafile.bytestring)
        return checksum.digest()

    def __process_downloaded_data_file(self, datafile, metadata=None):
        """
        Actual process_downloaded_data_file method used in the wrapper above
        """
        # Create the nested subdirectories that should hold this file
        metadata = metadata or {}
        parent_id = self.__root_folder_id
        if datafile.subdir_str != "":
            subdir_str_split = datafile.subdir_str.split("/")
            for folder_name in subdir_str_split:
                metadata_dict = self.minimal_metadata_dict.copy()
                try:
                    new_folder_id = self.__create_folder(
                        parent_id,
                        folder_name,
                        parentType="folder",
                        reuseExisting=True,
                    )
                    self.girder_client.addMetadataToFolder(
                        new_folder_id,
                        metadata_dict,
                    )
                except Exception as exc:
                    errmsg = f"ERROR: failed to create the '{folder_name}' folder"
                    self.logger.error(errmsg, exc_info=exc)
                    return exc
                parent_id = new_folder_id

        # Calculate sh256 checksum of the file for backward compatiblity
        checksum_hash = self.__get_checksum(datafile).hex()
        mimetype, _ = mimetypes.guess_type(datafile.filename)
        mimetype = mimetype or "application/octet-stream"

        # Check if an item with the same name already exists in the folder
        existing_file, existing_item = self.girder_client.existing_resource(
            datafile, parent_id
        )
        if existing_item and existing_file:
            same_file = (existing_file.get("sha512") == datafile.check_file_hash) or (
                existing_item.get("meta", {}).get("checksum", {}).get("sha256")
                == checksum_hash
            )
            if not (same_file or self.replace_existing):
                msg = (
                    f"Found an existing Item named {datafile.filename} in the folder at "
                    f"{datafile.relative_filepath}, but it has a different checksum than "
                    "the file being uploaded. "
                    "(Use --replace_existing to overwrite.)"
                )
                self.logger.info(msg)
                return None

            try:
                upload_obj = self.girder_client.replace_existing_file(
                    datafile, existing_file, mimetype=mimetype
                )
            except Exception as exc:
                errmsg = (
                    f"ERROR: failed to replace the file at {datafile.relative_filepath}"
                )
                self.logger.error(errmsg, exc_info=exc)
                return exc
        else:
            # Upload as a new item
            try:
                upload_obj = self.girder_client.upload_new_file(
                    parent_id, datafile, mimetype=mimetype
                )
            except Exception as exc:
                errmsg = (
                    f"ERROR: failed to upload the file at {datafile.relative_filepath}"
                )
                self.logger.error(errmsg, exc_info=exc)
                return exc

        # Add metadata to the item that was created for the file
        metadata_dict = self.minimal_metadata_dict.copy()
        metadata_dict["checksum"] = {
            "sha256": checksum_hash,
        }
        try:
            metadata_dict.update(metadata)
        except Exception as exc:
            errmsg = (
                "ERROR: failed to update the metadata dictionary with the given metadata: "
                f"{metadata} will be skipped. Exception will be logged but not re-raised."
            )
            self.logger.error(errmsg, exc_info=exc)
        try:
            self.girder_client.addMetadataToItem(upload_obj["itemId"], metadata_dict)
        except Exception as exc:
            errmsg = (
                "ERROR: failed to set metadata for the Item corresponding to the file "
                f"uploaded from {datafile.relative_filepath}"
            )
            self.logger.error(errmsg, exc_info=exc)
            return exc
        try:
            datafile.full_filepath.unlink(missing_ok=True)
        except Exception as exc:
            errmsg = (
                f"ERROR: failed to delete the file at {datafile.full_filepath} after "
                "uploading it to Girder. Exception will be logged but not re-raised."
            )
            self.logger.error(errmsg, exc_info=exc)
        return None

    def __init_collection(self, collection_name):
        """
        Find or create a Collection with the given name, returning its ID

        Logs and re-raises any Exceptions encountered

        Returns the ID of the Collection with the given name
        """
        collection_id = None
        try:
            for resp in self.girder_client.listCollection():
                if resp["_modelType"] == "collection" and resp["name"] == collection_name:
                    collection_id = resp["_id"]
            if not collection_id:
                new_collection = self.girder_client.createCollection(
                    collection_name, public=True
                )
                collection_id = new_collection["_id"]
        except Exception as exc:
            errmsg = (
                f"ERROR: failed to find or create a collection called {collection_name}. "
                "Exception will be re-raised."
            )
            self.logger.error(errmsg, exc_info=exc, reraise=True)
        return collection_id

    def __init_root_folder(self, root_folder, collection_id, collection_name):
        """
        Find or create a "root" Girder Folder at the given (posix-formatted string) path
        into which files should be reconstructed

        Returns the ID of the root folder and its relative path string for metadata use
        """
        root_folder_split = root_folder.split("/")
        start_index = 1 if root_folder_split[0] == collection_name else 0
        parent_folder_id = None
        for folder_depth, folder_name in enumerate(root_folder_split):
            metadata_dict = self.minimal_metadata_dict.copy()
            new_folder_id = self.__create_folder(
                parent_folder_id if parent_folder_id else collection_id,
                folder_name,
                parentType="folder" if parent_folder_id else "collection",
                reuseExisting=True,
                metadata=metadata_dict if folder_depth >= start_index else None,
            )
            parent_folder_id = new_folder_id
        root_folder_id = parent_folder_id
        return root_folder_id

    def __create_folder(self, parent_id, name, **kwargs):
        """
        Create a new folder in the Girder instance under the given parent ID
        with the given name

        Keyword arguments are passed to GirderClient.createFolder

        Logs and re-raises any encountered exceptions

        Returns the ID of the created folder
        """
        new_folder_id = None
        try:
            new_folder = self.girder_client.createFolder(
                parent_id,
                name,
                **kwargs,
            )
            new_folder_id = new_folder["_id"]
        except Exception as exc:
            errmsg = (
                f"ERROR: failed to find or create a folder called {name}. "
                "Exception will be re-raised."
            )
            self.logger.error(errmsg, exc_info=exc, reraise=True)
        return new_folder_id

    @classmethod
    def get_command_line_arguments(cls):
        """
        Return the names of arguments needed to run the program from the command line
        """
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            "girder_api_url",
            "girder_api_key",
            *superargs,
            "girder_root_folder_id",
            "collection_name",
            "girder_root_folder_path",
            "metadata",
            "replace_existing",
        ]
        return args, superkwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        args = [
            parsed_args.girder_api_url,
            parsed_args.girder_api_key,
            *superargs,
        ]
        kwargs = {
            **superkwargs,
            "girder_root_folder_id": parsed_args.girder_root_folder_id,
            "collection_name": parsed_args.collection_name,
            "girder_root_folder_path": parsed_args.girder_root_folder_path,
            "metadata": parsed_args.metadata,
            "replace_existing": parsed_args.replace_existing,
        }
        return args, kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        """
        Run a :class:`~GirderUploadStreamProcessor` directly from the command line

        Calls :func:`~DataFileStreamProcessor.process_files_as_read` on a
        :class:`~GirderUploadStreamProcessor` defined by command line (or given) arguments

        :param args: the list of arguments to send to the parser instead of getting them
            from sys.argv
        :type args: list, optional
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        # make the stream processor
        init_args, init_kwargs = cls.get_init_args_kwargs(args)
        girder_uploader = cls(*init_args, **init_kwargs)
        # start the processor running
        msg = (
            f"Listening to the {args.topic_name} topic for files to upload to "
            f"Girder using the API at {args.girder_api_url}"
        )
        girder_uploader.logger.info(msg)
        (
            n_read,
            n_msgs_procd,
            n_files_procd,
            procd_fps,
        ) = girder_uploader.process_files_as_read()
        # shut down when that function returns
        girder_uploader.close()
        msg = "Girder upload stream processor "
        if args.output_dir is not None:
            msg += f"writing to {args.output_dir} "
        msg += "shut down"
        girder_uploader.logger.info(msg)
        msg = (
            f"{n_read} total messages were consumed, {n_msgs_procd} messages were "
            f"successfully processed, and {n_files_procd} files were uploaded "
            f"to Girder"
        )
        girder_uploader.logger.info(msg)
        if len(procd_fps) > 0:
            msg = (
                f"{n_files_procd} file{' was' if n_files_procd == 1 else 's were'} "
                f"successfully uploaded to Girder."
                f"\nUploaded filepaths (up to {cls.N_RECENT_FILES} most recent):\n\t"
            )
            msg += "\n\t".join([str(fp) for fp in procd_fps])
            girder_uploader.logger.debug(msg)


def main(args=None):
    """
    Run the stream processor from the command line
    """
    GirderUploadStreamProcessor.run_from_command_line(args)


if __name__ == "__main__":
    main()
