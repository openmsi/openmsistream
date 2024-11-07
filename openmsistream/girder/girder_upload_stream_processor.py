# imports
import json
import mimetypes
from hashlib import sha256
from io import BytesIO

import girder_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from ..utilities.config import RUN_CONST
from ..version import __version__


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
        **other_kwargs,
    ):
        super().__init__(config_file, topic_name, **other_kwargs)
        # connect and authenticate to the Girder instance
        try:
            self.__girder_client = girder_client.GirderClient(apiUrl=girder_api_url)
            self.__girder_client.authenticate(apiKey=girder_api_key)
        except Exception as exc:
            errmsg = (
                f"ERROR: failed to authenticate to the Girder instance at {girder_api_url}. "
                "Exception will be re-raised."
            )
            self.logger.error(errmsg, exc_info=exc, reraise=True)
        # set some minimal amount of metadata fields
        self.minimal_metadata_dict = {
            "OpenMSIStreamVersion": __version__,
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
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
        )
        retry_adapter = HTTPAdapter(max_retries=retry_strategy)
        with self.__girder_client.session() as session:
            session.mount("http://", retry_adapter)
            session.mount("https://", retry_adapter)
            return self.__process_downloaded_data_file(datafile, lock)

    def __process_downloaded_data_file(self, datafile, lock):
        """
        Actual process_downloaded_data_file method used in the wrapper above
        """
        # Create the nested subdirectories that should hold this file
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
                    self.__girder_client.addMetadataToFolder(
                        new_folder_id,
                        metadata_dict,
                    )
                except Exception as exc:
                    errmsg = f"ERROR: failed to create the '{folder_name}' folder"
                    self.logger.error(errmsg, exc_info=exc)
                    return exc
                parent_id = new_folder_id
        else:
            subdir_str_split = []
        # Calculate the checksum of the file
        checksum_hash = sha256()
        checksum_hash.update(datafile.bytestring)
        # Check if a file with the same name and checksum already exists in the folder
        for resp in self.__girder_client.listItem(parent_id, name=datafile.filename):
            existing_sha256 = resp.get("meta", {}).get("checksum", {}).get("sha256")
            if existing_sha256 == checksum_hash.hexdigest():
                errmsg = (
                    f"WARNING: found an existing Item named {datafile.filename} with the same "
                    f"checksum in the folder at {datafile.relative_filepath}. Skipping upload."
                )
                self.logger.warning(errmsg)
                print(f"File {datafile.filename} already exists with the same checksum")
                return None

        # Upload the file from its bytestring or file on disk
        try:
            with lock:
                mimetype, _ = mimetypes.guess_type(datafile.filename)
                mimetype = mimetype or "application/octet-stream"
                try:
                    self.__girder_client.uploadStreamToFolder(
                        parent_id,
                        BytesIO(datafile.bytestring),
                        datafile.filename,
                        len(datafile.bytestring),
                        mimeType=mimetype,
                    )
                except AttributeError:
                    self.__girder_client.uploadFileToFolder(
                        parent_id, datafile.full_filepath, mimeType=mimetype
                    )
        except Exception as exc:
            errmsg = f"ERROR: failed to upload the file at {datafile.relative_filepath}"
            self.logger.error(errmsg, exc_info=exc)
            return exc
        # Add metadata to the item that was created for the file
        item_id = None
        for resp in self.__girder_client.listItem(parent_id, name=datafile.filename):
            if item_id:
                errmsg = (
                    f"ERROR: found more than one Item named {datafile.filename} "
                    f"after uploading the file at {datafile.relative_filepath}"
                )
                return RuntimeError(errmsg)
            item_id = resp["_id"]
        if not item_id:
            errmsg = (
                "ERROR: could not find a corresponding Item after uploading the file "
                f"at {datafile.relative_filepath}"
            )
            return RuntimeError(errmsg)
        metadata_dict = self.minimal_metadata_dict.copy()
        metadata_dict["checksum"] = {
            "sha256": checksum_hash.hexdigest(),
        }
        try:
            self.__girder_client.addMetadataToItem(item_id, metadata_dict)
        except Exception as exc:
            errmsg = (
                "ERROR: failed to set metadata for the Item corresponding to the file "
                f"uploaded from {datafile.relative_filepath}"
            )
            self.logger.error(errmsg, exc_info=exc)
            return exc
        return None

    def __init_collection(self, collection_name):
        """
        Find or create a Collection with the given name, returning its ID

        Logs and re-raises any Exceptions encountered

        Returns the ID of the Collection with the given name
        """
        collection_id = None
        try:
            for resp in self.__girder_client.listCollection():
                if resp["_modelType"] == "collection" and resp["name"] == collection_name:
                    collection_id = resp["_id"]
            if not collection_id:
                new_collection = self.__girder_client.createCollection(
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
            new_folder = self.__girder_client.createFolder(
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
                f"{n_files_procd} file{' was' if n_files_procd==1 else 's were'} "
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
