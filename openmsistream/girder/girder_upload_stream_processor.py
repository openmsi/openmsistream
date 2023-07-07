# imports
from io import BytesIO
from hashlib import sha256
import girder_client
import openmsistream
from ..data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from ..data_file_io.entity.download_data_file import DownloadDataFileToDisk
from ..utilities.config import RUN_CONST


class GirderUploadStreamProcessor(DataFileStreamProcessor):
    """
    A stream processor that adds downloaded files to folders in a Girder collection
    """

    def __init__(
        self,
        girder_api_url,
        girder_api_key,
        config_file,
        topic_name,
        *,
        collection_name=RUN_CONST.DEFAULT_COLLECTION_NAME,
        girder_root_folder=None,
        provider=None,
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
            "OpenMSIStreamVersion": openmsistream.__version__,
            "KafkaTopic": topic_name,
        }
        if provider:
            self.minimal_metadata_dict["provider"] = provider
        # get or create the collection with the given name, and save its ID
        collection_id = self.__init_collection(collection_name)
        # get or create the root folder, and save its ID and relative path
        root_folder = girder_root_folder
        if not root_folder:
            root_folder = f"{collection_name}/{topic_name}"
        self.__root_folder_id, self.__root_folder_rel_path = self.__init_root_folder(
            root_folder, collection_id, collection_name
        )

    def _process_downloaded_data_file(self, datafile, lock):
        """
        Upload the downloaded data file to the Girder instance

        returns None if processing was successful, an Exception otherwise
        """
        # Create the nested subdirectories that should hold this file
        parent_id = self.__root_folder_id
        if datafile.subdir_str != "":
            subdir_str_split = datafile.subdir_str.split("/")
            for folder_depth, folder_name in enumerate(subdir_str_split):
                metadata_dict = self.minimal_metadata_dict.copy()
                metadata_dict["dsRelPath"] = self.__get_girder_path(
                    subdir_str_split[:folder_depth]
                )
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
                    errmsg = (
                        "ERROR: failed to create the folder with relpath "
                        f"{metadata_dict['dsRelPath']}"
                    )
                    self.logger.error(errmsg, exc_info=exc)
                parent_id = new_folder_id
        else:
            subdir_str_split = []
        # Upload the file from its bytestring or file on disk
        try:
            with lock:
                try:
                    self.__girder_client.uploadStreamToFolder(
                        parent_id,
                        BytesIO(datafile.bytestring),
                        datafile.filename,
                        len(datafile.bytestring),
                    )
                except AttributeError:
                    self.__girder_client.uploadFileToFolder(
                        parent_id, datafile.full_filepath
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
        bytestring = None
        if isinstance(datafile, DownloadDataFileToDisk):
            with open(datafile.full_filepath, "rb") as fp:
                bytestring = fp.read()
        else:
            bytestring = datafile.bytestring
        checksum_hash = sha256()
        checksum_hash.update(bytestring)
        metadata_dict = self.minimal_metadata_dict.copy()
        metadata_dict["dsRelPath"] = self.__get_girder_path(
            subdir_str_split + [datafile.filename]
        )
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

    def __get_girder_path(self, pieces, relative_to_root_dir=True):
        """
        Given a list of strings that should be separated with forward slashes, return
        a constructed path string for Girder metadata.
        Can be relative to the root directory or absolute.
        """
        if relative_to_root_dir:
            path = self.__root_folder_rel_path
        else:
            path = "/"
        if len(pieces) < 1:
            return path
        path += "" if path.endswith("/") else "/"
        path += "/".join(pieces)
        return path

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
            metadata_dict["dsRelPath"] = self.__get_girder_path(
                root_folder_split[start_index:folder_depth], relative_to_root_dir=False
            )
            new_folder_id = self.__create_folder(
                parent_folder_id if parent_folder_id else collection_id,
                folder_name,
                parentType="folder" if parent_folder_id else "collection",
                reuseExisting=True,
                metadata=metadata_dict if folder_depth >= start_index else None,
            )
            parent_folder_id = new_folder_id
        root_folder_id = parent_folder_id
        root_folder_rel_path = "/" + "/".join(root_folder_split[start_index:])
        return root_folder_id, root_folder_rel_path

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
        superargs, superkwargs = super().get_command_line_arguments()
        ret_args = [
            "girder_api_url",
            "girder_api_key",
            *superargs,
            "collection_name",
            "girder_root_folder",
            "provider",
        ]
        return ret_args, superkwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        """
        Run the stream-processed analysis code from the command line
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        # make the stream processor
        girder_uploader = cls(
            args.girder_api_url,
            args.girder_api_key,
            args.config,
            args.topic_name,
            collection_name=args.collection_name,
            girder_root_folder=args.girder_root_folder,
            provider=args.provider,
            output_dir=args.output_dir,
            mode=args.mode,
            n_threads=args.n_threads,
            update_secs=args.update_seconds,
            consumer_group_id=args.consumer_group_id,
        )
        # start the processor running
        msg = (
            f"Listening to the {args.topic_name} topic for files to upload to the "
            f"{args.collection_name} collection using the Girder API at "
            f"{args.girder_api_url}"
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
            f"to the {args.collection_name} Girder collection"
        )
        girder_uploader.logger.info(msg)
        if len(procd_fps) > 0:
            msg = (
                f"{n_files_procd} file{' was' if n_files_procd==1 else 's were'} "
                f"successfully uploaded to the {args.collection_name} Girder collection."
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
