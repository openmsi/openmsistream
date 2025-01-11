"""Classes for serialization and deserialization operations"""

# imports
import pathlib, inspect, time
from hashlib import sha512
import msgpack
from confluent_kafka.error import SerializationError
from confluent_kafka.serialization import Serializer, Deserializer

from kafkacrypto.message import KafkaCryptoMessage, KafkaCryptoMessageError
from ..data_file_io.entity.data_file_chunk import DataFileChunk

####################### COMPOUND (DE)SERIALIZERS FOR STACKING MULTIPLE STEPS #######################


class CompoundSerDes(Serializer, Deserializer):
    """
    A Serializer/Deserializer that stacks multiple Serialization/Deserialization steps
    """

    def __init__(self, *args):
        """
        args = a list of (De)Serializer (or otherwise callable) objects to apply
        IN GIVEN ORDER to some data
        """
        self.__steps = list(args)

    def __call__(self, data, ctx=None):
        if data is None:
            return None
        for istep, serdes in enumerate(self.__steps, start=1):
            try:
                data = serdes(data)
            except Exception as exc:
                errmsg = (
                    f"ERROR: failed to (de)serialize at step {istep} (of {len(self.__steps)}) "
                    f"in CompoundSerDes! Callable = {serdes}, exception = {exc}"
                )
                raise SerializationError(errmsg)
        return data


class CompoundSerializer(Serializer):
    """
    A Serializer that stacks multiple steps
    For use with KafkaCrypto since topic names must be passed
    """

    def __init__(self, *args):
        self.__steps = list(args)

    def serialize(self, topic, data):
        """
        Call a chain of Serializers for a given message (may contain encryption)
        """
        for istep, ser in enumerate(self.__steps, start=1):
            try:
                if hasattr(ser, "serialize") and inspect.isroutine(ser.serialize):
                    data = ser.serialize(topic, data)
                else:
                    data = ser(data, ctx=None)
            except Exception as exc:
                errmsg = (
                    f"ERROR: failed to serialize at step {istep} (of {len(self.__steps)}) "
                    f"in {self.__class__.__name__}! Callable = {ser}, exception = {exc}"
                )
                raise SerializationError(errmsg)
        return data

    def __call__(self, data, ctx=None):
        if data is None:
            return None
        raise SerializationError(
            f"ERROR: __call__ method not implemented for a {self.__class__.__name__}"
        )


class CompoundDeserializer(Deserializer):
    """
    A Deserializer that stacks multiple steps
    For use with KafkaCrypto since topic names must be passed
    """

    def __init__(
        self,
        *args,
        treat_undecryptable_as_plaintext=False,
        max_wait_per_decrypt=5.0,
        max_initial_wait_per_decrypt=60.0,
    ):
        self.__steps = list(args)
        self.treat_undecryptable_as_plaintext = treat_undecryptable_as_plaintext
        self.max_wait_per_decrypt = max_wait_per_decrypt
        self.max_initial_wait_per_decrypt = max_initial_wait_per_decrypt
        self.have_waited = False

    def deserialize(self, topic, data):
        """
        Call a chain of Deserializers for a given message (may contain decryption)
        """
        for istep, des in enumerate(self.__steps, start=1):
            try:
                data = self.__deserialize_message(des, topic, data)
            except Exception as exc:
                errmsg = (
                    f"ERROR: failed to deserialize at step {istep} (of {len(self.__steps)}) "
                    f"in {self.__class__.__name__}! Callable = {des}, exception = {exc}"
                )
                raise SerializationError(errmsg)
        return data

    def __call__(self, data, ctx=None):
        if data is None:
            return None
        raise SerializationError(
            f"ERROR: __call__ method not implemented for a {self.__class__.__name__}"
        )

    def __deserialize_message(self, des, topic, data):
        """
        Deserialize a (possibly encrypted) message
        """
        # deserialize a KafkaCrypto message
        if hasattr(des, "deserialize") and inspect.isroutine(des.deserialize):
            data = des.deserialize(topic, data)
            if isinstance(data, KafkaCryptoMessage):
                success = False
                elapsed = 0
                while (
                    (not success)
                    and (
                        (self.have_waited and elapsed < self.max_wait_per_decrypt)
                        or (
                            (not self.have_waited)
                            and elapsed < self.max_initial_wait_per_decrypt
                        )
                    )
                    and data.isPossiblyDecryptable()
                ):
                    try:
                        data = data.getMessage()
                        success = True
                    except KafkaCryptoMessageError:
                        time.sleep(0.1)
                        elapsed += 0.1
                # if the message could not be decrypted, return the Message object
                # itself, or the raw bytes if never decryptable and tunable above
                # is True
                if not success:
                    self.have_waited = True
                    if (
                        not data.isPossiblyDecryptable()
                    ) and self.treat_undecryptable_as_plaintext:
                        return bytes(data)
                    return data
        # if a previous step failed to deserialize a KafkaCrypto message,
        # don't bother trying this next step
        elif not isinstance(data, KafkaCryptoMessage):
            data = des(data, ctx=None)
        return data


####################### SERIALIZING/DESERIALIZING FILE CHUNKS #######################


class DataFileChunkSerializer(Serializer):
    """
    Serializes DataFileChunk objects to bytesarrays
    """

    def __call__(self, file_chunk_obj, ctx=None):
        if file_chunk_obj is None:
            return None
        if not isinstance(file_chunk_obj, DataFileChunk):
            raise SerializationError(
                "ERROR: object passed to FileChunkSerializer is not a DataFileChunk!"
            )
        # pack up all the relevant bits of information into a single bytearray
        try:
            # get the chunk's data from the file if necessary
            if file_chunk_obj.data is None:
                file_chunk_obj.populate_with_file_data()
            # pack up all of the properties of the message
            ordered_properties = []
            ordered_properties.append(str(file_chunk_obj.filename))
            ordered_properties.append(file_chunk_obj.file_hash)
            ordered_properties.append(file_chunk_obj.chunk_hash)
            ordered_properties.append(file_chunk_obj.chunk_offset_write)
            ordered_properties.append(file_chunk_obj.chunk_i)
            ordered_properties.append(file_chunk_obj.n_total_chunks)
            ordered_properties.append(file_chunk_obj.subdir_str)
            ordered_properties.append(file_chunk_obj.filename_append)
            ordered_properties.append(file_chunk_obj.data)
            return msgpack.packb(ordered_properties, use_bin_type=True)
        except Exception as exc:
            raise SerializationError(
                f"ERROR: failed to serialize a DataFileChunk! Exception: {exc}"
            )


class DataFileChunkDeserializer(Deserializer):
    """
    Deseralizes bytearrays to DataFileChunk objects
    """

    def __call__(self, byte_array, ctx=None):
        if byte_array is None:
            return None
        try:
            # unpack the byte array
            ordered_properties = msgpack.unpackb(byte_array, raw=True)
            if len(ordered_properties) != 9:
                errmsg = (
                    f"ERROR: unrecognized token passed to DataFileChunkDeserializer. "
                    f"Expected 9 properties but found {len(ordered_properties)}"
                )
                raise ValueError(errmsg)
            try:
                filename = str(ordered_properties[0].decode())
                file_hash = ordered_properties[1]
                chunk_hash = ordered_properties[2]
                chunk_offset_read = None
                chunk_offset_write = int(ordered_properties[3])
                chunk_i = int(ordered_properties[4])
                n_total_chunks = int(ordered_properties[5])
                subdir_str = str(ordered_properties[6].decode())
                filename_append = str(ordered_properties[7].decode())
                data = ordered_properties[8]
            except Exception as exc:
                errmsg = (
                    f"ERROR: unrecognized value(s) when deserializing a DataFileChunk "
                    f"from token. Exception: {exc}"
                )
                raise ValueError(errmsg)
            # make sure the hash of the chunk's data matches with what it was before
            check_chunk_hash = sha512()
            check_chunk_hash.update(data)
            check_chunk_hash = check_chunk_hash.digest()
            if check_chunk_hash != chunk_hash:
                errmsg = (
                    f"ERROR: chunk hash {check_chunk_hash} != expected hash {chunk_hash} "
                    f"in file {filename}, offset {chunk_offset_write}"
                )
                raise RuntimeError(errmsg)
            # set the filepath based on the subdirectory string
            if subdir_str == "":
                filepath = pathlib.Path(filename)
            subdir_path = pathlib.PurePosixPath(subdir_str)
            filepath = pathlib.Path("").joinpath(*(subdir_path.parts), filename)
            return DataFileChunk(
                filepath,
                filename,
                file_hash,
                chunk_hash,
                chunk_offset_read,
                chunk_offset_write,
                len(data),
                chunk_i,
                n_total_chunks,
                data=data,
                filename_append=filename_append,
            )
        except Exception as exc:
            raise SerializationError(
                f"ERROR: failed to deserialize a DataFileChunk! Exception: {exc}"
            )
