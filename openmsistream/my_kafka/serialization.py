#imports
import msgpack, pathlib, inspect, time
from hashlib import sha512
from kafkacrypto.message import KafkaCryptoMessage, KafkaCryptoMessageError
from confluent_kafka.error import SerializationError
from confluent_kafka.serialization import Serializer, Deserializer
from ..data_file_io.data_file_chunk import DataFileChunk

####################### COMPOUND (DE)SERIALIZERS FOR STACKING MULTIPLE STEPS #######################

class CompoundSerDes(Serializer, Deserializer):
    """
    A Serializer/Deserializer that stacks multiple Serialization/Deserialization steps
    """

    def __init__(self, *args):
        """
        args = a list of (De)Serializer (or otherwise callable) objects to apply IN GIVEN ORDER to some data 
        """
        self.__steps = list(args)

    def __call__(self,data,ctx=None) :
        if data is None :
            return None
        for istep,serdes in enumerate(self.__steps,start=1) :
            try :
                data = serdes(data)
            except Exception as e :
                errmsg = f'ERROR: failed to (de)serialize at step {istep} (of {len(self.__steps)}) in CompoundSerDes! '
                errmsg+= f'Callable = {serdes}, exception = {e}'
                raise SerializationError(errmsg)
        return data

class CompoundSerializer(Serializer):
    """
    A Serializer that stacks multiple steps
    For use with KafkaCrypto since topic names must be passed
    """

    def __init__(self, *args):
        self.__steps = list(args)

    def serialize(self,topic,data) :
        for istep,ser in enumerate(self.__steps,start=1) :
            try :
                if hasattr(ser,'serialize') and inspect.isroutine(ser.serialize) :
                    data = ser.serialize(topic,data)
                else :
                    data = ser(data,ctx=None)
            except Exception as e :
                errmsg = f'ERROR: failed to serialize at step {istep} (of {len(self.__steps)}) in CompoundSerializer! '
                errmsg+= f'Callable = {ser}, exception = {e}'
                raise SerializationError(errmsg)
        return data

class CompoundDeserializer(Deserializer):
    """
    A Deserializer that stacks multiple steps
    For use with KafkaCrypto since topic names must be passed
    """

    MAX_WAIT_PER_MESSAGE = 60 #in seconds

    def __init__(self, *args):
        self.__steps = list(args)

    def deserialize(self,topic,data) :
        for istep,des in enumerate(self.__steps,start=1) :
            try :
                if hasattr(des,'deserialize') and inspect.isroutine(des.deserialize) :
                    data = des.deserialize(topic,data)
                    if isinstance(data,KafkaCryptoMessage) :
                        success = False; elapsed = 0
                        while (not success) and elapsed<CompoundDeserializer.MAX_WAIT_PER_MESSAGE :
                            try :
                                data = data.getMessage()
                                success = True
                            except KafkaCryptoMessageError :
                                time.sleep(1)
                                elapsed+=1
                        #if the message could not be decrypted, return the Message object itself
                        if not success :
                            return data
                else :
                    data = des(data,ctx=None)
            except Exception as e :
                errmsg = f'ERROR: failed to deserialize at step {istep} (of {len(self.__steps)})'
                errmsg+= f' in CompoundDeserializer! Callable = {des}, exception = {e}'
                raise SerializationError(errmsg)
        return data

####################### SERIALIZING/DESERIALIZING FILE CHUNKS #######################

class DataFileChunkSerializer(Serializer) :

    def __call__(self,file_chunk_obj,ctx=None) :
        if file_chunk_obj is None :
            return None
        elif not isinstance(file_chunk_obj,DataFileChunk) :
            raise SerializationError('ERROR: object passed to FileChunkSerializer is not a DataFileChunk!')
        #pack up all the relevant bits of information into a single bytearray
        try :
            #get the chunk's data from the file if necessary
            if file_chunk_obj.data is None :
                file_chunk_obj.populate_with_file_data()
            #pack up all of the properties of the message
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
            return msgpack.packb(ordered_properties,use_bin_type=True)
        except Exception as e :
            raise SerializationError(f'ERROR: failed to serialize a DataFileChunk! Exception: {e}')

class DataFileChunkDeserializer(Deserializer) :

    def __call__(self,byte_array,ctx=None) :
        if byte_array is None :
            return None
        try :
            #unpack the byte array
            ordered_properties = msgpack.unpackb(byte_array,raw=True)
            if len(ordered_properties)!=9 :
                errmsg = 'ERROR: unrecognized token passed to DataFileChunkDeserializer. Expected 9 properties'
                errmsg+= f' but found {len(ordered_properties)}'
                raise ValueError(errmsg)
            try :
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
            except Exception as e :
                errmsg = f'ERROR: unrecognized value(s) when deserializing a DataFileChunk from token. Exception: {e}'
                raise ValueError(errmsg)
            #make sure the hash of the chunk's data matches with what it was before
            check_chunk_hash = sha512()
            check_chunk_hash.update(data)
            check_chunk_hash = check_chunk_hash.digest()
            if check_chunk_hash!=chunk_hash :
                errmsg = f'ERROR: chunk hash {check_chunk_hash} != expected hash {chunk_hash} in file {filename}, '
                errmsg+= f'offset {chunk_offset_write}'
                raise RuntimeError(errmsg)
            #set the filepath based on the subdirectory string
            if subdir_str=='' :
                filepath = pathlib.Path(filename)
            subdir_path = pathlib.PurePosixPath(subdir_str)
            filepath = pathlib.Path('').joinpath(*(subdir_path.parts),filename)
            return DataFileChunk(filepath,filename,file_hash,chunk_hash,chunk_offset_read,chunk_offset_write,
                                 len(data),chunk_i,n_total_chunks,data=data,filename_append=filename_append)
        except Exception as e :
            raise SerializationError(f'ERROR: failed to deserialize a DataFileChunk! Exception: {e}')

