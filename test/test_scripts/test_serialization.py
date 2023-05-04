# imports
import pathlib, time
from confluent_kafka.error import SerializationError
from openmsistream import UploadDataFile
from openmsistream.kafka_wrapper.config_file_parser import KafkaConfigFileParser
from openmsistream.kafka_wrapper.serialization import (
    DataFileChunkSerializer,
    DataFileChunkDeserializer,
)
from openmsistream.kafka_wrapper.serialization import (
    CompoundSerializer,
    CompoundDeserializer,
)
from openmsistream.kafka_wrapper.openmsistream_kafka_crypto import (
    OpenMSIStreamKafkaCrypto,
)
from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk
from config import TEST_CONST
from test_base_classes import TestWithLogger

# constants
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]]


class TestSerialization(TestWithLogger):
    """
    Class for testing classes in openmsistream.kafka_wrapper.serialization
    """

    def setUp(self):
        """
        Make the dictionary of reference serialized binaries
        from the existing reference files
        """
        super().setUp()
        self.test_chunk_binaries = {}
        glob_pattern = f"{TEST_CONST.TEST_DATA_FILE_PATH.stem}_test_chunk_*.bin"
        for tcfp in TEST_CONST.TEST_DATA_DIR_PATH.glob(glob_pattern):
            with open(tcfp, "rb") as fp:
                self.test_chunk_binaries[tcfp.stem.split("_")[-1]] = fp.read()
        if len(self.test_chunk_binaries) < 1:
            msg = (
                "ERROR: could not find any binary DataFileChunk test files to use "
                f"as references for TestSerialization in {TEST_CONST.TEST_DATA_DIR_PATH}!"
            )
            raise RuntimeError(msg)
        # make the dictionary of reference DataFileChunk objects
        data_file = UploadDataFile(
            TEST_CONST.TEST_DATA_FILE_PATH,
            rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
            logger=self.logger,
        )
        data_file._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
        data_file.add_chunks_to_upload()
        self.test_ul_chunk_objects = {}
        self.test_dl_chunk_objects = {}
        for chunk_i in self.test_chunk_binaries.keys():
            ul_dfc = data_file.chunks_to_upload[int(chunk_i)]
            ul_dfc.populate_with_file_data(logger=self.logger)
            self.test_ul_chunk_objects[chunk_i] = ul_dfc
            subdir_as_path = pathlib.Path("").joinpath(
                *(pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).parts)
            )
            dl_dfc = DataFileChunk(
                subdir_as_path / ul_dfc.filename,
                ul_dfc.filename,
                ul_dfc.file_hash,
                ul_dfc.chunk_hash,
                None,
                ul_dfc.chunk_offset_write,
                ul_dfc.chunk_size,
                ul_dfc.chunk_i,
                ul_dfc.n_total_chunks,
                filename_append=ul_dfc.filename_append,
                data=ul_dfc.data,
            )
            dl_dfc.rootdir = TEST_CONST.TEST_RECO_DIR_PATH
            self.test_dl_chunk_objects[chunk_i] = dl_dfc

    def test_data_file_chunk_serializer(self):
        dfcs = DataFileChunkSerializer()
        self.assertIsNone(dfcs(None))  # required by Kafka
        with self.assertRaises(SerializationError):
            dfcs("This is a string, not a DataFileChunk!")
        for chunk_i in self.test_chunk_binaries.keys():
            self.assertEqual(
                dfcs(self.test_ul_chunk_objects[chunk_i]),
                self.test_chunk_binaries[chunk_i],
            )

    def test_data_file_chunk_deserializer(self):
        dfcds = DataFileChunkDeserializer()
        self.assertIsNone(dfcds(None))  # required by Kafka
        with self.assertRaises(SerializationError):
            dfcds("This is a string, not an array of bytes!")
        for chunk_i in self.test_chunk_binaries.keys():
            self.assertEqual(
                self.test_dl_chunk_objects[chunk_i],
                dfcds(self.test_chunk_binaries[chunk_i]),
            )

    def test_encrypted_compound_serdes_kafka(self):
        parser1 = KafkaConfigFileParser(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=self.logger
        )
        kc1 = OpenMSIStreamKafkaCrypto(parser1.broker_configs, parser1.kc_config_file_str)
        parser2 = KafkaConfigFileParser(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2, logger=self.logger
        )
        kc2 = OpenMSIStreamKafkaCrypto(parser2.broker_configs, parser2.kc_config_file_str)
        dfcs = DataFileChunkSerializer()
        dfcds = DataFileChunkDeserializer()
        comp_ser = CompoundSerializer(dfcs, kc1.value_serializer)
        comp_des = CompoundDeserializer(kc2.value_deserializer, dfcds)
        self.assertIsNone(comp_ser.serialize(TOPIC_NAME, None))
        self.assertIsNone(comp_des.deserialize(TOPIC_NAME, None))
        with self.assertRaises(SerializationError):
            comp_ser.serialize(TOPIC_NAME, "This is a string, not a DataFileChunk!")
        with self.assertRaises(SerializationError):
            comp_des.deserialize(TOPIC_NAME, "This is a string, not a DataFileChunk!")
        for chunk_i in self.test_chunk_binaries.keys():
            time.sleep(1)
            serialized = comp_ser.serialize(
                TOPIC_NAME, self.test_ul_chunk_objects[chunk_i]
            )
            deserialized = comp_des.deserialize(TOPIC_NAME, serialized)
            self.assertEqual(deserialized, self.test_ul_chunk_objects[chunk_i])
        kc1.close()
        kc2.close()
