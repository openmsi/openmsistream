import pathlib
import time
import logging
import pytest

from confluent_kafka.error import SerializationError
from openmsistream import UploadDataFile
from openmsistream.kafka_wrapper.config_file_parser import KafkaConfigFileParser
from openmsistream.kafka_wrapper.serialization import (
    DataFileChunkSerializer,
    DataFileChunkDeserializer,
    CompoundSerializer,
    CompoundDeserializer,
)
from openmsistream.kafka_wrapper.openmsistream_kafka_crypto import (
    OpenMSIStreamKafkaCrypto,
)
from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk

from .config import TEST_CONST
from openmsistream.utilities.config import RUN_CONST


# ---------------------------------------------------------------------
# Helper fixture: builds the UL/DL chunk objects and binary references
# ---------------------------------------------------------------------
@pytest.fixture
def serialization_test_data(logger):
    """
    Reproduces EXACTLY what your setUp() did.
    Loads reference binaries + UL chunks + DL chunks.
    """

    # Load binary reference files
    test_chunk_binaries = {}
    glob_pattern = f"{TEST_CONST.TEST_DATA_FILE_PATH.stem}_test_chunk_*.bin"
    for tcfp in TEST_CONST.TEST_DATA_DIR_PATH.glob(glob_pattern):
        with open(tcfp, "rb") as fp:
            test_chunk_binaries[tcfp.stem.split("_")[-1]] = fp.read()

    if len(test_chunk_binaries) < 1:
        raise RuntimeError(
            f"ERROR: could not find any binary DataFileChunk test files in "
            f"{TEST_CONST.TEST_DATA_DIR_PATH}"
        )

    # Build UL chunks using UploadDataFile
    data_file = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    data_file._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    data_file.add_chunks_to_upload()

    test_ul = {}
    test_dl = {}

    for chunk_i in test_chunk_binaries:
        ul_dfc = data_file.chunks_to_upload[int(chunk_i)]
        ul_dfc.populate_with_file_data(logger=logger)
        test_ul[chunk_i] = ul_dfc

        # Build DL chunk object (your exact code)
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
        test_dl[chunk_i] = dl_dfc

    return test_chunk_binaries, test_ul, test_dl

TOPICS = {
    "test_oms_encrypted": {},
    "test_oms_encrypted.keys": {"--partitions": 1},
    "test_oms_encrypted.reqs": {"--partitions": 1},
    "test_oms_encrypted.subs": {"--partitions": 1},
}
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
class TestSerialization:
    TOPIC_NAME = "test_oms_encrypted"

    def test_data_file_chunk_serializer(self, serialization_test_data):
        binary_refs, test_ul, _ = serialization_test_data

        dfcs = DataFileChunkSerializer()
        assert dfcs(None) is None

        with pytest.raises(SerializationError):
            dfcs("not a chunk")

        for chunk_i, chunk_binary in binary_refs.items():
            assert dfcs(test_ul[chunk_i]) == chunk_binary

    def test_data_file_chunk_deserializer(self, serialization_test_data):
        binary_refs, _, test_dl = serialization_test_data

        dfcds = DataFileChunkDeserializer()
        assert dfcds(None) is None

        with pytest.raises(SerializationError):
            dfcds("not bytes")

        for chunk_i, chunk_binary in binary_refs.items():
            assert test_dl[chunk_i] == dfcds(chunk_binary)

    def test_encrypted_compound_serdes_kafka(self, serialization_test_data, logger):
        _, test_ul, _ = serialization_test_data

        parser1 = KafkaConfigFileParser(TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=logger)
        kc1 = OpenMSIStreamKafkaCrypto(
            parser1.broker_configs, parser1.kc_config_file_str, logging.WARNING
        )

        parser2 = KafkaConfigFileParser(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2, logger=logger
        )
        kc2 = OpenMSIStreamKafkaCrypto(
            parser2.broker_configs, parser2.kc_config_file_str, logging.WARNING
        )

        dfcs = DataFileChunkSerializer()
        dfcds = DataFileChunkDeserializer()
        comp_ser = CompoundSerializer(dfcs, kc1.value_serializer)
        comp_des = CompoundDeserializer(kc2.value_deserializer, dfcds)

        # Kafka requires returning None for None
        assert comp_ser.serialize(self.TOPIC_NAME, None) is None
        assert comp_des.deserialize(self.TOPIC_NAME, None) is None

        with pytest.raises(SerializationError):
            comp_ser.serialize(self.TOPIC_NAME, "not a chunk")

        with pytest.raises(SerializationError):
            comp_des.deserialize(self.TOPIC_NAME, "not bytes")

        # full round-trip encrypted serdes
        for chunk_i, ul_chunk in test_ul.items():
            time.sleep(1)   # EXACT SAME BEHAVIOR YOU HAD
            serialized = comp_ser.serialize(self.TOPIC_NAME, ul_chunk)
            deserialized = comp_des.deserialize(self.TOPIC_NAME, serialized)
            assert deserialized == ul_chunk

        kc1.close()
        kc2.close()
