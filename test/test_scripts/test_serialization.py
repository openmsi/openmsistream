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


TOPIC_NAME = "test_encrypted_serialization"
TOPICS = {
    TOPIC_NAME: {},
    f"{TOPIC_NAME}.keys": {"--partitions": 1},
    f"{TOPIC_NAME}.reqs": {"--partitions": 1},
    f"{TOPIC_NAME}.subs": {"--partitions": 1},
}


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics")
class TestSerialization:
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

    @pytest.mark.skip(
        reason="Not ported properly yet, needs some refactoring to work with KafkaCrypto"
    )
    def test_encrypted_compound_serdes_kafka(
        self,
        serialization_test_data,
        kafka_config_file,
        encrypted_kafka_node_config,
        logger,
    ):
        _, test_ul, _ = serialization_test_data
        node_id = "consumer_node"
        consumer_config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(
            consumer_config_path,
            node_id,
            TOPIC_NAME,
            "test-rotation-password",
            "test-password",
            1,
            "consumer",
        )

        parser1 = KafkaConfigFileParser(consumer_config_path, logger=logger)
        kc1 = OpenMSIStreamKafkaCrypto(
            parser1.broker_configs, parser1.kc_config_file_str, logging.WARNING
        )

        node_id = "producer_node"
        producer_config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(
            producer_config_path,
            node_id,
            TOPIC_NAME,
            "test-rotation-password",
            "test-password",
            1,
            "producer",
        )
        parser2 = KafkaConfigFileParser(producer_config_path, logger=logger)
        kc2 = OpenMSIStreamKafkaCrypto(
            parser2.broker_configs, parser2.kc_config_file_str, logging.WARNING
        )

        # from pathlib import Path
        # import os

        # print("CWD:", os.getcwd())

        # cfg_dir = Path(parser1.kc_config_file_str).parent
        # print("CFG DIR:", cfg_dir)

        # print("crypto exists:", (cfg_dir / "testing_node.crypto").exists())
        # print("seed exists:", (cfg_dir / "testing_node.seed").exists())

        # crypto_path = cfg_dir / "testing_node.crypto"

        # with open(crypto_path, "r") as f:
        #     for _ in range(5):
        #         print(f.readline().strip())

        ck = kc1._kc._cryptokey
        print("1 CryptoKey dict:", ck.__dict__)
        print("1 esk keys:", ck._CryptoKey__esk.keys())
        ck = kc2._kc._cryptokey
        print("2 CryptoKey dict:", ck.__dict__)
        print("2 esk keys:", ck._CryptoKey__esk.keys())

        # cfg = kc1._kc._cfg
        # print("node:", cfg.node_id)
        # print("cryptokey path:", cfg.cryptokey)
        # print("ratchet path:", cfg.ratchet)

        # print("cwd:", os.getcwd())
        # print("crypto exists:", pathlib.Path(cfg.cryptokey.split("#")[1]).exists())
        # print("seed exists:", pathlib.Path(cfg.ratchet.split("#")[1]).exists())

        # kc1._kc.get_root(self.TOPIC_NAME)
        # ck = kc1._kc._cryptokey
        # assert hasattr(ck, "_CryptoKey__spk"), ck.__dict__
        # kc1._kc.get_producer(kc1._kc.get_root(self.TOPIC_NAME))

        # kc2._kc.get_root(self.TOPIC_NAME)
        # kc2._kc.get_producer(kc2._kc.get_root(self.TOPIC_NAME))

        # print("KC1 config file:", parser1.kc_config_file_str)
        # print("KC1 exists:", pathlib.Path(parser1.kc_config_file_str).exists())
        # print("KC1 crypto object:", kc1._kc.__dict__)

        # print("kc1 crypto:", kc1._kc.__dict__)
        # print("kc2 crypto:", kc2._kc.__dict__)

        dfcs = DataFileChunkSerializer()
        dfcds = DataFileChunkDeserializer()
        comp_ser = CompoundSerializer(dfcs, kc1.value_serializer)
        comp_des = CompoundDeserializer(kc2.value_deserializer, dfcds)

        # Kafka requires returning None for None
        assert comp_ser.serialize(TOPIC_NAME, None) is None
        assert comp_des.deserialize(TOPIC_NAME, None) is None

        with pytest.raises(SerializationError):
            comp_ser.serialize(TOPIC_NAME, "not a chunk")

        with pytest.raises(SerializationError):
            comp_des.deserialize(TOPIC_NAME, "not bytes")

        # full round-trip encrypted serdes
        for _chunk_i, ul_chunk in test_ul.items():
            time.sleep(1)  # EXACT SAME BEHAVIOR YOU HAD

            serialized = comp_ser.serialize(TOPIC_NAME, ul_chunk)
            deserialized = comp_des.deserialize(TOPIC_NAME, serialized)
            # deserialized = dfcds(deserialized)

            assert deserialized == ul_chunk

        kc1.close()
        kc2.close()


def test_chunk_serialization_with_mtime(logger):
    """Verify DataFileChunk with file_mtime round-trips."""
    from openmsistream.kafka_wrapper.serialization import (
        DataFileChunkSerializer,
        DataFileChunkDeserializer,
    )
    from openmsistream.data_file_io.entity.upload_data_file import (
        UploadDataFile,
    )

    udf = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    udf.add_chunks_to_upload()

    chunk = udf.chunks_to_upload[0]
    chunk.populate_with_file_data(logger=logger)

    assert chunk.file_mtime is not None
    assert chunk.file_mtime > 0.0

    serializer = DataFileChunkSerializer()
    deserializer = DataFileChunkDeserializer()

    packed = serializer(chunk)
    unpacked = deserializer(packed)

    assert unpacked.file_mtime == chunk.file_mtime
    assert unpacked.n_total_chunks == chunk.n_total_chunks
    assert unpacked.file_hash == chunk.file_hash


def test_chunk_deserialization_backward_compat(logger):
    """Verify 9-field messages (no mtime) still deserialize."""
    import msgpack
    from hashlib import sha512
    from openmsistream.kafka_wrapper.serialization import (
        DataFileChunkDeserializer,
    )

    data = b"test data for backward compat"
    chunk_hash = sha512(data).digest()
    file_hash = sha512(data).digest()

    old_format = [
        b"testfile.dat",
        file_hash,
        chunk_hash,
        0,
        1,
        1,
        b"subdir",
        b"",
        data,
    ]
    packed = msgpack.packb(old_format, use_bin_type=True)

    deserializer = DataFileChunkDeserializer()
    chunk = deserializer(packed)

    assert chunk.file_mtime is None
    assert chunk.filename == "testfile.dat"
    assert chunk.n_total_chunks == 1
    assert chunk.data == data
