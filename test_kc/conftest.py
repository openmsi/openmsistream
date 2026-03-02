import configparser
import logging
import pathlib
import subprocess
import time
from binascii import hexlify

import msgpack
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from kafkacrypto import KafkaCryptoStore
from kafkacrypto.chain import process_chain
from kafkacrypto.cryptokey import CryptoKey
from kafkacrypto.provisioners import PasswordProvisioner, PasswordROT
from kafkacrypto.ratchet import Ratchet
from kafkacrypto.utils import msgpack_default_pack
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from pysodium import randombytes
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs


@pytest.fixture
def state(tmp_path):
    """A simple mutable dict holding runtime state per test."""
    return {"tmp_path": tmp_path}


@pytest.fixture
def logger():
    """Provide a real OpenMSILogger for tests."""
    test_logger = OpenMSILogger(
        logger_name="pytest_test",
        streamlevel=logging.DEBUG,
        logger_filepath=None,  # no file logging
        filelevel=logging.DEBUG,
        conf_global_logger=False,
    )
    return test_logger


@pytest.fixture(scope="session")
def secure_kafka_container(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("kafka_security")
    # Fix directory permissions for Docker traversal
    tmp_dir.chmod(0o755)

    jaas_file = tmp_dir / "kafka_server_jaas.conf"
    keystore_file = tmp_dir / "server.keystore.jks"
    truststore_file = tmp_dir / "server.truststore.jks"
    password = "test-password"

    # Confluent script requirement: dummy file named the same as the password
    dummy_password_file = tmp_dir / password
    dummy_password_file.write_text(password)

    # 1. Create JAAS Config
    # Added 'user_admin' to ensure the 'admin' principal exists in the PLAIN mechanism
    jaas_file.write_text(
        f"""
    KafkaServer {{
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="admin"
        password="{password}"
        user_admin="{password}"
        user_alice="alice-password";
    }};
    """
    )

    # 2. Generate SSL Assets
    # Generate Keystore
    subprocess.run(
        [
            "keytool",
            "-genkey",
            "-keystore",
            str(keystore_file),
            "-alias",
            "localhost",
            "-validity",
            "365",
            "-keyalg",
            "RSA",
            "-storepass",
            password,
            "-keypass",
            password,
            "-dname",
            "CN=localhost",
            "-noprompt",
        ],
        check=True,
    )

    # Export Cert for Truststore and Client PEM
    cert_file = tmp_dir / "server.crt"
    subprocess.run(
        [
            "keytool",
            "-exportcert",
            "-keystore",
            str(keystore_file),
            "-alias",
            "localhost",
            "-file",
            str(cert_file),
            "-storepass",
            password,
            "-noprompt",
        ],
        check=True,
    )

    # Create Truststore
    subprocess.run(
        [
            "keytool",
            "-importcert",
            "-keystore",
            str(truststore_file),
            "-alias",
            "localhost",
            "-file",
            str(cert_file),
            "-storepass",
            password,
            "-noprompt",
        ],
        check=True,
    )

    # Set file permissions for the container user
    for f in [jaas_file, keystore_file, truststore_file, dummy_password_file]:
        f.chmod(0o777)

    # 3. Define Containers
    network = Network()
    zookeeper = (
        DockerContainer("confluentinc/cp-zookeeper:7.3.0")
        .with_name("zksecure")
        .with_network(network)
        .with_env("ZOOKEEPER_CLIENT_PORT", "2181")
        .with_env("ZOOKEEPER_TICK_TIME", "2000")
    )

    broker = (
        DockerContainer("confluentinc/cp-kafka:7.3.0")
        .with_name("kafkasecure")
        .with_network(network)
        .with_bind_ports(9093, 9093)
        .with_volume_mapping(str(jaas_file), "/etc/kafka/kafka_server_jaas.conf")
        .with_volume_mapping(str(tmp_dir), "/etc/kafka/secrets", mode="rw")
        # General Config
        .with_env("KAFKA_BROKER_ID", "1")
        # Increase the ZK connection timeout in case the network bridge is slow
        .with_env("KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS", "30000")
        # Disable the Confluent "Self-Help" health checks that can hang
        .with_env("KAFKA_CONFLUENT_BALANCER_ENABLE", "false")
        .with_env("KAFKA_ZOOKEEPER_CONNECT", "zksecure:2181")
        # Critical: Disable SASL for Zookeeper connections (applies to all utilities)
        .with_env("ZOOKEEPER_SASL_ENABLED", "false")
        # Listeners: External uses SASL_SSL, Internal uses PLAINTEXT to avoid handshake hangs
        .with_env("KAFKA_LISTENERS", "SASL_SSL://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9092")
        .with_env(
            "KAFKA_ADVERTISED_LISTENERS",
            "SASL_SSL://localhost:9093,PLAINTEXT://kafkasecure:9092",
        )
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT",
        )
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        # SASL Config
        .with_env("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
        # SSL Config - Explicit Locations
        .with_env("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/server.keystore.jks")
        .with_env(
            "KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/server.truststore.jks"
        )
        .with_env("KAFKA_SSL_KEYSTORE_FILENAME", "server.keystore.jks")
        .with_env("KAFKA_SSL_TRUSTSTORE_FILENAME", "server.truststore.jks")
        # Passwords & Credentials (DUB logic bypass)
        .with_env("KAFKA_SSL_KEY_CREDENTIALS", password)
        .with_env("KAFKA_SSL_KEYSTORE_CREDENTIALS", password)
        .with_env("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", password)
        .with_env("KAFKA_SSL_KEYSTORE_PASSWORD", password)
        .with_env("KAFKA_SSL_KEY_PASSWORD", password)
        .with_env("KAFKA_SSL_TRUSTSTORE_PASSWORD", password)
        # JVM Options: Disable ZK SASL and point to JAAS
        .with_env(
            "KAFKA_OPTS",
            (
                "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf "
                "-Dzookeeper.sasl.client=false"
            ),
        )
        # Single Node Optimization
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
    )

    # 4. Lifecycle Management
    with network:
        with zookeeper:
            with broker:
                # Use a loop with log printing for easier debugging if it hangs
                print("Container started. Waiting for Kafka 'started' signal...")
                wait_for_logs(broker, r".*\[KafkaServer id=\d+\] started.*", timeout=90)

                # Export PEM for Python client (Confluent-Kafka needs PEM)
                truststore_pem = tmp_dir / "server.truststore.pem"
                subprocess.run(
                    [
                        "keytool",
                        "-exportcert",
                        "-rfc",
                        "-keystore",
                        str(keystore_file),
                        "-alias",
                        "localhost",
                        "-file",
                        str(truststore_pem),
                        "-storepass",
                        password,
                    ],
                    check=True,
                )

                broker.truststore_pem = str(truststore_pem)
                broker.sasl_password = password
                yield broker


@pytest.fixture(scope="session")
def kafka_conf(secure_kafka_container):
    return {
        "bootstrap.servers": "localhost:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "admin",
        "sasl.password": secure_kafka_container.sasl_password,
        "ssl.ca.location": secure_kafka_container.truststore_pem,
        "ssl.endpoint.identification.algorithm": "none",
    }


@pytest.fixture
def kafka_topics(kafka_conf, request):
    """
    Dynamically provision Kafka topics for tests.

    Usage with indirect parametrization:
        @pytest.mark.parametrize(
            "kafka_topics",
            [
                {"topic1": {"num_partitions": 3, "replication_factor": 1}},
                {"topic2": {"num_partitions": 1, "replication_factor": 1}},
            ],
            indirect=True,
        )
        def test_example(kafka_topics):
            # kafka_topics will contain the list of created topic names
            pass
    """
    topics_dict = request.param
    topic_names = list(topics_dict.keys())

    # Create AdminClient
    admin_client = AdminClient(kafka_conf)

    # Build NewTopic objects
    new_topics = []
    for topic_name, config in topics_dict.items():
        num_partitions = config.get("num_partitions", 1)
        replication_factor = config.get("replication_factor", 1)
        topic_config = config.get("config", {})

        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=topic_config,
            )
        )

    # Create topics
    fs = admin_client.create_topics(new_topics)

    # Wait for topic creation to complete
    for topic_name, f in fs.items():
        try:
            f.result()  # Block until topic is created
            print(f"Topic '{topic_name}' created successfully")
        except Exception as e:
            # Topic might already exist, which is fine
            print(f"Topic '{topic_name}' creation result: {e}")

    # Yield the list of topic names to the test
    yield topic_names

    # Cleanup: Delete topics after test
    delete_fs = admin_client.delete_topics(topic_names, operation_timeout=30)
    for topic_name, f in delete_fs.items():
        try:
            f.result()
            print(f"Topic '{topic_name}' deleted successfully")
        except Exception as e:
            print(f"Failed to delete topic '{topic_name}': {e}")


@pytest.fixture
def kafka_config_file(tmp_path, kafka_conf):
    def _kafka_config_file(nodeID=None):
        if nodeID is None:
            raise ValueError("nodeID must be provided to kafka_config_file")
        # 1. Define your configuration as a nested dictionary
        config_dict = {
            "kafkacrypto": {"node_id": nodeID},
            "broker": {
                "bootstrap.servers": kafka_conf["bootstrap.servers"],
                "security.protocol": kafka_conf["security.protocol"],
                "sasl.mechanism": kafka_conf["sasl.mechanism"],
                "sasl.username": kafka_conf["sasl.username"],
                "sasl.password": kafka_conf["sasl.password"],
                # confluent-kafka naming (for Producer/Consumer/KafkaCrypto)
                "ssl.ca.location": kafka_conf["ssl.ca.location"],
                "ssl.endpoint.identification.algorithm": kafka_conf[
                    "ssl.endpoint.identification.algorithm"
                ],
            },
            "producer": {
                "batch.size": "200000",
                "linger.ms": "100",
                "compression.type": "lz4",
                "key.serializer": "StringSerializer",
                "value.serializer": "DataFileChunkSerializer",
            },
            "consumer": {
                "group.id": "create_new",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "False",
                "fetch.min.bytes": "100000",
                "key.deserializer": "StringDeserializer",
                "value.deserializer": "DataFileChunkDeserializer",
            },
        }

        # 2. Initialize ConfigParser and load the dictionary
        config = configparser.ConfigParser()
        config.read_dict(config_dict)

        # 3. Define path and write to file
        config_path = tmp_path / f"test_{nodeID}.config"
        with open(config_path, "w") as configfile:
            config.write(configfile)

        return str(config_path)

    return _kafka_config_file


@pytest.fixture()
def encrypted_kafka_node_config():
    def _encrypted_kafka_node_config(
        config_file, nodeID, topic, rot_password, password, keytype, choice, limited
    ):
        root = pathlib.Path(config_file).parent
        (root / nodeID).mkdir(exist_ok=True)
        seed_path = root / nodeID / (nodeID + ".seed")
        cryptokey_path = root / nodeID / (nodeID + ".crypto")
        config_path = root / nodeID / (nodeID + ".config")

        _lifetime = 316224000  # lifetime (10 years)
        _usages = {
            "producer": ["key-encrypt"],
            "consumer": ["key-encrypt-request", "key-encrypt-subscribe"],
            "prodcon": ["key-encrypt", "key-encrypt-request", "key-encrypt-subscribe"],
            "prodcon-limited": ["key-encrypt", "key-encrypt-subscribe"],
            "consumer-limited": ["key-encrypt-subscribe"],
            "controller": ["key-encrypt-request"],
        }
        _keys = {
            "producer": "producer",
            "consumer": "consumer",
            "prodcon": "prodcon",
            "prodcon-limited": "prodcon",
            "consumer-limited": "consumer",
            "controller": "consumer",
        }

        rot = PasswordROT(rot_password, keytype)
        _rot = rot._pk
        _msgrot = msgpack.packb(
            [0, b"\x90", _rot], default=msgpack_default_pack, use_bin_type=True
        )
        _chainrot = _rot
        _msgchainrot = _msgrot
        prov = PasswordProvisioner(password, _rot, keytype)

        _signedprov = {
            "producer": None,
            "consumer": None,
            "prodcon": None,
        }
        for kn in _signedprov.keys():
            key = prov._pk[kn]
            poison = msgpack.packb(
                [["usages", _usages[kn]]],
                default=msgpack_default_pack,
                use_bin_type=True,
            )
            tosign = msgpack.packb(
                [0, poison, key], default=msgpack_default_pack, use_bin_type=True
            )
            _signedprov[kn] = rot._sk.crypto_sign(tosign)

        _msgchains = {
            "producer": msgpack.packb(
                [_signedprov[_keys["producer"]]], use_bin_type=True
            ),
            "consumer": msgpack.packb(
                [_signedprov[_keys["consumer"]]], use_bin_type=True
            ),
            "prodcon": msgpack.packb([_signedprov[_keys["prodcon"]]], use_bin_type=True),
            "prodcon-limited": msgpack.packb(
                [_signedprov[_keys["prodcon-limited"]]], use_bin_type=True
            ),
            "consumer-limited": msgpack.packb(
                [_signedprov[_keys["consumer-limited"]]], use_bin_type=True
            ),
            "controller": msgpack.packb(
                [_signedprov[_keys["controller"]]], use_bin_type=True
            ),
        }

        sole = False

        if choice == 1:
            key = "controller"
        elif choice == 2:
            key = "producer"
        elif choice == 3 and limited:
            key = "consumer-limited"
        elif choice == 3 and not limited:
            key = "consumer"
        elif choice == 4 and limited:
            key = "prodcon-limited"
        elif choice == 4 and not limited:
            key = "prodcon"
        else:
            assert False, "Invalid combination of choices!"

        # Check we have appropriate chains
        if choice == 1:
            # Controllers must be signed by ROT
            _msgchkrot = _msgrot
        else:
            # Everyone else by Chain ROT (may = ROT)
            _msgchkrot = _msgchainrot
        assert len(_msgchains[key]) > 0, (
            "A trusted chain for "
            + key
            + " is missing. This should not happen with simple-provision, please report as a bug."
        )
        pk = process_chain(_msgchains[key], None, None, allowlist=[_msgchkrot])[0]
        assert len(pk) >= 3 and pk[2] == prov._pk[_keys[key]], (
            "Malformed chain for " + key + ". Did you enter your passwords correctly?"
        )

        topics = None
        while topics is None:
            topics = list(set(map(lambda i: i.split(".", 1)[0], topic.split())))
            if len(topics) < 1:
                ans = input("Are you sure you want to allow all topics (Y/n)?")
                if len(ans) > 0 and (ans[0].lower() == "n"):
                    topics = None
                else:
                    topics = ["^.*$"]

        pathlen = -1

        print("Root topics will be:")
        print(topics)
        print(
            "Lifetime of initial crypto configuration will be",
            _lifetime / 86400,
            "days.",
        )
        if pathlen != -1:
            print("Maximum pathlength is", pathlen)
        else:
            print("No maximum pathlength")
        print("")

        # Generate KDF seed first, if needed
        if seed_path.exists():
            with open(seed_path, "rb+") as f:
                seedidx, rb = msgpack.unpackb(f.read(), raw=True)
                f.seek(0, 0)
                f.write(msgpack.packb([seedidx, rb], use_bin_type=True))
                f.flush()
                f.truncate()
        else:
            with open(seed_path, "wb") as f:
                seedidx = 0
                rb = randombytes(Ratchet.SECRETSIZE)
                f.write(msgpack.packb([seedidx, rb], use_bin_type=True))
        if choice == 2 or choice == 4:
            print(
                "There will be no escrow key for initial shared secret. If you lose connectivity for an extended period of time, you may lose access to data from this producer unless you store the following value in a secure location:"
            )
            print(nodeID + ":", hexlify(rb), " (key index", seedidx, ")")

        # Second, generate identify keypair and chain, and write cryptokey config file
        # TODO: this assumes there is only one key of each type, which should be true, but...
        eck = CryptoKey(cryptokey_path.as_posix(), keytypes=[keytype])
        for idx in range(0, eck.get_num_spk()):
            if eck.get_spk(idx).same_type(keytype):
                break
        pk = eck.get_spk(idx)

        poison = [["usages", _usages[key]]]
        if len(topics) > 0:
            poison.append(["topics", topics])
        if pathlen != -1:
            poison.append(["pathlen", pathlen])
        poison = msgpack.packb(poison, default=msgpack_default_pack, use_bin_type=True)
        msg = [time.time() + _lifetime, poison, pk]
        msg = prov._sk[_keys[key]].crypto_sign(
            msgpack.packb(msg, default=msgpack_default_pack, use_bin_type=True)
        )
        chain = msgpack.packb(
            msgpack.unpackb(_msgchains[key], raw=False) + [msg],
            default=msgpack_default_pack,
            use_bin_type=True,
        )
        print(nodeID, "Public Key:", str(pk))

        # Third, write config
        kcs = KafkaCryptoStore(config_path.as_posix(), nodeID)
        kcs.store_value("maxage", _lifetime, section="crypto")
        kcs.store_value("chain" + str(idx), chain, section="chains")
        kcs.store_value("rot" + str(idx), _msgrot, section="allowlist")
        if _msgchkrot != _msgrot:
            kcs.store_value("chainrot" + str(idx), _msgchkrot, section="allowlist")
        if kcs.load_value("temporary", section="allowlist"):
            print("Found temporary ROT, removing.")
            kcs.store_value("temporary", None, section="allowlist")
        # If controller, list of provisioners
        if choice == 1 and _msgchainrot != _msgrot and _msgchainrot != _msgchkrot:
            kcs.store_value("provisioners" + str(idx), _msgchainrot, section="allowlist")
        if kcs.load_value("cryptokey") is None:
            kcs.store_value("cryptokey", "file#" + nodeID + ".crypto")
        if kcs.load_value("ratchet") is None:
            kcs.store_value("ratchet", "file#" + nodeID + ".seed")
        if choice == 2 or choice == 4:
            if sole:
                kcs.store_value("mgmt_long_keyindex", False)
            else:
                kcs.store_value("mgmt_long_keyindex", True)
        kcs.store_value(
            "crypto_sub_interval", 5
        )  # Decrease crypto sub interval for testing
        print("Congratulations! Provisioning is complete.")

    return _encrypted_kafka_node_config
