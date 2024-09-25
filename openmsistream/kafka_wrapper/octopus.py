import os
from confluent_kafka import Consumer, TopicPartition
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"


# followed https://github.com/aws/aws-msk-iam-sasl-signer-python
def update_configs_if_use_octopus(all_configs):
    if "OCTOPUS_BOOTSTRAP_SERVERS" in os.environ:
        assert os.environ["AWS_ACCESS_KEY_ID"]
        assert os.environ["AWS_SECRET_ACCESS_KEY"]

        def oauth_cb(oauth_config):
            auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
            print(auth_token)
            return auth_token, expiry_ms / 1000

        all_configs.update({"oauth_cb": oauth_cb})
        print("Octopus::all_configs=", all_configs, os.getpid())


def find_starting_offsets_if_use_octopus(topic_name):
    starting_offsets = []
    if "OCTOPUS_BOOTSTRAP_SERVERS" in os.environ:
        conf = {
            "bootstrap.servers": os.environ["OCTOPUS_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "group.id": "mygroup",
        }
        update_configs_if_use_octopus(conf)
        consumer = Consumer(conf)
        consumer.poll(1)  # to receive metadata
        cluster_metadata = consumer.list_topics(topic=topic_name)
        for pa in cluster_metadata.topics[topic_name].partitions:
            lo, hi = consumer.get_watermark_offsets(TopicPartition(topic_name, pa))
            starting_offset = TopicPartition(topic_name, pa, hi)
            starting_offsets.append(starting_offset)
            print("Octopus::starting_offset=", starting_offset)
        return starting_offsets
