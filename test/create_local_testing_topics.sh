#!/bin/bash

set -euxo pipefail

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_data_file_directories

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_oms_encrypted
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_oms_encrypted.keys
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_oms_encrypted.reqs
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_oms_encrypted.subs

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_data_file_stream_processor

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_data_file_stream_processor_2

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_data_file_stream_processor_encrypted
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_data_file_stream_processor_encrypted.keys
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_data_file_stream_processor_encrypted.reqs
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 1 --replication-factor 1 --topic test_data_file_stream_processor_encrypted.subs

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_s3_transfer_stream_processor

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_metadata_extractor_source
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_metadata_extractor_dest

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_plots_for_tutorial

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --partitions 2 --replication-factor 1 --topic test_girder_upload_stream_processor
