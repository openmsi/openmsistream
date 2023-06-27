#!/bin/bash

set -euxo pipefail

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_directories

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.keys
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.reqs
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.subs

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_2

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.keys
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.reqs
docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.subs

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_s3_transfer_stream_processor

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_metadata_extractor_source

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_metadata_extractor_dest

docker exec local_kafka_broker kafka-topics --bootstrap-server broker:9092 --create --topic test_plots_for_tutorial
