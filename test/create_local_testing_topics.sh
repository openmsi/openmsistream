#!/bin/sh

set -euxo pipefail

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_directories

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.keys
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.reqs
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_oms_encrypted.subs

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_2

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.keys
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.reqs
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_data_file_stream_processor_encrypted.subs

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_s3_transfer_stream_processor

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_metadata_extractor_source

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_metadata_extractor_dest

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test_plots_for_tutorial
