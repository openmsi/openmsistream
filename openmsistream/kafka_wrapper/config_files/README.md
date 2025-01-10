This directory is the default location for configuration files, and for directories created by `ProvisionNode` for use with KafkaCrypto.

The `testing_node/testing_node_2` KafkaCrypto config files were created using `simple-provision.py` from KafkaCrypto, with a ROT password of `123456789012` and a provisioning password of `123456`, as both a producer and consumer, not limited to just controllers, and with these topics: `test_data_file_stream_processor_encrypted test_oms_encrypted test_oms_encrypted_heartbeats test_oms_encrypted_logs test_oms_undecryptable_messages_flag test_oms_undecryptable_messages`. Encrypted message tests that use new root topics require these files to be updated.

Note that after running simpleprovision, the file `testing_node/testing_node_2.config` had to be edited to remove these three lines:
```
bootstrap_servers :
security_protocol : SSL
ssl_cafile : /usr/lib/ssl/certs/ca-certificates.crt
```
So that broker configuration is pulled from the main OpenMSIStream configs.

It is also important that both have the same ROT password.

