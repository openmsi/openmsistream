## Programs to upload/download contents of arbitrary data files

The `openmsistream` code provides a few programs to use for uploading or downloading arbitrary data stored in files on disk.

### UploadDataFile

This module uploads a single specified file to a topic on a broker by breaking it into chunks of a particular size and uploading those chunks in several parallel threads. To run it in the most common use case, enter the following command and arguments:

`UploadDataFile [file_path] --config [config_file_path] --topic_name [topic_name]`

where `[file_path]` is the path to the text file to upload, `[config_file_path]` is the path to a config file including at least `[broker]` and `[producer]` sections, and `[topic_name]` is the name of the topic to produce to. Running the code will produce all the chunks of the single file to the topic; the process will hang until receipts of delivery come back for every message that was produced.

Options for running the code include:
1. Changing the maximum number of parallel threads allowed to run at a time: add the `--n_threads [threads]` argument where `[threads]` is the desired number of parallel threads to allow (the default is 5 threads).
1. Changing the size of the individual file chunks: add the `--chunk_size [n_bytes]` argument where `[n_bytes]` is the desired chunk size in bytes. `[n_bytes]` must be a nonzero power of two (the default is 16384).

To see other optional command line arguments, run `UploadDataFile -h`. The Python Class defining this module is [here](./upload_data_file.py).

### DataFileUploadDirectory

This module uploads any files that are added to a given directory path to a topic on a broker using the same "chunking" idea as above. It also preserves subdirectory structure relative to the watched directory. To run it in the most common use case, enter the following command and arguments:

`DataFileUploadDirectory [directory_path] --config [config_file_path] --topic_name [name_of_topic]`

where `[directory_path]` is the path to a directory to monitor for files to upload, `[config_file_path]` is the path to a config file including at least `[broker]` and `[producer]` sections, and `[topic_name]` is the name of the topic to produce to. Running the code will automatically enqueue any files added to the directory during runtime to be produced. If you would like to upload files that already exist in the directory when the program is started up, add the `--upload_existing` flag; otherwise only new files added will be uploaded.

While the main process is running, a line with a "`.`" character will be printed out every five minutes to indicate the process is still alive. At any time, typing "`check`" or "`c`" into the console will print a message specifying how many total files have been enqueued or are in progress. Messages will be printed to the console showing how many chunks each file is broken into, and the progress of actually producing those chunks to the topic. The processes can be shut down by typing "`quit`" or "`q`" into the console. Note that the process won't actually shut down until all currently enqueued messages have been delivered to the broker (or returned an error). Also note that the files will have all of their chunks enqueued almost immediately, but actually producing the chunks to the broker may take slightly more time depending on how many files are being uploaded at once.

Using a `DataFileUploadDirectory` to Produce chunks of files to the broker is robust if the code crashes and can be restarted. A special subdirectory called "`LOGS`" is created in `[directory_path]` when the code starts up (any files added to the "LOGS" subdirectory will not be uploaded). That subdirectory will include the log file, as well as two files called "`files_to_upload_to_[name_of_topic].csv`" and "`files_fully_uploaded_to_[name_of_topic].csv`". The `.csv` files are special datatable files that list [the chunks of each recognized file that still need to be uploaded](./producer_file_registry.py#L8-L17) and [information about files that have been fully uploaded](./producer_file_registry.py#L19-L25), respectively. The list of chunks uploaded for each file is updated atomically upon receipt of a positive acknowledgment callback from the broker: chunks are not listed as uploaded until they have been fully received and acknowledged by the broker and therefore guaranteed to exist on the topic. When `DataFileUploadDirectory` is restarted pointing to the same directory and topic, any files that did not get fully uploaded will have their missing chunks re-enqeued for upload if they still exist in the same location. 

If the same file is produced multiple times to the same topic, it will appear multiple times in the "fully_uploaded" file. Files uploaded to different topics from the same directory will have their own independent `.csv` files. The files are atomic and accurate to within 5 seconds. You can copy and then browse them while the code is running to check which files have been fully uploaded or recognized.

Options for running the code include:
1. Changing the maximum number of parallel threads allowed to run at a time: add the `--n_threads [threads]` argument where `[threads]` is the desired number of parallel threads to use. The default is 5 threads.
1. Changing which files in the directory get uploaded: add the `--upload_regex [regex]` argument where `[regex]` is a regular expression. Only files whose names match `[regex]` will be uploaded. The default excludes any hidden files (names start with '.') and log files (names end with '.log').
1. Uploading existing files as well as newly-added files: add the `--upload_existing` flag.
1. Changing the size of the individual file chunks: add the `--chunk_size [n_bytes]` argument where `[n_bytes]` is the desired chunk size in bytes. `[n_bytes]` must be a nonzero power of two (the default is 16384).
1. Changing the number of messages that are allowed to be internally queued at once (that is, queued before being produced): add the `--queue_max_size [n_messages]` argument where `[n_messages]` is the desired number of messages allowed in the internal queue (the default is 3000 messages). This internal queue is used to make sure that there's some buffer between recognizing a file exists to be uploaded and producing all of its associated messages to the topic; its size should be set to some number of messages such that the total size of the internal queue is capped at a few batches of messages ("`batch.size`" in the producer config). The default values supplied are well compatible.
1. Changing how often the "still alive" character is printed to the console: add the `--update_seconds [seconds]` argument where `[seconds]` is the number of seconds to wait between printing the character to the console from the main thread (the default is 300 seconds). Giving -1 for this argument disables printing the "still alive" character entirely.

To see other optional command line arguments, run `DataFileUploadDirectory -h`. The Python Class defining this module is [here](./data_file_upload_directory.py).

### DataFileDownloadDirectory

This module subscribes a group of consumers to a topic on a broker and passively listens in several parallel threads for messages that are file chunks of the type produced by `upload_data_file`. It reconstructs files produced to the topic from their individual chunks and puts the reconstructed files in a specified directory, preserving any subdirectory structure on the production end. To run it in the most common use case, enter the following command and arguments:

`DataFileDownloadDirectory [working_directory_path] --config [config_file_path] --topic_name [topic_name]`

where `[working_directory_path]` is the path to the directory that the reconstructed files should be put in (if it doesn't exist it will be created), `[config_file_path]` is the path to a config file including at least `[broker]` and `[consumer]` sections, and `[topic_name]` is the name of the topic to subscribe to/consume messages from. 

While the main process is running, a line with a "`.`" character will be printed out every five minutes to indicate the process is still alive. At any time, typing "`check`" or "`c`" into the console will print a message specifying how many total messages have been read and how many files have been completely reconstructed. When all the messages for a single file have been received and the file is completely reconstructed, a message will be printed to the console saying what file it was. The processes can be shut down at any time by typing "`quit`" or "`q`" into the console.

Options for running the code include:
1. Changing the maximum number of parallel threads allowed to run at a time: add the `--n_threads [threads]` argument where `[threads]` is the desired number of parallel threads to use (and, also, the number of consumers to allow in the group). The default is 4 threads/consumers; increasing this number may give Kafka warnings or errors depending on how many consumers can be subscribed to a particular topic.
1. Changing how often the "still alive" character is printed to the console: add the `--update_seconds [seconds]` argument where `[seconds]` is the number of seconds to wait between printing the character to the console from the main thread (the default is 300 seconds). Giving -1 for this argument disables printing the "still alive" character entirely. 

To see other optional command line arguments, run `DataFileDownloadDirectory -h`. The Python Class defining this module is [here](./data_file_download_directory.py).
