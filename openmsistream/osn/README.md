### General information

The Open Storage Network (OSN) is intended for data integrity coming from different labs through kafka-streaming into corresponding objects.

### OSNStreamProcessor

This module uploads data from streaming-consumer into OSN directly.

### OSNStreamProcessor Command Line and Environmental Configuration

There are different approaches that data could be sent into OSN. In this particular module we send data through s3Client. Therefore, we will need to access the following information:

i. Environmental Configuration, under [osn] in an inputted config file :
   1) access_key_id ($ACCESS_SECRET_KEY_ID)
   2) secret_key_id ($SECRET_KEY_ID)
   3) endpoint_url ($ENDPOINT_URL)
   4) region ($REGION)

The above items from 1-4, need to be set as environment variables on your machine, or otherwise specified in the configuration file you use. 

ii. Command Line:
   5) bucket_name
   6) topic_name
   7) config file (optional)
   8) logger_file (optional)

The bucket_name needs to be entered as the first argument from the command line. You will also need to add the name of your topic on the command line (as this module consumes data from a specific topic). 

Optional: If you want to use a custom config file, you will need to add '--config [path_to_config_file]'.

Optional: As this module interacts with consumer, it will log events somewhere on your local machine. If you do not specify a file or folder for that with '--logger_file [path_to_log_file_or_directory]', the log file will be saved in the current directory,

Example of a command line with optional commands:
args = ['C:\\osn_data', 'bucket01', '--config', 'C:\\config_files\\test.config', '--topic_name', 'topic_1']

Example of a command line without optional commands:
args = ['bucket01', '--config', '--topic_name', 'topic_1']
