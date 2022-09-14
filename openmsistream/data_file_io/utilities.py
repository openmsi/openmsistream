"""Various utility functions used elsewhere in this subdirectory"""

#imports
import datetime
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME

def get_encrypted_message_timestamp_string(msg) :
    """
    Given a Message object containing still-encrypted keys and/or values,
    return a string describing the timestamp in local time.
    """
    format_string = "on_%b_%d_%Y_at_%H_%M_%S_%f"
    ts_type, timestamp = msg.timestamp
    if ts_type==TIMESTAMP_NOT_AVAILABLE :
        timestamp_string = f'consumed_{datetime.datetime.now().strftime(format_string)}'
    elif ts_type in (TIMESTAMP_CREATE_TIME,TIMESTAMP_LOG_APPEND_TIME) :
        utc_dt = datetime.datetime.fromtimestamp(timestamp/1000.,tz=datetime.timezone.utc)
        local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        local_dt = utc_dt.astimezone(local_timezone)
        if ts_type==TIMESTAMP_CREATE_TIME :
            timestamp_string = 'produced'
        elif ts_type==TIMESTAMP_LOG_APPEND_TIME :
            timestamp_string = 'received'
        timestamp_string+=f'_{local_dt.strftime(format_string)}'
    else :
        raise ValueError(f'ERROR: Unrecognized timestamp type {ts_type}')
    return timestamp_string

def get_encrypted_message_key_and_value_filenames(msg,topic_name) :
    """
    Given a Message object containing still-encrypted keys and/or values and the name
    of the topic it came from, return the names of the files to which the key and
    value should have their bytes written
    """
    timestamp_string = get_encrypted_message_timestamp_string(msg)
    fp_pp = f'encrypted_message_from_{topic_name}_{timestamp_string}'
    return f'{fp_pp}_key.bin',f'{fp_pp}_value.bin'

def get_message_prepend(subdir_str,filename) :
    """
    Return the prepend for each message key, based on the filepath
    """
    key_pp = ''
    if subdir_str is not None :
        key_pp = f'{"_".join(subdir_str.split("/"))}'
    if key_pp!='' :
        key_pp+='_'
    return f'{key_pp}{filename}_chunk'
