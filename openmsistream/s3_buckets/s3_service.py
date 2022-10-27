"""Utilities for working with S3 buckets"""

# @author: Amir H. Sharifzadeh, https://pages.jh.edu/asharif9/
# @project: OpenMSI
# @date: 04/12/2022
# @owner: The Institute of Data Intensive Engineering and Science, https://idies.jhu.edu/
# Johns Hopkins University http://www.jhu.edu/
import hashlib
import boto3
from ..utilities import LogOwner

class S3Service(LogOwner) :
    """
    A class to work with an S3 connection session
    """

    def __init__(self, s3_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = boto3.session.Session()

        self.bucket_name = s3_config['bucket_name']
        endpoint_url = str(s3_config['endpoint_url'])

        if not endpoint_url.startswith('https://'):
            endpoint_url = 'https://' + endpoint_url
        self.s3_client = self.session.client(
            service_name='s3',
            aws_access_key_id=s3_config['access_key_id'],
            aws_secret_access_key=s3_config['secret_key_id'],
            region_name=s3_config['region'],
            endpoint_url= endpoint_url
        )
        self.region = s3_config['region']
        self.grant_read = 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

    def get_object_stream_by_object_key(self, bucket_name, object_key):
        """
        Return the stream of an object in a bucket given its key
        """
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return s3_response_object['Body'].read()

    def delete_object_from_bucket(self, bucket_name, object_key):
        """
        Delete an object with a given key from a bucket
        """
        try:
            # make sure object exits before deleting the object from the bucket
            s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            if s3_response_object is None:
                msg = 'The object ' + object_key + ' does not exist in this bucket: ' + bucket_name
                self.logger.error(msg)
                return
        except Exception:
            msg  = 'A problem occurred while reading ' + object_key + ' from bucket: ' + bucket_name
            self.logger.error(msg)
            return
        # delete the object from the bucket safely
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            msg = object_key + ' was deleted successfully from bucket: ' + bucket_name
            self.logger.debug(msg)
        except Exception:
            msg = 'Could not delete ' + object_key + ' from bucket: ' + bucket_name
            self.logger.error(msg)
            return

    def compare_producer_datafile_with_s3_object_stream(self, bucket_name, object_key, hashed_datafile_stream):
        """
        Compare a given DataFile hash with the hash of an object in a bucket
        """
        if hashed_datafile_stream is None:
            return False
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        object_content = s3_response_object['Body'].read()
        md5 = hashlib.md5()
        md5.update(object_content)
        if object_content is None:
            return False
        hhh = format(md5.hexdigest())
        return str(hhh) == hashed_datafile_stream

    def compare_consumer_datafile_with_s3_object_stream(self, bucket_name, object_key, datafile):
        """
        Compare a given DataFile with an object in a bucket using their hashes
        """
        if datafile is None:
            return False
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        object_content = s3_response_object['Body'].read()
        if object_content is None:
            return False
        datafile_md5 = hashlib.md5()
        datafile_md5.update(datafile.bytestring)
        object_content_md5 = hashlib.md5()
        object_content_md5.update(object_content)
        return format(datafile_md5.hexdigest()) == format(object_content_md5.hexdigest())
