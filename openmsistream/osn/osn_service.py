# @author: Amir H. Sharifzadeh, https://pages.jh.edu/asharif9/
# @project: OpenMSI
# @date: 04/12/2022
# @owner: The Institute of Data Intensive Engineering and Science, https://idies.jhu.edu/
# Johns Hopkins University http://www.jhu.edu/
import hashlib
import boto3
from ..shared.logging import LogOwner

class OSNService(LogOwner) :

    def __init__(self, osn_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = boto3.session.Session()

        self.bucket_name = osn_config['bucket_name']
        endpoint_url = str(osn_config['endpoint_url'])

        if not endpoint_url.startswith('https://'):
            endpoint_url = 'https://' + endpoint_url
        self.s3_client = self.session.client(
            service_name='s3',
            aws_access_key_id=osn_config['access_key_id'],
            aws_secret_access_key=osn_config['secret_key_id'],
            region_name=osn_config['region'],
            endpoint_url= endpoint_url
        )
        self.region = osn_config['region']
        self.grant_read = 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

    # def set_upload_config(self, osn_config):
    #
    #     self.bucket_name = osn_config['bucket_name']
    #     self.grant_read = 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

    def get_object_stream_by_object_key(self, bucket_name, object_key):
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return s3_response_object['Body'].read()

    def get_object_stream_by_osn_datafile(self, topic_name, bucket_name, datafile):
        file_name = str(datafile.filename)
        sub_dir = datafile.subdir_str
        object_key = topic_name + '/' + sub_dir + '/' + file_name
        return self.get_object_stream_by_object_key(bucket_name, object_key)

    def delete_object_from_osn(self, bucket_name, object_key):
        try:
            # make sure object exits before deleting the object from osn
            s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            if s3_response_object == None:
                msg = 'The object ' + object_key + ' not exists in this bucket: ' + bucket_name
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
            self.logger.info(msg)
        except Exception:
            msg = 'Could not delete ' + object_key + ' from bucket: ' + bucket_name
            self.logger.error(msg)
            return

    def find_by_object_key(self, key):
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        for obj in objects['Contents']:
            object_key = obj[key]
            if object_key != None:
                return object_key
        return None

    def compare_producer_datafile_with_osn_object_stream(self, bucket_name, object_key, hashed_datafile_stream):
        if hashed_datafile_stream == None:
            return False
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        object_content = s3_response_object['Body'].read()
        md5 = hashlib.md5()
        md5.update(object_content)
        if object_content == None:
            return False
        hhh = format(md5.hexdigest())
        return str(hhh) == hashed_datafile_stream

    def compare_consumer_datafile_with_osn_object_stream(self, topic_name, bucket_name, datafile):
        if datafile == None:
            return False
        file_name = str(datafile.filename)
        sub_dir = datafile.subdir_str
        object_key = topic_name + '/' + sub_dir + '/' + file_name
        s3_response_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        object_content = s3_response_object['Body'].read()
        if object_content == None:
            return False
        datafile_md5 = hashlib.md5()
        datafile_md5.update(datafile.bytestring)
        object_content_md5 = hashlib.md5()
        object_content_md5.update(object_content)
        return format(datafile_md5.hexdigest()) == format(object_content_md5.hexdigest())

    def get_all_object_names(self, bucket_name):
        object_names = []
        objects = self.s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objects['Contents']:
            object_names.append(obj['Key'])
        return object_names

    def close_session(self):
        pass