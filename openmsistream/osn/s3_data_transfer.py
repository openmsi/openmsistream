# @author: Amir H. Sharifzadeh, https://pages.jh.edu/asharif9/
# @project: OpenMSI
# @date: 04/12/2022
# @owner: The Institute of Data Intensive Engineering and Science, https://idies.jhu.edu/
# Johns Hopkins University http://www.jhu.edu
# import logging

from botocore.exceptions import ClientError
from .osn_service import OSNService

class S3DataTransfer(OSNService) :

    def __init__(self, osn_config, *args, **kwargs):
        super().__init__(osn_config,*args,**kwargs)

    def transfer_object_stream(self, topic_name,datafile):
        file_name = str(datafile.filename)
        sub_dir=datafile.subdir_str
        osn_full_path = topic_name + '/' + sub_dir + '/' + file_name
        try:
            self.s3_client.put_object(Body=datafile.bytestring, Bucket=self.bucket_name,
                                      Key=osn_full_path,
                                      # GrantRead=self.grant_read
                                      )
            msg = file_name + ' successfully transferred into /' + sub_dir
            self.logger.info(msg)
        except ClientError as e:
            self.logger.error(e.response + ': failed to transfer ' + file_name + ' into /' + sub_dir)
