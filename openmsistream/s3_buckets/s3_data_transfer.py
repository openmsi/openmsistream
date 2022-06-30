# @author: Amir H. Sharifzadeh, https://pages.jh.edu/asharif9/
# @project: OpenMSI
# @date: 04/12/2022
# @owner: The Institute of Data Intensive Engineering and Science, https://idies.jhu.edu/
# Johns Hopkins University http://www.jhu.edu
# import logging

from botocore.exceptions import ClientError
from .s3_service import S3Service

class S3DataTransfer(S3Service) :

    def __init__(self, s3_config, *args, **kwargs):
        super().__init__(s3_config,*args,**kwargs)

    def transfer_object_stream(self, topic_name,datafile):
        file_name = str(datafile.filename)
        sub_dir=datafile.subdir_str
        s3_full_path = topic_name 
        if sub_dir is not None :
            s3_full_path+= '/' + sub_dir 
        s3_full_path+= '/' + file_name
        try:
            self.s3_client.put_object(Body=datafile.bytestring, Bucket=self.bucket_name,
                                      Key=s3_full_path,
                                      # GrantRead=self.grant_read
                                      )
            msg = f'{file_name} successfully transferred into {self.bucket_name}/{s3_full_path}'
            self.logger.info(msg)
        except ClientError as e:
            self.logger.error(f'{e.response}: failed to transfer {file_name} into {self.bucket_name}/{s3_full_path}')
