#imports
from ..utilities.config_file_parser import ConfigFileParser

class S3ConfigFileParser(ConfigFileParser) :
    """
    A class to parse S3 bucket configurations from files
    """

    @property
    def s3_configs(self) :
        if self.__s3_configs is None :
            self.__s3_configs = self._get_config_dict('s3')
        return self.__s3_configs

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__s3_configs = None
