#imports
from ..shared.config_file_parser import ConfigFileParser

class OSNConfigFileParser(ConfigFileParser) :
    """
    A class to parse OSN configurations from files
    """

    @property
    def osn_configs(self) :
        if self.__osn_configs is None :
            self.__osn_configs = self._get_config_dict('osn')
        return self.__osn_configs

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__osn_configs = None
