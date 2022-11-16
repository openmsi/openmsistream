"""Wrapper around a Python ConfigParser to simplify some commonly-used operations"""

#imports
import os, configparser
from .logging import LogOwner

class ConfigFileParser(LogOwner) :
    """
    A class to parse configurations from files
    """

    #################### PROPERTIES ####################

    @property
    def has_default(self) :
        """
        True if a config file has a DEFAULT section
        """
        return 'DEFAULT' in self._config
    @property
    def available_group_names(self) :
        """
        The list of section names in the config file
        """
        return self._config.sections()
    @property
    def env_var_names(self) :
        """
        A generator of the environment variable names used in the config file
        """
        for csd in self._config.values() :
            for v in csd.values() :
                if v.startswith('$') :
                    yield v[1:]

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,config_path,*args,**kwargs) :
        """
        config_path = path to the config file to parse
        """
        super().__init__(*args,**kwargs)
        self.filepath = config_path
        if not config_path.is_file() :
            self.logger.error(f'ERROR: configuration file {config_path} does not exist!',exc_type=FileNotFoundError)
        self._config = configparser.ConfigParser()
        self._config.read(config_path)

    def get_config_dict_for_groups(self,group_names) :
        """
        Return a config dictionary populated with configurations from groups with the given names

        group_names = the list of group names to add to the dictionary (or a single string)
        """
        if isinstance(group_names,str) :
            group_names = [group_names]
        config_dict = {}
        for group_name in group_names :
            if group_name not in self._config :
                errmsg = f'ERROR: {group_name} is not a recognized section in {self.filepath}!'
                self.logger.error(errmsg,exc_type=ValueError)
            for key, value in self._config[group_name].items() :
                #don't add the 'node_id' to groups for brokers, producers, or consumers
                if key=='node_id' and group_name in ['broker','producer','consumer'] :
                    continue
                #if the value is an environment variable, expand it on the current system
                if value.startswith('$') :
                    exp_value = os.path.expandvars(value)
                    if exp_value == value :
                        errmsg = f'ERROR: Expanding {value} in {self.filepath} as an environment variable failed '
                        errmsg+= '(must be set on system)'
                        self.logger.error(errmsg,exc_type=ValueError)
                    else :
                        value = exp_value
                config_dict[key] = value
        return config_dict

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _get_config_dict(self,group_name) :
        to_return = {}
        if group_name in self.available_group_names :
            to_return = self.get_config_dict_for_groups(group_name)
        return to_return
