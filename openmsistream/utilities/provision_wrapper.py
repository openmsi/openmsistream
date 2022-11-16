"""
A wrapper around KafkaCrypto provision scripts to automatically
move the output in the location expected by OpenMSIStream
"""

#imports
import pathlib, shutil, logging, warnings
from argparse import ArgumentParser
import urllib.request
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import kafkacrypto
from ..utilities.config import RUN_CONST
from .logging import Logger
from .config_file_parser import ConfigFileParser
from .misc import change_dir

#constants
LOGGER = Logger('ProvisionNode',logging.INFO)
KC_PATH = kafkacrypto.__path__
SP_NAME = 'simple-provision.py'
OP_NAME = 'online-provision.py'
GITHUB_URL = f'https://raw.githubusercontent.com/tmcqueen-materials/kafkacrypto/master/tools/{SP_NAME}'
TEMP_DIR_PATH = RUN_CONST.CONFIG_FILE_DIR/'temp_kafkacrypto_dir'

def get_script_location_and_code_from_input(args) :
    """
    Use a given path to get the script code
    """
    p_code = None
    p_loc = None
    if args.script_path.is_file() :
        p_loc = args.script_path
        with open(args.script_path,'rb') as fp :
            p_code = fp.read()
    #directory containing the script was given
    elif args.script_path.is_dir() :
        if (args.script_path/SP_NAME).is_file() :
            p_loc = args.script_path/SP_NAME
            with open(args.script_path/SP_NAME,'rb') as fp :
                p_code = fp.read()
        elif (args.script_path/OP_NAME).is_file() :
            p_loc = args.script_path/OP_NAME
            with open(args.script_path/OP_NAME,'rb') as fp :
                p_code = fp.read()
    return p_loc, p_code

def get_script_location_and_code(args) :
    """
    Return the location of the provision script and its text contents, given the arguments
    """
    p_code = None
    p_loc = None
    if args.script_path is not None:
        #path to the script itself was given
        p_loc, p_code = get_script_location_and_code_from_input(args)
    #if not set yet, try getting from the kafkacrypto install location (works if installed with --editable)
    if len(KC_PATH)==1 :
        if args.mode=='simple' :
            to_try = pathlib.Path(KC_PATH[0]).parent/'tools'/SP_NAME
        elif args.mode=='online' :
            to_try = pathlib.Path(KC_PATH[0]).parent/'tools'/OP_NAME
        if to_try.is_file() :
            p_loc = to_try
            with open(to_try,'rb') as fp :
                p_code = fp.read()
    if p_code is None :
        #if all else fails, try getting from the Github webpage (only simple-provision can be fetched this way)
        if args.mode=='simple' :
            try :
                with urllib.request.urlopen(GITHUB_URL) as urlp :
                    p_code = urlp.read()
                p_loc = GITHUB_URL
            except Exception :
                pass
    if p_loc is None or p_code is None :
        LOGGER.error('ERROR: failed to find the provisioning script to use!',exc_type=RuntimeError)
    return p_loc, p_code

def move_files(p_loc) :
    """
    Move the created files into the expected locations
    """
    try :
        new_files = {}
        exts = ['.config','.seed','.crypto']
        for ext in exts :
            n_files = 0
            for fp in TEMP_DIR_PATH.glob(f'*{ext}') :
                n_files+=1
                new_files[ext] = fp.resolve()
            if n_files!=1 :
                LOGGER.error(f'ERROR: found {n_files} new {ext} files in {TEMP_DIR_PATH}',exc_type=RuntimeError)
        node_id = None
        for ext in exts :
            filename = new_files[ext].name
            this_node_id = ((filename).split(ext))[0]
            if node_id is None :
                node_id = this_node_id
            elif node_id!=this_node_id :
                errmsg = f'ERROR: found a file called {filename} that conflicts with node_id {node_id}!'
                LOGGER.error(errmsg,exc_type=RuntimeError)
        cfp = ConfigFileParser(new_files['.config'],logger=LOGGER)
        default_dict = cfp.get_config_dict_for_groups('DEFAULT')
        if 'node_id' not in default_dict :
            LOGGER.error(f"ERROR: node_id not listed in {new_files['.config']}!",exc_type=ValueError)
        elif default_dict['node_id']!=node_id :
            errmsg = f"ERROR: node_id listed in {new_files['.config']} mismatched to filenames ({node_id})!"
            LOGGER.error(errmsg,exc_type=ValueError)
        new_dirpath = TEMP_DIR_PATH.parent/node_id
        TEMP_DIR_PATH.rename(new_dirpath)
        LOGGER.info(f'Successfuly set up new KafkaCrypto node called "{node_id}"')
    except Exception as exc :
        errmsg = f'ERROR: Running {p_loc} did not produce the expected output! Temporary directories '
        errmsg+= 'will be removed and you will need to try again. Exception will be logged and re-raised.'
        LOGGER.error(errmsg,exc_info=exc,reraise=True)
    finally :
        if TEMP_DIR_PATH.is_dir() :
            shutil.rmtree(TEMP_DIR_PATH)

def main() :
    """
    Script to run either simple or online provision for KafkaCrypto and
    move the output to the location expected by OpenMSIStream
    """
    #command line arguments
    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--mode', choices=['simple','online'], default='simple',
                       help='''Choice of which known type of provisioning to use.
                               Can also provide a path to the script to run instead.''')
    group.add_argument('--script-path', type=pathlib.Path, default='.',
                        help='Path to the provision script to run')
    args = parser.parse_args()
    if TEMP_DIR_PATH.is_dir() :
        shutil.rmtree(TEMP_DIR_PATH)
    #get the location/content of the simple-provision script
    p_loc, p_code = get_script_location_and_code(args)
    #run the script
    try :
        if not TEMP_DIR_PATH.is_dir() :
            TEMP_DIR_PATH.mkdir(parents=True)
        with change_dir(TEMP_DIR_PATH) :
            exec(p_code)
    except Exception as exc :
        errmsg = f'ERROR: failed to run provisioning using {p_loc}! Exception will be logged and re-raised.'
        LOGGER.error(errmsg,exc_info=exc,reraise=True)
    #make sure required files exist and move them into a new directory named for the node_ID
    move_files(p_loc)

if __name__=='__main__' :
    main()
