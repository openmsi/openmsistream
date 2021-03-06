#imports
import pathlib, os, sys, platform
from subprocess import check_output, CalledProcessError
from .config import SERVICE_CONST

def get_os_name() :
    """
    Return the name of the operating system the Service is being installed or running on
    """
    if platform.system()=='Windows' :
        return 'Windows'
    elif platform.system()=='Linux' :
        return 'Linux'
    #MacOS is not supported
    elif platform.system()=='Darwin' :
        errmsg = 'ERROR: Installing programs as Services is not supported on MacOS!'
        SERVICE_CONST.LOGGER.error(errmsg,NotImplementedError)
    #otherwise I don't know what happened
    else :
        errmsg = f'ERROR: could not determine operating system from platform.system() output "{platform.system()}"'
        SERVICE_CONST.LOGGER.error(errmsg,ValueError)

def run_cmd_in_subprocess(args,*,shell=False,logger=None) :
    """
    run a command in a subprocess and return its result, printing and re-throwing any exceptions it causes
    """
    if isinstance(args,str) :
        args = [args]
    try :
        result = check_output(args,shell=shell,env=os.environ)
        return result
    except CalledProcessError as e :
        errmsg = 'ERROR: failed to run a command! '
        if e.output is not None and e.output.strip()!='' :
            errmsg+= f'\noutput:\n{e.output.decode()}'
        if e.stdout is not None and e.stdout.strip()!=''  :
            errmsg+= f'\nstdout:\n{e.stdout.decode()}'
        if e.stderr is not None and e.stderr.strip()!='' :
            errmsg+= f'\nstderr:\n{e.stderr.decode()}'
        if logger is not None :
            logger.error(errmsg,exc_obj=e)
        else :
            SERVICE_CONST.LOGGER.error(errmsg,exc_obj=e)

def set_env_var(var_name,var_val) :
    """
    set an environment variable given its name and value
    """
    if get_os_name()=='Windows' :
        pwrsh_cmd = f'[Environment]::SetEnvironmentVariable("{var_name}","{var_val}",[EnvironmentVariableTarget]::Machine)'
        run_cmd_in_subprocess(['powershell.exe',pwrsh_cmd])
    elif get_os_name()=='Linux' :
        run_cmd_in_subprocess(['export',f'{var_name}={var_val}'],shell=True)
    os.environ[var_name]=var_val

def remove_env_var(var_name) :
    """
    remove an environment variable given its name
    """
    if get_os_name()=='Windows' :
        pwrsh_cmd = f'[Environment]::SetEnvironmentVariable("{var_name}",$null,[EnvironmentVariableTarget]::Machine)'
        run_cmd_in_subprocess(['powershell.exe',pwrsh_cmd])
    elif get_os_name()=='Linux' :
        run_cmd_in_subprocess(['unset',var_name],shell=True)
    else :
        raise NotImplementedError
    os.environ[var_name]=''

def set_env_var_from_user_input(var_name) :
    """
    set an environment variable with the given name and description based on user input
    """
    var_val = input(f'Please enter the value for environment variable {var_name}: ')
    set_env_var(var_name,var_val)

def set_env_vars(env_var_names,interactive=True) :
    """
    set the necessary environment variables
    """
    variables_set = False
    for env_var_name in env_var_names :
        if (not interactive) and (env_var_name in ('KAFKA_PROD_CLUSTER_USERNAME','KAFKA_PROD_CLUSTER_PASSWORD')) :
            continue
        if os.path.expandvars(f'${env_var_name}') == f'${env_var_name}' :
            if interactive :
                set_env_var_from_user_input(env_var_name)
                variables_set = True
            else :
                raise RuntimeError(f'ERROR: a value for the {env_var_name} environment variable is not set!')
        else :
            if interactive :
                choice = input(f'A value for the {env_var_name} is already set, would you like to reset it? [y/(n)]: ')
                if choice.lower() in ('yes','y') :
                    set_env_var_from_user_input(env_var_name)
                    variables_set = True
    return (variables_set and get_os_name()=='Windows')

def test_python_code() :
    """
    briefly test the python code for the repo to catch any errors
    """
    must_rerun = set_env_vars(['KAFKA_TEST_CLUSTER_USERNAME','KAFKA_TEST_CLUSTER_PASSWORD'])
    if must_rerun :
        msg = 'New values for environment variables have been set. '
        msg+= 'Please close this window and rerun InstallService so that their values get picked up.'
        SERVICE_CONST.LOGGER.info(msg)
        sys.exit(0)
    SERVICE_CONST.LOGGER.debug('Testing code to check for errors...')
    unittest_dir_path = pathlib.Path(__file__).parent.parent.parent / 'test' / 'unittests'
    SERVICE_CONST.LOGGER.debug(f'Running all unittests in {unittest_dir_path}...')
    run_cmd_in_subprocess([f'{sys.executable}','-m','unittest','discover','-s',f'{unittest_dir_path}','-vf'])
    SERVICE_CONST.LOGGER.debug('All unittest checks complete : )')
