"""Various helper functions for working with Services/daemons"""

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
    if platform.system()=='Linux' :
        return 'Linux'
    #MacOS is not supported
    if platform.system()=='Darwin' :
        errmsg = 'ERROR: Installing programs as Services is not supported on MacOS!'
        SERVICE_CONST.logger.error(errmsg,exc_type=NotImplementedError)
    #otherwise I don't know what happened
    else :
        errmsg = f'ERROR: could not determine operating system from platform.system() output "{platform.system()}"'
        SERVICE_CONST.logger.error(errmsg,exc_type=ValueError)
    return None

def run_cmd_in_subprocess(args,*,shell=False,logger=None) :
    """
    run a command in a subprocess and return its result, printing and re-throwing any exceptions it causes
    """
    if isinstance(args,str) :
        args = [args]
    try :
        result = check_output(args,shell=shell,env=os.environ)
        return result
    except CalledProcessError as exc :
        errmsg = 'ERROR: failed to run a command! '
        if exc.output is not None and exc.output.strip()!='' :
            errmsg+= f'\noutput:\n{exc.output.decode()}'
        if exc.stdout is not None and exc.stdout.strip()!=''  :
            errmsg+= f'\nstdout:\n{exc.stdout.decode()}'
        if exc.stderr is not None and exc.stderr.strip()!='' :
            errmsg+= f'\nstderr:\n{exc.stderr.decode()}'
        if logger is not None :
            logger.error(errmsg,exc_info=exc)
        else :
            SERVICE_CONST.logger.error(errmsg,exc_info=exc)
        return None

def set_env_var(var_name,var_val) :
    """
    set an environment variable given its name and value
    """
    if get_os_name()=='Windows' :
        pwrsh_cmd = f'[Environment]::SetEnvironmentVariable("{var_name}","{var_val}"'
        pwrsh_cmd+= ',[EnvironmentVariableTarget]::Machine)'
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
        SERVICE_CONST.logger.info(msg)
        sys.exit(0)
    SERVICE_CONST.logger.debug('Testing code to check for errors...')
    unittest_dir_path = pathlib.Path(__file__).parent.parent.parent / 'test' / 'unittests'
    SERVICE_CONST.logger.debug(f'Running all unittests in {unittest_dir_path}...')
    run_cmd_in_subprocess([f'{sys.executable}','-m','unittest','discover','-s',f'{unittest_dir_path}','-vf'])
    SERVICE_CONST.logger.debug('All unittest checks complete : )')
