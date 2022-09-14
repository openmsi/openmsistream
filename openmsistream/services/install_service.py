"""Script to use the OS-appropriate ServceManager to install code as a Windows Service or Linux daemon"""

#imports
import sys
from .config import SERVICE_CONST
from .utilities import test_python_code, get_os_name
from .service_manager_base import ServiceManagerBase
from .windows_service_manager import WindowsServiceManager
from .linux_service_manager import LinuxServiceManager

def main(given_args=None) :
    """
    Install a Windows Service/Linux daemon (depending on the OS that's running)
    """
    #get the arguments
    if given_args is None :
        parser = ServiceManagerBase.get_argument_parser('install',sys.argv[1] if len(sys.argv)>1 else None)
        args, _ = parser.parse_known_args()
    else :
        parser = ServiceManagerBase.get_argument_parser('install',given_args[0] if len(given_args)>0 else None)
        args, _ = parser.parse_known_args(given_args)
    #run the tests if requested
    if args.service_spec_string=='test' :
        test_python_code()
    else :
        if args.service_name is None :
            #default name of the Service is just the class name
            service_name = args.service_spec_string
        else :
            service_name = args.service_name
        #make the list of arguments that should be sent to the run function in the executable (removing "service_name")
        argslist = sys.argv[2:] if given_args is None else given_args[1:]
        if '--service_name' in argslist :
            index = argslist.index('--service_name')
            argslist.pop(index)
            argslist.pop(index)
        #define arguments to and create the ServiceManager
        manager_args = [service_name]
        manager_kwargs = {'service_spec_string':args.service_spec_string,
                          'argslist':argslist,
                          'interactive':given_args is None,
                          'logger':SERVICE_CONST.logger}
        managers_by_os_name = {'Windows':WindowsServiceManager,
                               'Linux':LinuxServiceManager,}
        manager_class = managers_by_os_name[get_os_name()]
        manager = manager_class(*manager_args,**manager_kwargs)
        #install the service
        manager.install_service()

if __name__=='__main__' :
    main()
