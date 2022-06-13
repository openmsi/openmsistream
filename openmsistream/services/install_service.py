#imports
import sys
from .config import SERVICE_CONST
from .utilities import test_python_code, get_os_name
from .service_manager_base import ServiceManagerBase
from .windows_service_manager import WindowsServiceManager
from .linux_service_manager import LinuxServiceManager

def main() :
    #get the arguments
    parser = ServiceManagerBase.get_argument_parser('install')
    args = parser.parse_args()
    #run the tests if requested
    if args.service_class_name=='test' :
        test_python_code()
    else :
        if args.service_name is None :
            #default name of the Service is just the class name
            service_name = args.service_class_name
        else :
            service_name = args.service_name
        #make the list of arguments that should be sent to the run function in the executable (removing "service_name")
        argslist = sys.argv[2:]
        if '--service_name' in argslist :
            index = argslist.index('--service_name')
            argslist.pop(index)
            argslist.pop(index)
        #get the name of the OS and start the object
        operating_system = get_os_name()
        manager_args = [service_name]
        manager_kwargs = {'service_class_name':args.service_class_name,
                          'argslist':argslist,
                          'interactive':True,
                          'logger':SERVICE_CONST.LOGGER}
        managers_by_os_name = {'Windows':WindowsServiceManager,
                               'Linux':LinuxServiceManager,}
        manager_class = managers_by_os_name[operating_system]
        manager = manager_class(*manager_args,**manager_kwargs)
        #install the service
        manager.install_service()

if __name__=='__main__' :
    main()