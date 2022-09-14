"""Code for installing OpenMSIStream programs (or others) as Windows Services/Linux daemons"""

from .windows_service_manager import WindowsServiceManager
from .linux_service_manager import LinuxServiceManager

__all__ = [
    'WindowsServiceManager',
    'LinuxServiceManager',
]
