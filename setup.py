#imports
import setuptools, site

site.ENABLE_USER_SITE = True #https://www.scivision.dev/python-pip-devel-user-install/

setupkwargs = dict(
    name='openmsistream',
    version='0.9.1.0',
    packages=setuptools.find_packages(include=['openmsistream*']),
    include_package_data=True,
    entry_points = {
        'console_scripts' : ['UploadDataFile=openmsistream.data_file_io.upload_data_file:main',
                             'DataFileUploadDirectory=openmsistream.data_file_io.data_file_upload_directory:main',
                             'DataFileDownloadDirectory=openmsistream.data_file_io.data_file_download_directory:main',
                             'InstallService=openmsistream.services.install_service:main',
                             'ManageService=openmsistream.services.manage_service:main',
                             'ProvisionNode=openmsistream.utilities.provision_wrapper:main',
                             'OSNStreamProcessor=openmsistream.osn.osn_stream_processor:main',
                            ],
    },
    python_requires='>=3.7,<3.10',
    install_requires=['atomicwrites>=1.4.0',
                      'boto3>=1.23.0',
                      'confluent-kafka>=1.8.2',
                      'kafkacrypto>=0.9.9.15',
                      'methodtools',
                      'msgpack',
                     ],
    extras_require = {'test': ['beautifulsoup4',
                               'gitpython',
                               'lxml',
                               'marko[toc]',
                               'pyflakes>=2.2.0',
                               ],
                        },
)

setupkwargs["extras_require"]["all"] = sum(setupkwargs["extras_require"].values(), [])

setuptools.setup(**setupkwargs)
