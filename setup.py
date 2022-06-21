#imports
import setuptools, site

site.ENABLE_USER_SITE = True #https://www.scivision.dev/python-pip-devel-user-install/

setupkwargs = dict(
    name='openmsistream',
    packages=setuptools.find_packages(include=['openmsistream*']),
    include_package_data=True,
    version='0.9.1.0',
    description='''Applications for laboratory, analysis, and computational materials data streaming using Apache Kafka.
                   Developed for Open MSI (NSF DMREF award #1921959)''',
    author='Maggie Eminizer, Amir Sharifzadeh, Sam Tabrisky, Alakarthika Ulaganathan, David Elbert',
    author_email='margaret.eminizer@gmail.com',
    url='https://github.com/openmsi/openmsistream',
    download_url='https://github.com/openmsi/openmsistream/archive/refs/tags/v0.9.1.0.tar.gz',
    keywords=['data_streaming','stream_processing','apache_kafka','materials','data_science'],
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
