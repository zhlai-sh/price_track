'''Dataflow preprocessing pacakge configuration'''

from setuptools import setup, find_packages
REQUIRED_PACKAGES = ['PyYAML==5.4',
                     'avro-python3==1.9.1',
                     'apache-beam[gcp]==2.12.0'
                     ]

setup(name='scratch_data_puller',
      version='1.0.0',
      python_requires='>2.7',
      packages=find_packages(exclude=('tests','tests.*')),
      include_package_data=True,
      description='Step: Data Puller',
      install_requires=REQUIRED_PACKAGES,
      zip_safe=False
      )