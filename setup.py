#!/usr/bin/env python
from os.path import expanduser

from setuptools import setup, find_packages, Extension
from shutil import copy


install_requires = [
    'Cython',
    'lithops'
]

# Load example storage data
home_path = expanduser("~")
copy("storage_data/pred_ibm_cos_64.pickle", "%s/.lithops/pred_ibm_cos_64.pickle"%(home_path))

setup(
    name='seercloud',
    version=0.1,
    url='https://github.com/GEizaguirre/seercloud',
    author='German T. Eizaguirre',
    description='Serverless shuffle utility',
    author_email='gteizaguirre@gmail.com',
    packages=find_packages(exclude=("fig", "seer", "src")),
    install_requires=install_requires,
    # extras_require=extras_require,
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
    ],
    python_requires='>=3.6',
    ext_modules = [Extension("seercloud.src.chash", ["seercloud/src/chash.c"])]
)
