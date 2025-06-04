#!/usr/bin/env python

from setuptools import setup
from setuptools.command.test import test as TestCommand
import pyhive
import sys


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


with open('README.rst') as readme:
    long_description = readme.read()

test_deps = [
    'mock>=1.0.0',
    'pytest',
    'pytest-cov',
    'requests>=1.0.0',
    'sqlalchemy>=1.3.0,<=1.4.46',
    'thrift-iomete==0.22.0',
]

setup(
    name="py-hive-iomete",
    version=pyhive.__version__,
    description="Python interface to iomete (Hive)",
    long_description=long_description,
    url='https://github.com/iomete/py-hive-iomete',
    author="Vusal Dadalov",
    author_email="vusal@iomete.com",
    license="Apache License, Version 2.0",
    packages=['pyhive', 'TCLIService'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        'future',
        'python-dateutil',
        'thrift-iomete==0.22.0'
    ],
    extras_require={
        'sqlalchemy': ['sqlalchemy>=1.3.0,<=1.4.46'],
        'test': test_deps,
        'presto': ['requests>=1.0.0'],
        'trino': ['requests>=1.0.0'],
        'hive': ['sasl>=0.2.1', 'thrift>=0.10.0', 'thrift_sasl>=0.1.0'],
        'kerberos': ['requests_kerberos>=0.12.0'],
    },
    tests_require=test_deps,
    cmdclass={'test': PyTest},
    package_data={
        '': ['*.rst'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            "hive = pyhive.sqlalchemy_hive:HiveDialect",
            "iomete = pyhive.sqlalchemy_iomete:IometeHttpsDialect",
            "iomete.http = pyhive.sqlalchemy_iomete:IometeHttpDialect",
            "iomete.https = pyhive.sqlalchemy_iomete:IometeHttpsDialect",
            "hive.http = pyhive.sqlalchemy_hive:HiveHTTPDialect",
            "hive.https = pyhive.sqlalchemy_hive:HiveHTTPSDialect",
            'presto = pyhive.sqlalchemy_presto:PrestoDialect',
            'trino.pyhive = pyhive.sqlalchemy_trino:TrinoDialect'
        ],
    }
)
