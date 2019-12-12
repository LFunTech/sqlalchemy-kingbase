#!/usr/bin/env python
# -*-coding:utf-8-*-
"""
Author : Min
date   : 2019/12/12
"""
from setuptools import setup

import kingbase

setup(
    name="PyHive",
    version=kingbase.__version__,
    description="Python interface to Kingbase",
    url='https://github.com/dropbox/PyHive',
    author="Jing Wang",
    author_email="jing@dropbox.com",
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
    ],
    extras_require={
        'sqlalchemy': ['sqlalchemy>=0.8.7'],
        'kerberos': ['requests_kerberos>=0.12.0'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'hive = pyhive.sqlalchemy_hive:HiveDialect',
            'presto = pyhive.sqlalchemy_presto:PrestoDialect',
        ],
    }
)
