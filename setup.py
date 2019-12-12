#!/usr/bin/env python
# -*-coding:utf-8-*-
"""
Author : Min
date   : 2019/12/12
"""
from setuptools import setup

import kingbase

setup(
    name="kingbase",
    version=kingbase.__version__,
    description="Python interface to Kingbase",
    author="Min Wang",
    author_email="min@minspace.cn",
    license="Apache License, Version 2.0",
    packages=['kingbase'],
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
        'psycopg2-binary': ['psycopg2-binary==2.8.4'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'kingbase = kingbase.kingbase_dialects:PGDialect_kingbase',
        ],
    }
)
