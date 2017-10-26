#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from mundone import __version__


setup(
    name='mundone',
    version=__version__,
    description='Mundane task management',
    long_description='',
    packages=find_packages(exclude=['ipu']),
    zip_safe=False
)
