#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys

from setuptools import setup, Extension

try:
    import numpy as np
except ImportError:
    sys.stderr.write('NumPy must be installed!\n')
    exit(1)
else:
    _NPY_DIRS = [np.get_include()]

try:
    from Cython.Build import cythonize
except ImportError:
    _HAS_CYTHON = False
else:
    _HAS_CYTHON = True

ROOT = os.path.dirname(__file__)

if _HAS_CYTHON:
    extensions = cythonize([Extension('pyswiss', sources=[os.path.join(ROOT, 'pyswiss', 'pyswiss.pyx')], include_dirs=_NPY_DIRS)])
else:
    extensions = [Extension('pyswiss', sources=[os.path.join(ROOT, 'pyswiss', 'pyswiss.c')], include_dirs=[np.get_include()])]


def get_version():
    with open(os.path.join(ROOT, 'pyswiss', 'pyswiss.pyx'), 'rt') as fh:
        for line in fh:
            m = re.match("__version__ = '([^']+)", line)
            if m:
                return m.group(1)
        else:
            return None


setup(
    name='pyswiss',
    version=get_version(),
    description='pyswiss is a module for reading files in the SWISS-PROT format',
    long_description='',
    ext_modules=extensions,
    zip_safe=False,
    install_requires=[
        'numpy >= 1.9.0',
        'h5py >= 2.6.0',
    ]
)
