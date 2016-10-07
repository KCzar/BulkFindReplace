# -*- coding: utf-8 -*-
"""
Created on Thu Nov 06 12:39:14 2014

@author: Ken
"""

from distutils.core import setup
from Cython.Build import cythonize
import numpy

setup(
    ext_modules = cythonize("BulkFindReplaceCy4.pyx"),
    include_dirs=[numpy.get_include()]
)
