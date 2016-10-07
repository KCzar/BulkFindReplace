# -*- coding: utf-8 -*-
"""
Created on Thu Nov 06 12:39:14 2014

@author: Ken
"""

from distutils.core import setup
# from distutils.extension import Extension
from Cython.Build import cythonize
import numpy

setup(
    name = 'BulkFindReplace',
    packages = ['BulkFindReplace'], # this must be the same as the name above
    version = '1.0.1',
    description = 'Fast Find-Replace for large amounts of data using a trie method',
    author = 'Ken Larrey',
    author_email = 'klarrey@gmail.com',
    url = 'https://github.com/KCzar/BulkFindReplace', # use the URL to the github repo
    download_url = 'https://github.com/KCzar/BulkFindReplace/tarball/1.0.1', # I'll explain this in a second
    keywords = ['find', 'replace', 'trie'], # arbitrary keywords
    classifiers = [],

    ext_modules = cythonize("BulkFindReplace/BulkFindReplaceCy4.pyx"),
    include_dirs=[numpy.get_include()]
)

