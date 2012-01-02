#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='txfileio',
      version='0.0.0',
      description='Asynchronous file I/O for Twisted',
      author='Yaroslav Fedevych',
      author_email='jaroslaw.fedewicz@gmail.com',
      url='https://github.com/jafd/txfileio',
      py_modules=['txfileio'],
      install_requires=['distribute', 'twisted']
     )

