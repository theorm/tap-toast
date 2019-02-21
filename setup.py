#!/usr/bin/env python

from setuptools import setup

setup(name='tap-toast',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Toast API',
      author='@lambtron',
      url='https://andyjiang.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_toast'],
      install_requires=[
          'singer-python==5.4.1',
          'requests==2.20.0',
          'backoff==1.3.2'
      ],
      entry_points='''
          [console_scripts]
          tap-toast=tap_toast:main
      ''',
      packages=['tap_toast'],
      include_package_data=True,
)
