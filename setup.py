#!/usr/bin/env python

from setuptools import setup

setup(name='target-s3csv',
      version='0.0.1',
      description='Singer.io target for writing CSV files to S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_s3csv'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python==3.2.1',
          'boto3'
      ],
      entry_points='''
          [console_scripts]
          target-s3csv=target_s3csv:main
      ''',
)
