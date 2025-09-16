#!/usr/bin/env python3

from setuptools import find_packages
from setuptools import setup

setup(
    name='workano-reports-plugin',
    version='1.0',
    description='workano otp request plugin',
    author='workano team',
    author_email='info@workano.com',
    packages=find_packages(),
    url='https://workano.com',
    include_package_data=True,
    package_data={
        'workano_reports_plugin': ['api.yml'],
    },

    entry_points={
        'wazo_call_logd.plugins': [
            'workano_reports_plugin = workano_reports_plugin.plugin:Plugin'
        ],
        'console_scripts': [
            'workano-queue-log=workano_reports_plugin.queue_log.main:main',
        ]
    }
)
