# -*- coding: utf-8 -*-
from os.path import join, dirname
from setuptools import setup, find_packages

VERSION = (6, 1, 0)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

f = open(join(dirname(__file__), 'README'))
long_description = f.read().strip()
f.close()

install_requires = [
    'aiohttp',
    'async_timeout',
    'elasticsearch>=6.0.0',
]

tests_require = [
    'pytest',
    'pytest-asyncio',
    'pytest-cov',
]

setup(
    name='elasticsearch-async',
    description="Async backend for elasticsearch-py",
    license="Apache License, Version 2.0",
    url="https://github.com/elastic/elasticsearch-py-async",
    long_description=long_description,
    version=__versionstr__,
    author="Honza Král",
    author_email="honza.kral@gmail.com",
    packages=find_packages(
        where='.',
        exclude=('test_elasticsearch_async*', )
    ),
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    install_requires=install_requires,
    python_requires="~=3.4",

    test_suite="test_elasticsearch_async.run_tests.run_all",
    tests_require=tests_require,

    extras_require={'develop': tests_require},
)
