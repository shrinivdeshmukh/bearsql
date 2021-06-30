#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ 'coverage==5.5',
                'duckdb==0.2.7',
                'numpy==1.21.0',
                'pandas==1.2.5',
                'pyarrow==4.0.1',
                'python-dateutil==2.8.1',
                'pytz==2021.1',
                'six==1.16.0'
            ]

test_requirements = [ ]

setup(
    author="Shrinivas Vijay Deshmukh",
    author_email='shrinivas.deshmukh11@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Bearsql aadds sql syntax on pandas dataframe. It uses duckdb to speedup the pandas processing and as the sql engine",
    entry_points={
        'console_scripts': [
            'bearsql=bearsql.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='bearsql',
    name='bearsql',
    packages=find_packages(include=['bearsql', 'bearsql.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/shrinivdeshmukh/bearsql',
    version='0.1.0b2',
    zip_safe=False,
)
