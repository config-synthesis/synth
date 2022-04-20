"""Synth setuptools configuration."""


# Imports.
from pathlib import Path

from setuptools import setup


# Paths.
BASE_DIR = Path(__file__).parent


# Setup.
setup(
    name='synth',
    version='0.0.1',
    description='Synth brings harmony to configurations.',
    long_description=(BASE_DIR / 'README.md').read_text(encoding='utf-8'),
    long_description_content_type='text/markdown',
    packages=[
        'synth',
    ],
    scripts=[
        'bin/synth',
    ],
    install_requires=[
        'ansible',
        'aws-sam-cli',
        'aws-sam-translator',
        'boto3',
        'coloredlogs',
        'docker[tls]',
        'pandas',
        'pymysql',
        'PyYAML',
        'more-itertools',
        'networkx',
        'numpy',
        'sh',
        'sqlalchemy',
    ],
)
