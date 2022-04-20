"""Synth settings."""


# Imports.
from pathlib import Path

import yaml


# Paths.
_LOCAL_SETTINGS = Path(__file__).parent.parent / 'config.yml'
_USER_SETTINGS = Path('~/.synth/config.yml')


# Get settings.
_settings = {
    'working_directory': 'ignored',
    'cache_directory': 'cache',
    'mysql_username': 'root',
    'mysql_password': '',
}
if _LOCAL_SETTINGS.is_file():
    with _LOCAL_SETTINGS.open() as fd:
        _settings |= yaml.safe_load(fd) or {}
elif _USER_SETTINGS.is_file():
    with _USER_SETTINGS.open() as fd:
        _settings |= yaml.safe_load(fd) or {}


# Set settings.
WORKING_DIRECTORY = _settings['working_directory']
CACHE_DIRECTORY = _settings['cache_directory']
MYSQL_USERNAME = _settings['mysql_username']
MYSQL_PASSWORD = _settings['mysql_password']
