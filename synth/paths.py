"""Synth filesystem paths."""


# Imports.
from pathlib import Path

from synth import settings


ETC_SYNTH = Path('/etc/synth')

BASE_DIR = Path(__file__).parent.parent.absolute()

WORKING_DIR = BASE_DIR / settings.WORKING_DIRECTORY
DATASET_METADATA_DIR = BASE_DIR / 'data'

ANALYSIS_DIR = WORKING_DIR / 'analysis'
CACHE_DIR = WORKING_DIR / settings.CACHE_DIRECTORY
DOCKER_CACHE_DIR = CACHE_DIR / 'docker'
DATASET_DIR = WORKING_DIR / 'datasets'
EXPERIMENTS_OUTPUT_DIR = WORKING_DIR / 'experiments_output'
