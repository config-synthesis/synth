"""Synth datasets."""


# Imports.
import operator
from functools import reduce

from synth.paths import DATASET_DIR, DATASET_METADATA_DIR


# Paths
ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_DATA_DIR = (
    DATASET_DIR / 'ansible/simple_debian_playbooks'
)
ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_DATA_DIR.mkdir(exist_ok=True, parents=True)
ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA_DIR = (
    DATASET_METADATA_DIR / 'ansible/simple_debian_playbooks'
)


DOCKER_CURATED_DOCKERFILES_DATA_DIR = DATASET_DIR / 'docker/curated'
DOCKER_CURATED_DOCKERFILES_DATA_DIR.mkdir(exist_ok=True, parents=True)
DOCKER_CURATED_DOCKERFILES_METADATA_DIR = (
    DATASET_METADATA_DIR / 'docker/curated'
)
DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR = (
    DATASET_DIR / 'docker/simple_debian_dockerfiles'
)
DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR.mkdir(exist_ok=True, parents=True)
DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA_DIR = (
    DATASET_METADATA_DIR / 'docker/simple_debian_dockerfiles'
)

DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR = (
    DATASET_DIR / 'docker/debian_ubuntu_dockerfiles'
)
DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR.mkdir(exist_ok=True, parents=True)
DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA_DIR = (
    DATASET_METADATA_DIR / 'docker/debian_ubuntu_dockerfiles'
)

LAMBDA_REPOS_PYTHON_DATA_DIR = DATASET_DIR / 'lambda_repos/python'
LAMBDA_REPOS_PYTHON_DATA_DIR.mkdir(exist_ok=True, parents=True)
LAMBDA_REPOS_PYTHON_METADATA_DIR = DATASET_METADATA_DIR / 'lambda_repos/python'


ANSIBLE_CURATED_ANALYSIS_SCRIPTS_DATA_DIR = DATASET_DIR / 'ansible/curated'
ANSIBLE_CURATED_ANALYSIS_SCRIPTS_DATA_DIR.mkdir(exist_ok=True, parents=True)
ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR = (
    DATASET_METADATA_DIR / 'ansible/curated'
)
SHELL_CURATED_ANALYSIS_SCRIPTS_DATA_DIR = DATASET_DIR / 'shell/curated'
SHELL_CURATED_ANALYSIS_SCRIPTS_DATA_DIR.mkdir(exist_ok=True, parents=True)
SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR = (
    DATASET_METADATA_DIR / 'shell/curated'
)

# Datasets.
ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA = {
    'debian-playbooks_2021-11-18': (
        ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA_DIR
        / 'debian-playbooks_2021-11-18.csv'
    ),
}
DOCKER_CURATED_DOCKERFILES_METADATA = {
    'curated_debian_dockerfiles': (
        DOCKER_CURATED_DOCKERFILES_METADATA_DIR
        / 'curated_debian_dockerfiles.csv'
    ),
}
DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA = {
    'debian-dockerfiles_2021-11-18': (
        DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA_DIR
        / 'debian-dockerfiles_2021-11-18.csv'
    ),
}
DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA = {
    'sampled-deduplicated-debian-ubuntu-dockerfiles_2022-03-01': (
        DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA_DIR
        / 'sampled-deduplicated-debian-ubuntu-dockerfiles_2022-03-01.csv'
    ),
}
LAMBDA_REPOS_PYTHON_METADATA = {
    'python-lambda-repos_2021-03-05': (
        LAMBDA_REPOS_PYTHON_METADATA_DIR / 'python-lambda-repos_2021-03-05.csv'
    ),
}
ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA = {
    'curated_analysis_playbooks': (
        ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR
        / 'curated_analysis_playbooks.csv'
    ),
}
SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA = {
    'curated_analysis_scripts': (
        SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR
        / 'curated_analysis_scripts.csv'
    ),
}
ALL_DATASET_METADATA = reduce(
    operator.or_,
    [
        ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA,
        DOCKER_CURATED_DOCKERFILES_METADATA,
        DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA,
        DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
        LAMBDA_REPOS_PYTHON_METADATA,
        ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA,
        SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA,
    ],
    {},
)
