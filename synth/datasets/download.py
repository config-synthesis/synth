"""Download Synth datasets."""


# Imports.
from functools import partial
from pathlib import Path
from shutil import make_archive, rmtree, unpack_archive
from time import strftime
from typing import Optional

import pandas
from pandas import DataFrame, RangeIndex
from sh import ErrorReturnCode, git

from synth.datasets import (
    ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_DATA_DIR,
    ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA,
    LAMBDA_REPOS_PYTHON_DATA_DIR,
    LAMBDA_REPOS_PYTHON_METADATA,
)
from synth.logging import logger
from synth.paths import DATASET_DIR, WORKING_DIR


def _shallow_clone(url: str, branch: str, dest: Path, try_main: bool = True):
    """Perform a git shallow clone.

    Parameters
    ----------
    url : str
        Git remote url to clone.
    branch : str
        Git branch to clone.
    dest : Path
        Destination path to clone to.
    try_main : bool
        If true, try cloning the branch ``main`` if the original clone failed
        and the branch was ``master``.

    Raises
    ------
    ErrorReturnCode
        Raised on git error.
    """
    try:
        git.clone(
            '--depth=1',
            '--single-branch',
            f'--branch={branch}',
            url,
            dest,
        )
    except ErrorReturnCode as e:
        if (branch == 'master'
                and 'fatal: Remote branch master not found' in str(e.stderr)
                and try_main):
            logger.verbose('Failed. Attempting to clone branch `main`.')
            _shallow_clone(url, 'main', dest)
        else:
            raise


def _clone_github_repos(repos: DataFrame,
                        dest: Path,
                        clean: bool,
                        n: Optional[int] = None,
                        of: Optional[int] = None,
                        ):
    """Clone GitHub repos to an output destination.

    Parameters
    ----------
    repos : DataFrame
        A DataFrame listing GitHub repos. The DataFrame must have the columns
        repo_name, ref, path, symlink_target. Path and symlink_target are
        references to an existing file of interest within the repo.
    dest : Path
        Path to a destination directory where the repo will be cloned.
    clean : bool
        If true, existing repos will be recloned. Otherwise the existing repo
        will be added to the index without modification.
    n : Optional[int]
        Server number ``(1..of)``. If specified, this number will be used to
        clone only repos for this specific server based on a round-robin
        approach.
    of : Optional[int]
        Total number of servers. Must be specified if ``n`` is provided.
    """
    if (n is not None and of is None) or (n is None and of is not None):
        raise ValueError(
            'Either both `n` and `of` must be provided or neither of them '
            'must be provided.'
        )

    logger.verbose(f'Cloning repos to `{dest}`.')

    # If a number of servers is provided, reindex repos to select the correct
    # subset. We do this by using a range index that starts at n-1 (index 0
    # for the first server), ends after all repos, and steps by the number of
    # servers.
    #
    # This must come before selecting unique repos to clone, since the set of
    # repos to download should only be those in the list of repos for this
    # server.
    if n and of:
        logger.verbose(f'Reindexing repos to select for server `{n}`.')
        repos = repos.reindex(RangeIndex(
            start=n - 1,
            stop=len(repos),
            step=of,
        ))

    # Get all unique repos to clone. Repos may be duplicated because the same
    # repo can have multiple Dockerfiles.
    unique_repos: DataFrame = (
        repos
        .loc[:, ['repo_name', 'ref']]
        .sort_values(['repo_name', 'ref'])
        .drop_duplicates()
    )

    # Crate the dataset directory if it does not already exist.
    dest.mkdir(exist_ok=True, parents=True)

    # Clone each repo at the listed ref.
    index_rows = []
    num_unique_repos = len(unique_repos)
    for idx, (_, row) in enumerate(unique_repos.iterrows()):
        repo_name = row['repo_name']
        ref = row['ref']
        url = f'https://github.com/{repo_name}'
        logger.verbose(
            f'({idx + 1}/{num_unique_repos}) '
            f'Cloning repo `{repo_name}` from `{url}`.'
        )

        *_, branch = ref.rsplit('/', maxsplit=1)
        repo_dir = dest / repo_name.replace('/', r'__')

        # If the repo directory already exists and we are doing a clean clone,
        # remove the directory.
        if repo_dir.exists() and clean:
            logger.verbose(
                f'Directory `{repo_dir}` already exists. Removing.'
            )
            rmtree(repo_dir)

        # If the repo directory does not exist, either because we cleaned it
        # or because it wasn't there to begin with, do a shallow clone. If it
        # does exist, that means that it was previously cloned and we're not
        # cleaning. In that case log but do not clone.
        if not repo_dir.exists():
            try:
                logger.verbose(
                    f'Cloning branch `{branch}` of repo `{repo_name}` '
                    f'into {repo_dir.name}.'
                )
                _shallow_clone(url, branch, repo_dir)
            except ErrorReturnCode:
                logger.exception(f'Could not clone `{repo_name}`.')
                continue
        else:
            logger.verbose(
                f'Directory `{repo_dir}` already exists. '
                f'Adding to index without cloning.'
            )

        # Add the repo to the index.
        sha = git('rev-parse', 'HEAD', _cwd=repo_dir).strip()
        repo_dir_name = str(repo_dir.name)
        index_rows.append(
            (repo_name, ref, url, branch, sha, repo_dir_name,)
        )

    # Write the dataset index.
    index = DataFrame(
        index_rows,
        columns=('repo_name', 'ref', 'url', 'branch', 'sha', 'repo_dir'),
    )
    index = index.merge(repos, on=['repo_name', 'ref'])
    index.to_csv(dest / 'index.csv', index=False)


def download_datasets(metadata: dict[str, Path],
                      dest: Path,
                      clean: bool = False,
                      n: Optional[int] = None,
                      of: Optional[int] = None,):
    """Download all datasets in a metadata directory.

    Parameters
    ----------
    metadata : dict[str, Path]
        Metadata mapping the dataset name to its data file path.
    dest : Path
        Path to the directory where datasets should be downloaded.
    clean : bool
        If true, existing repos will be re-cloned. Otherwise the existing repo
        will be added to the index without modification.
    n : Optional[int]
        Server number ``(1..of)``. If specified, this number will be used to
        clone only repos for this specific server based on a round-robin
        approach.
    of : Optional[int]
        Total number of servers. Must be specified if ``n`` is provided.
    """
    # Process each set of repos.
    for name, path in metadata.items():
        if name.startswith('curated'):
            continue
        logger.info(f'Downloading dataset `{name}`.')

        # Read metadata and get unique repo/ref pairs.
        df = pandas.read_csv(path)
        dataset_dir = dest / name
        _clone_github_repos(df, dataset_dir, clean, n=n, of=of)


download_ansible_simple_debian_playbooks = partial(
    download_datasets,
    ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_METADATA,
    ANSIBLE_SIMPLE_DEBIAN_PLAYBOOKS_DATA_DIR,
)

download_docker_simple_debian_dockerfiles = partial(
    download_datasets,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR,
)

download_debian_ubuntu_dockerfiles = partial(
    download_datasets,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR,
)

download_lambda_repos_python = partial(
    download_datasets,
    LAMBDA_REPOS_PYTHON_METADATA,
    LAMBDA_REPOS_PYTHON_DATA_DIR,
)


def download_all(clean: bool = False,
                 n: Optional[int] = None,
                 of: Optional[int] = None):
    """Download all datasets.

    Datasets will be placed in ``synth.paths.DATASET_DIR``.

    Parameters
    ----------
    clean : bool
        If true, existing repos will be re-cloned. Otherwise the existing repo
        will be added to the index without modification.
    n : Optional[int]
        Server number ``(1..of)``. If specified, this number will be used to
        clone only repos for this specific server based on a round-robin
        approach.
    of : Optional[int]
        Total number of servers. Must be specified if ``n`` is provided.
    """
    # download_ansible_simple_debian_playbooks(clean=clean, n=n, of=of)
    # download_docker_simple_debian_dockerfiles(clean=clean, n=n, of=of)
    download_debian_ubuntu_dockerfiles(clean=clean, n=n, of=of)
    # download_lambda_repos_python(clean=clean, n=n, of=of)

    # Archive.
    # archive_path = DATASET_DIR.with_name(
    #     f'{DATASET_DIR.name}-{strftime("%Y-%m-%dT%H:%M:%S")}'
    # )
    # logger.info(
    #     f'Archiving downloaded datasets to '
    #     f'`{archive_path.with_suffix(".zip")}`.'
    # )
    # make_archive(
    #     base_name=str(archive_path),
    #     root_dir=str(DATASET_DIR.parent),
    #     base_dir=DATASET_DIR.name,
    #     format='zip',
    # )


def unpack(archive: Path):
    """Unpack a dataset archive.

    Parameters
    ----------
    archive : Path
        Path to the dataset archive to unpack.
    """
    logger.info(
        f'Unarchiving the dataset archive at `{archive}`.'
    )
    unpack_archive(
        filename=str(archive),
        extract_dir=str(WORKING_DIR),
    )
