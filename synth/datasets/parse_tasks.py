"""Parse all tasks from a dataset."""


# Imports.
from pathlib import Path

import pandas
from pandas import DataFrame

from synth.datasets import ALL_DATASET_METADATA
from synth.paths import DATASET_DIR
from synth.synthesis.configuration_scripts import get_parser


def parse_tasks(dataset: str, use_index: bool = False) -> DataFrame:
    """Parse all configuration tasks from a Synth dataset.

    Synth loads tasks from the dataset training set by default. Tasks may
    optionally be loaded from the full dataset index. Tasks are returned as an
    ordered DataFrame of executable, arguments, and count.

    Parameters
    ----------
    dataset : str
        Dataset name.
    use_index : bool
        Use the full dataset index instead of the training set.

    Returns
    -------
    DataFrame
        All parsed configuration tasks.
    """
    # Get the correct dataset dir.
    metadata_path: Path = ALL_DATASET_METADATA[dataset]
    metadata_path = metadata_path.relative_to(
        metadata_path.parent.parent.parent
    )
    metadata_dir = metadata_path.parent / metadata_path.stem
    dataset_dir: Path = DATASET_DIR / metadata_dir

    # Get the correct index path.
    if use_index:
        index_path = dataset_dir / 'index.csv'
    else:
        index_path = dataset_dir / 'training_set.csv'

        if not index_path.is_file():
            raise Exception(
                'Dataset training_set.csv index does not exist. Make sure you '
                'have run dataset preparation, or parse tasks from the index.'
            )

    # Load the dataset.
    dataset: DataFrame = pandas.read_csv(index_path)

    # Parse all tasks.
    tasks = []
    for _, row in dataset.iterrows():
        script_path: Path = dataset_dir / row['repo_dir'] / row['path']
        if not script_path.exists():
            continue

        parser = get_parser(script_path)
        parse_result = parser(script_path)

        for task in parse_result.tasks:
            tasks.append((task.executable, task.arguments))

    # Create and return the aggregated dataframe.
    tasks_df: DataFrame = DataFrame(tasks, columns=['executable', 'arguments'])
    tasks_df['count'] = 1
    return (
        tasks_df
        .groupby(['executable', 'arguments'], as_index=False)
        .count()
        .sort_values(by=['executable', 'arguments'])
    )
