"""
Copy the shared batch seed CSVs (products, users, sales) into the student's volume.

`sales` is the interesting one: a day-0 snapshot followed by daily deltas carrying
insert / update / delete markers, with a `region` column appearing partway through.
That is the CDC and schema-evolution material for the batch track.
"""

import fnmatch
from typing import Iterator, List

from helpers import utils

dbutils = utils.dbutils

DATASETS = ("products", "users", "sales")


def source_path(dataset: str) -> str:
    return f"/Volumes/{utils.SOURCE_CATALOG}/{utils.SOURCE_SCHEMA}/raw_files/{dataset}"


def destination_path(dataset: str) -> str:
    return f"{utils.get_configs()['volume_raw']}/{dataset}"


def _source_files(dataset: str, extension: str = ".csv") -> List[str]:
    files = [f.path for f in dbutils.fs.ls(source_path(dataset)) if not f.path.endswith("/")]
    files = [f for f in files if f.lower().endswith(extension)]
    files = [f for f in files if fnmatch.fnmatch(f.split("/")[-1], f"{dataset}*")]
    return sorted(files)


def copy_files(dataset: str, num_files: int = 0, start_from: int = 1) -> Iterator[str]:
    """
    Copy seed files in name order.

    Args:
        dataset: products | users | sales
        num_files: how many to copy; 0 means all remaining.
        start_from: 1-based index to start at, for staging a late-arriving file.
    """
    files = _source_files(dataset)[max(start_from - 1, 0):]
    if num_files > 0:
        files = files[:num_files]

    dest = destination_path(dataset)
    for file_path in files:
        name = file_path.split("/")[-1]
        dbutils.fs.cp(file_path, f"{dest}/{name}")
        yield name


def seed(dataset: str, num_files: int = 0, start_from: int = 1) -> int:
    print(f"{source_path(dataset)}  ->  {destination_path(dataset)}")
    copied = list(copy_files(dataset, num_files, start_from))
    for name in copied:
        print(f"  copied {name}")
    print(f"{len(copied)} file(s) copied for '{dataset}'.")
    return len(copied)


def seed_all() -> None:
    for dataset in DATASETS:
        seed(dataset)
        print()
