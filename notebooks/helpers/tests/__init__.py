"""
Register every test module so `test_runner` can import it by name.

Notebook `B01-batch-ingestion-and-delta.ipynb` -> `batch_ingestion_and_delta_tests.py`.
"""

"""
Register every test module so `test_runner` can import it by name.

Notebook `B01-batch-ingestion-and-delta.ipynb` -> `batch_ingestion_and_delta_tests.py`.
"""

from . import batch_ingestion_and_delta_tests
from . import streaming_autoloader_tests

__all__ = [
    "batch_ingestion_and_delta_tests",
    "streaming_autoloader_tests",
]
