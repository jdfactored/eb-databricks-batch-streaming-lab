"""
Register every test module so `test_runner` can import it by name.

Notebook `B01-batch-ingestion-and-delta.ipynb` -> `batch_ingestion_and_delta_tests.py`.
"""

from . import batch_ingestion_and_delta_tests
from . import data_quality_and_governance_tests
from . import performance_tuning_tests
from . import advanced_etl_and_orchestration_tests
from . import streaming_fundamentals_tests
from . import ingesting_streams_autoloader_tests
from . import stateful_streaming_tests
from . import monitoring_and_governance_tests

__all__ = [
    "batch_ingestion_and_delta_tests",
    "data_quality_and_governance_tests",
    "performance_tuning_tests",
    "advanced_etl_and_orchestration_tests",
    "streaming_fundamentals_tests",
    "ingesting_streams_autoloader_tests",
    "stateful_streaming_tests",
    "monitoring_and_governance_tests",
]
