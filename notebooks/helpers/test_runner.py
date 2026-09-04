"""
Lightweight test runner for the lab notebooks.

Changed from the old capstone repo:
  * The notebook name is passed in explicitly instead of being scraped from
    `dbutils.notebook.entry_point...`, which is not available on serverless
    compute and silently produced "Could not detect notebook name".
  * No pytest dependency. The old notebooks ran `%pip install pytest==8.4.2`
    but the tests are plain functions with asserts, so pytest was never used.
  * Failures print the assertion message only, not a full traceback, so a
    student sees "Expected 4 columns, found 3" instead of 20 lines of noise.
"""

import importlib
import inspect
import os
import re
import traceback
from typing import Optional

from helpers.tests import *


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    ORANGE = "\033[93m"
    RESET = "\033[0m"


def _module_name_for(notebook: str) -> str:
    base = re.sub(r"^[A-Za-z]*\d+-", "", notebook)   # drop "B01-" / "S03-" / "02-"
    base = os.path.splitext(base)[0]           # drop ".ipynb" if present
    base = base.replace("-", "_")
    return f"helpers.tests.{base}_tests"


def run(notebook: Optional[str] = None, verbose: bool = False, **kwargs) -> dict:
    """
    Run every `test_*` function in the test module that matches this notebook.

    Args:
        notebook: notebook file name, e.g. "B01-batch-ingestion-and-delta".
                  Falls back to the NOTEBOOK_NAME environment variable.
        verbose:  print a full traceback for failures instead of just the message.
        **kwargs: forwarded to every test function.

    Returns:
        {"passed": int, "failed": int, "failures": [(name, message), ...]}
    """
    notebook = notebook or os.getenv("NOTEBOOK_NAME", "")
    if not notebook:
        print(f"{Colors.RED}ERROR{Colors.RESET}: pass the notebook name, e.g. "
              f'test_runner.run("B01-batch-ingestion-and-delta")')
        return {"passed": 0, "failed": 0, "failures": []}

    module_name = _module_name_for(notebook)
    try:
        test_module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        print(f"{Colors.RED}ERROR{Colors.RESET}: no test module named {module_name}")
        return {"passed": 0, "failed": 0, "failures": []}

    test_functions = sorted(
        (name, func)
        for name, func in inspect.getmembers(test_module, inspect.isfunction)
        if name.startswith("test_")
    )
    if not test_functions:
        print(f"{Colors.RED}ERROR{Colors.RESET}: {module_name} contains no test_* functions")
        return {"passed": 0, "failed": 0, "failures": []}

    print(f"Running {len(test_functions)} checks for {Colors.ORANGE}{notebook}{Colors.RESET}\n")

    results = {"passed": 0, "failed": 0, "failures": []}
    for name, func in test_functions:
        try:
            func(**kwargs)
            print(f"  [{Colors.GREEN}PASS{Colors.RESET}] {name}")
            results["passed"] += 1
        except AssertionError as exc:
            print(f"  [{Colors.RED}FAIL{Colors.RESET}] {name}")
            print(f"         {exc}")
            results["failed"] += 1
            results["failures"].append((name, str(exc)))
            if verbose:
                print(traceback.format_exc())
        except Exception as exc:  # a test blew up rather than failed
            print(f"  [{Colors.RED}ERROR{Colors.RESET}] {name}")
            print(f"         {type(exc).__name__}: {exc}")
            results["failed"] += 1
            results["failures"].append((name, f"{type(exc).__name__}: {exc}"))
            if verbose:
                print(traceback.format_exc())

    total = results["passed"] + results["failed"]
    mark = Colors.GREEN + "ALL CHECKS PASSED" + Colors.RESET if results["failed"] == 0 \
        else Colors.RED + f"{results['failed']} CHECK(S) FAILED" + Colors.RESET
    print(f"\n{results['passed']}/{total} passed — {mark}")
    return results
