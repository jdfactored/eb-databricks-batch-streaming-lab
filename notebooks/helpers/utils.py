"""
Shared helpers for the Databricks Batch & Streaming lab.

Carried over from the Fundamentals lab, extended with a gold layer, checkpoint /
schema volumes for Auto Loader, and a few streaming inspection helpers.
"""

from typing import Dict, List

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

_w = WorkspaceClient()
dbutils = _w.dbutils
workspace = _w  # exposed for helpers that need files.upload()

# Read-only catalog holding the shared seed CSVs (same one the Fundamentals lab uses).
SOURCE_CATALOG = "capstone_src"
SOURCE_SCHEMA = "dev_bronze"

DEFAULT_CATALOG = "capstone_dev"

LAYERS = ("bronze", "silver", "gold")
VOLUMES = ("raw_files", "checkpoints", "schemas")


def get_param(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def get_catalog() -> str:
    return get_param("catalog", DEFAULT_CATALOG)


def get_base_user_schema() -> str:
    """jane.doe@factored.ai -> jane_doe"""
    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    return user_email.split("@")[0].replace(".", "_").replace("-", "_")


def get_configs(table_name: str = "") -> Dict[str, str]:
    catalog = get_catalog()
    base_user = get_base_user_schema()
    bronze = f"{base_user}_bronze"

    configs = {
        "catalog": catalog,
        "base_user": base_user,
        "schema_bronze": bronze,
        "schema_silver": f"{base_user}_silver",
        "schema_gold": f"{base_user}_gold",
        "volume_raw": f"/Volumes/{catalog}/{bronze}/raw_files",
        "volume_checkpoints": f"/Volumes/{catalog}/{bronze}/checkpoints",
        "volume_schemas": f"/Volumes/{catalog}/{bronze}/schemas",
    }

    if table_name:
        configs["table_bronze"] = f"{catalog}.{bronze}.{table_name}"
        configs["table_silver"] = f"{catalog}.{base_user}_silver.{table_name}"
        configs["table_gold"] = f"{catalog}.{base_user}_gold.{table_name}"
        configs["raw_path"] = f"{configs['volume_raw']}/{table_name}"
        configs["checkpoint_path"] = f"{configs['volume_checkpoints']}/{table_name}"
        configs["schema_path"] = f"{configs['volume_schemas']}/{table_name}"

    return configs


# --------------------------------------------------------------------------- files

def list_files(path: str) -> List[str]:
    return [f.name for f in dbutils.fs.ls(path) if not f.name.endswith("/")]


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def reset_path(path: str) -> None:
    """Delete a checkpoint / schema directory so a stream can start clean."""
    try:
        dbutils.fs.rm(path, True)
        print(f"Removed {path}")
    except Exception as exc:
        print(f"Nothing to remove at {path} ({exc})")


# --------------------------------------------------------------------------- tables

def table_exists(full_table_name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def get_table_schema(full_table_name: str) -> List[tuple]:
    df = spark.table(full_table_name)
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def get_column_names(full_table_name: str) -> List[str]:
    return [name for name, _ in get_table_schema(full_table_name)]


def get_table_row_count(full_table_name: str) -> int:
    return spark.table(full_table_name).count()


def get_table_property(full_table_name: str, key: str) -> str:
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect():
        if (row["col_name"] or "").strip() == key:
            return (row["data_type"] or "").strip()
    return ""


def get_table_properties(full_table_name: str) -> Dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    }


def get_detail(full_table_name: str) -> Dict:
    return spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()


def get_history(full_table_name: str):
    return spark.sql(f"DESCRIBE HISTORY {full_table_name}").collect()


def get_history_operations(full_table_name: str) -> List[str]:
    return [row["operation"] for row in get_history(full_table_name)]
