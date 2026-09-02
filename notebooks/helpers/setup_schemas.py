"""Create / drop the per-student bronze, silver and gold schemas."""

from helpers import utils

spark = utils.spark


def create_user_schemas() -> None:
    catalog = utils.get_catalog()
    base_user = utils.get_base_user_schema()
    spark.sql(f"USE CATALOG {catalog}")
    for layer in utils.LAYERS:
        schema_name = f"{base_user}_{layer}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Created schema: {catalog}.{schema_name}")


def clean_up_schemas() -> None:
    catalog = utils.get_catalog()
    base_user = utils.get_base_user_schema()
    spark.sql(f"USE CATALOG {catalog}")
    for layer in utils.LAYERS:
        schema_name = f"{base_user}_{layer}"
        spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        print(f"Dropped schema: {catalog}.{schema_name}")
