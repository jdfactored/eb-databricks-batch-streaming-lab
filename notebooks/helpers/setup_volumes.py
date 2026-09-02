"""Create / drop the volumes: raw files, stream checkpoints, Auto Loader schemas."""

from helpers import utils

spark = utils.spark


def create_volumes() -> None:
    catalog = utils.get_catalog()
    schema_name = f"{utils.get_base_user_schema()}_bronze"
    spark.sql(f"USE CATALOG {catalog}")
    for volume in utils.VOLUMES:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema_name}.{volume}")
        print(f"Created volume: /Volumes/{catalog}/{schema_name}/{volume}")


def clean_up_volumes() -> None:
    catalog = utils.get_catalog()
    schema_name = f"{utils.get_base_user_schema()}_bronze"
    spark.sql(f"USE CATALOG {catalog}")
    for volume in utils.VOLUMES:
        spark.sql(f"DROP VOLUME IF EXISTS {schema_name}.{volume}")
        print(f"Dropped volume: /Volumes/{catalog}/{schema_name}/{volume}")
