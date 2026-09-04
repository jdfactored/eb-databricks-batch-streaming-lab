from pyspark.sql.types import StructType, StructField, StringType, LongType

def test_validate_schema(**kwargs):
    schema_evoluted = kwargs.get("schema_evoluted", False)
    df = kwargs.get("df")
    if schema_evoluted!=True:
        expected_schema = StructType([
            StructField("device_type", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("session_id", StringType(), True),
            StructField("user_id", LongType(), True),
            StructField("_rescued_data", StringType(), True)
        ])
    else:
        expected_schema = StructType([
            StructField("device_type", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("session_id", StringType(), True),
            StructField("user_id", LongType(), True),
            StructField("_rescued_data", StringType(), True),
            StructField("referrer_url", StringType(), True)
        ])
    actual_schema = df.schema
    assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"

def test_validate_data_integrity(**kwargs):
    df = kwargs.get("df")

    critical_columns = ["event_id", "session_id"]
    for col in critical_columns:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, f"Null values found in column: {col}"

def test_validate_row_count(**kwargs):
    df = kwargs.get("df")

    row_count = df.count()
    assert row_count > 0, f"Row count is not greater than 0: {row_count}"

def test_validate_event_id_uniqueness(**kwargs):
    df = kwargs.get("df")

    event_id_count = df.select("event_id").distinct().count()
    total_count = df.count()
    assert event_id_count == total_count, f"event_id is not unique: {event_id_count} distinct IDs vs {total_count} total rows"

