from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime
import pandas as pd

spark = SparkSession.builder.getOrCreate()
raw_posts_df = spark.read.parquet("spark-warehouse/raw_posts")

context = gx.get_context()

datasource_config = {
    "name": "spark_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "runtime_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        }
    },
}

try:
    context.add_datasource(**datasource_config)
except:
    pass

suite_name = "posts_validation_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_connector",
    data_asset_name="raw_posts",
    runtime_parameters={"batch_data": raw_posts_df},
    batch_identifiers={"batch_id": "posts_batch"},
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=suite_name
)

validator.expect_column_values_to_not_be_null(column="Id")
validator.expect_column_values_to_not_be_null(column="CreationDate")

validator.expect_column_values_to_be_between(
    column="CreationDate", max_value=datetime.now(), parse_strings_as_datetimes=True
)

validator.expect_column_values_to_be_in_set(
    column="PostTypeId", value_set=["1", "2", "3", "4"]
)

validator.save_expectation_suite(discard_failed_expectations=False)

results = validator.validate()
