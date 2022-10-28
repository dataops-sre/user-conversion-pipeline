"""
ETL python job for split input data into two data output
"""
import sys
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkFiles

from jobs.events_split.events_split_config import (
    APP_NAME,
    INPUT_DATA_PATH,
    OUTPUT_DATA_PATH_PREFIX,
    input_data_schema,
    logger,
)
from jobs.events_split.events_split_model import (
    events_split
)


def main():
    """
    Create spark session and run SPARK ETL
    """
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logger.info(
        f"""Starting batch process to parse input events into user_registration and app_loaded events
        with {INPUT_DATA_PATH} as source
        and {OUTPUT_DATA_PATH_PREFIX} as output path."""
    )
    input_data = extract_data(spark)
    data_transformed = transform_data(spark, input_data)
    load_data(data_transformed)
    spark.stop()


def extract_data(spark: SparkSession) -> DataFrame:
    """
    SPARK ETL --> E as Extract
    Extract input data in json format
    Load them as Spark Dataframes

    :param spark_session: spark session
    :return: The input dataframe
    """

    data_df = (
        spark.read.option("inferSchema", True)
        .schema(input_data_schema)
        .json(INPUT_DATA_PATH)
    )

    return data_df


def transform_data(spark_session, data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    SPARK ETL --> T as Transform
    1) Clean pageviews dataframe with blacklist dataframe
    2) Aggregate cleaned data with by page_titles and domain_code
    3) Compute topranks woth aggregated clean data

    :param spark_session: spark session
    :param data: Tuple of dataframes -> pageviews dataframe and blacklist dataframe
    :return: A dataframe contains top ranked page_titles per domain_code
    """
    splited_df_tuple = events_split(data)
    return splited_df_tuple


def load_data(data: Tuple[DataFrame, DataFrame]) -> None:
    """
    SPARK ETL --> L as Load
    Load user_registration and app_loaded dataframes to different targets, support s3 or local
    snappy parquet format

    output path format: outputs/user_registration/format=parquet/derived_tstamp_day=2020-01-07

    :param data: a dataframe tuple contains user_registration and app_loaded data
    :return: None
    """
    user_registration_df, app_loaded_df = data
    user_registration_df.repartition(1)\
        .write.option("compression", "snappy")\
        .save(
           path=f"{OUTPUT_DATA_PATH_PREFIX}/user_registration/format=parquet/",
           format="parquet",
           mode="overwrite",
           partitionBy="derived_tstamp_day",
         )
    app_loaded_df.repartition(1)\
        .write.option("compression", "snappy")\
        .save(
           path=f"{OUTPUT_DATA_PATH_PREFIX}/app_loaded/format=parquet/",
           format="parquet",
           mode="overwrite",
           partitionBy="derived_tstamp_day",
         )

# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
