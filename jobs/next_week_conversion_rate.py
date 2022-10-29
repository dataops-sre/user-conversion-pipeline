"""
ETL pyspark job to calculate next week user conversion rate
"""
import sys
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession

from jobs.conversion_rate.next_week_conversion_rate_config import (
    APP_NAME,
    USER_REGISTRATION_DATA_PATH,
    APP_LOADED_DATA_PATH,
    logger,
)
from jobs.conversion_rate.user_conversion_rate_model import (
    dedup_user_registration_data,
    generate_user_conversion_data,
    get_next_week_conversion_rate,
)


def main():
    """
    Create spark session and run SPARK ETL
    """
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(
        f"""Starting process to produce next week conversion metrics
        parse input events into user_registration and app_loaded events
        with user_registration data from {USER_REGISTRATION_DATA_PATH},
        app_loaded data from {APP_LOADED_DATA_PATH},
        Result will be printed in stdout."""
    )
    input_data = extract_data(spark)
    data_transformed = transform_data(input_data)
    load_data(data_transformed)
    spark.stop()


def extract_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    SPARK ETL --> E as Extract
    Extract user_registration and app_loaded data in parquet format
    Load them as Spark Dataframes

    :param spark_session: spark session
    :return: A dataframe tuple contains user_registration and app_loaded data
    """

    u_df = spark.read.parquet(USER_REGISTRATION_DATA_PATH)
    a_df = spark.read.parquet(APP_LOADED_DATA_PATH)
    return (u_df, a_df)


def transform_data(data: Tuple[DataFrame, DataFrame]) -> float:
    """
    SPARK ETL --> T as Transform
    1) Prepare user registration data, remove duplicated records
    2) Generate user conversion time data
    3) Calculate next week user conversion rate from user conversion data

    :param data:  A dataframe tuple contains user_registration and app_loaded data
    :return: Next week conversion rate, as float
    """
    u_df, a_df = data
    u_df = dedup_user_registration_data(u_df)
    uc_df = generate_user_conversion_data(u_df, a_df)
    res = get_next_week_conversion_rate(uc_df)
    return res


def load_data(data: float) -> None:
    """
    SPARK ETL --> L as Load
    Print user conversion 1 week after registration rate
    :return: None
    """
    print(f"Metric: {round(data * 100,2)}%")


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
