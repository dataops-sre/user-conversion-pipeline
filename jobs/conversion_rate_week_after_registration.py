"""
ETL pyspark job to calculate user conversion rate of one week after registration
"""

import sys
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession

from jobs.conversion_rate.conversion_rate_week_after_registration_config import (
    APP_NAME,
    USER_REGISTRATION_DATA_PATH,
    APP_LOADED_DATA_PATH,
    OUTPUT_DATA_PATH_PREFIX,
    logger,
)
from jobs.conversion_rate.user_conversion_rate_model import (
    dedup_user_registration_data,
    generate_user_conversion_data,
    calculate_weekly_summary,
    get_conversion_rate_week_after_registration,
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
    uc_df, ws_df = transform_data(input_data)
    load_data_s3(ws_df)
    load_data_console(uc_df)
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


def transform_data(data: Tuple[DataFrame, DataFrame]) -> Tuple[DataFrame, DataFrame]:
    """
    SPARK ETL --> T as Transform
    1) Prepare user registration data, remove duplicated records
    2) Generate user conversion time data
    3) Generate weekly summary data

    :param data:  A dataframe tuple contains user_registration and app_loaded data
    :return: user conversion time dataframe and weekly summary dataframe
    """
    u_df, a_df = data
    u_df = dedup_user_registration_data(u_df)
    uc_df = generate_user_conversion_data(u_df, a_df)
    ws_df = calculate_weekly_summary(uc_df)
    return (uc_df, ws_df)


def load_data_s3(df: DataFrame) -> None:
    """
    SPARK ETL --> L as Load
    load weekly summary into s3
    :return: None
    """
    df.repartition(1).write.option("compression", "snappy").save(
        path=f"{OUTPUT_DATA_PATH_PREFIX}/weekly_summary/format=parquet/",
        format="parquet",
        mode="overwrite",
        partitionBy="registration_week",
    )


def load_data_console(df: DataFrame) -> None:
    """
    SPARK ETL --> L as Load
    Print user conversion 1 week after registration rate
    :return: None
    """
    res = get_conversion_rate_week_after_registration(df)

    print(f"Metric: {round(res * 100, 2)}%")


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
