"""
ETL functions for events split, separate file for easier unittests
"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, date_format


def split_user_registration_df(data: DataFrame) -> DataFrame:
    """
    Split the original event batch to user_registration data, select useful columns and make
    renaming of timestamp column

    :param data: input dataframe.
    :return user_registration dataframe
    """
    user_registration_df = data.select(
        "event", "timestamp", "initiator_id", "channel"
    ).where(data.event == "registered")
    user_registration_df = user_registration_df.withColumn(
        "derived_tstamp_day", date_format(col("timestamp"), "yyyy-MM-dd")
    ).withColumnRenamed("timestamp", "time")
    return user_registration_df


def split_app_loaded_df(data: DataFrame) -> DataFrame:
    """
    Split the original event batch to app_loaded data, select useful columns and make
    renaming of timestamp column

    :param data: input dataframe.
    :return app_loaded dataframe
    """
    app_loaded_df = data.select(
        "event", "timestamp", "initiator_id", "device_type"
    ).where(data.event == "app_loaded")

    app_loaded_df = app_loaded_df.withColumn(
        "derived_tstamp_day", date_format(col("timestamp"), "yyyy-MM-dd")
    ).withColumnRenamed("timestamp", "time")
    return app_loaded_df
