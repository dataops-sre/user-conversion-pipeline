"""
ETL functions for next week conversion rate, separate from job file for easier unittests
"""

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


def dedup_user_registration_data(data: DataFrame) -> DataFrame:
    """
    Prepare proper user registration data, when duplicate happens
    take earliest registed record

    :param data: user_registration dataframe.
    :return deduplicated user_registration dataframe
    """
    user_registration_df = data.groupby("initiator_id").agg(
        F.min("time").alias("time"),
        F.first("channel").alias("channel"),
        F.first("event").alias("event"),
    )
    return user_registration_df


def generate_user_conversion_data(u_df: DataFrame, a_df: DataFrame) -> DataFrame:
    """
    Generate user conversion data from user_registration and app_loaded data
    output data looks like :
    +-------------------+-------------------+-------------------+---------+
    |       initiator_id|                 rt|                 at|week_diff|
    +-------------------+-------------------+-------------------+---------+
    |3074457347194263830|2020-01-28 23:46:47|               null|     null|
    |3074457347186926395|2020-01-27 09:15:32|2020-01-30 13:44:06|        0|
    +-------------------+-------------------+-------------------+---------+

    :param u_df: user_registration dataframe.
    :param a_df: app_loaded dataframe.
    :return A user conversion time dataframe
    """
    first_time_app_loaded_df = a_df.groupby("initiator_id").agg(
        F.min("time").alias("time"),
        F.first("device_type").alias("device_type"),
        F.first("event").alias("event"),
    )
    conversion_time_data = (
        u_df.join(
            first_time_app_loaded_df,
            u_df.initiator_id == first_time_app_loaded_df.initiator_id,
            "left",
        )
        .select(
            u_df.initiator_id,
            u_df.time.alias("rt"),
            first_time_app_loaded_df.time.alias("at"),
        )
        .withColumn(
            "week_diff",
            (F.datediff(F.trunc("at", "week"), F.trunc("rt", "week")) / 7).cast("int"),
        )
    )

    return conversion_time_data


def get_conversion_rate_week_after_registration(data: DataFrame) -> float:
    """
    Get User conversion 1 week after the registration rate by dividing number of
    registed users who loaded the app in the next calendar week to the total number of registed users

    :param data: user conversion time dataframe.
    :return User conversion 1 week after the registration rate.
    """
    res = data.where(F.col("week_diff") == 1).count() / data.count()

    return res
