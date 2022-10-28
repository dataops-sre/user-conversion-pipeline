
"""
ETL functions for events split
"""
from pyspark.sql.dataframe import DataFrame
from typing import Tuple
from pyspark.sql.functions import (
    col,
    date_format
)

def events_split(data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    user_registration_df = data.select("event",
                                 "timestamp",
                                 "initiator_id",
                                 "channel"
                                )
    app_loaded_df = data.select("event",
                              "timestamp",
                              "initiator_id",
                              "device_type"
                             )
    user_registration_df = user_registration_df.withColumn(
            "derived_tstamp_day", date_format(col("timestamp"), "yyyy-MM-dd")
        ).withColumnRenamed(
        'timestamp', 'time'
        )
    
    app_loaded_df = app_loaded_df.withColumn(
            "derived_tstamp_day", date_format(col("timestamp"), "yyyy-MM-dd")
        ).withColumnRenamed(
        'timestamp', 'time'
        )
    return (user_registration_df, app_loaded_df)