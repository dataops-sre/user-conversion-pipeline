import pandas as pd

from jobs.events_split.events_split_config import (
    input_data_schema,
)
from jobs.events_split.events_split_model import (
    split_user_registration_df,
    split_app_loaded_df,
)


def test_split_user_registration_df(spark_session):
    """test split_user_registration_df() function"""
    data = [
        {
            "initiator_id": 3074457347135400447,
            "timestamp": "2020-01-08T06:21:14.000Z",
            "event": "registered",
            "channel": "invited",
        },
        {
            "initiator_id": 3074457345816644047,
            "timestamp": "2020-01-08T06:24:42.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "79.0",
        },
        {
            "initiator_id": 3074457347135385819,
            "timestamp": "2020-01-08T06:25:10.000Z",
            "event": "registered",
        },
        {
            "initiator_id": 3074457347135385819,
            "timestamp": "2020-01-08T06:25:11.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "79.0",
        },
        {
            "initiator_id": 3074457346246864126,
            "timestamp": "2020-01-08T06:27:23.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "78.0",
        },
    ]

    expected_result = [
        {
            "event": "registered",
            "time": "2020-01-08T06:21:14.000Z",
            "initiator_id": 3074457347135400447,
            "derived_tstamp_day": "2020-01-08",
            "channel": "invited",
        },
        {
            "event": "registered",
            "time": "2020-01-08T06:25:10.000Z",
            "initiator_id": 3074457347135385819,
            "derived_tstamp_day": "2020-01-08",
        },
    ]

    df_data = spark_session.createDataFrame(data)

    result_df = split_user_registration_df(df_data).toPandas()
    expected_result_df = spark_session.createDataFrame(expected_result).toPandas()

    pd.testing.assert_frame_equal(
        result_df,
        expected_result_df,
        check_like=True,
        check_dtype=False,
    )


def test_split_app_loaded_df(spark_session):
    """test split_app_loaded_df() function"""
    data = [
        {
            "initiator_id": 3074457347135400447,
            "timestamp": "2020-01-08T06:21:14.000Z",
            "event": "registered",
            "channel": "invited",
        },
        {
            "initiator_id": 3074457345816644047,
            "timestamp": "2020-01-08T06:24:42.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "79.0",
        },
        {
            "initiator_id": 3074457347135385819,
            "timestamp": "2020-01-08T06:25:10.000Z",
            "event": "registered",
        },
        {
            "initiator_id": 3074457347135385819,
            "timestamp": "2020-01-08T06:25:11.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "79.0",
        },
        {
            "initiator_id": 3074457346246864126,
            "timestamp": "2020-01-08T06:27:23.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "browser_version": "78.0",
        },
    ]

    expected_result = [
        {
            "initiator_id": 3074457345816644047,
            "time": "2020-01-08T06:24:42.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "derived_tstamp_day": "2020-01-08",
        },
        {
            "initiator_id": 3074457347135385819,
            "time": "2020-01-08T06:25:11.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "derived_tstamp_day": "2020-01-08",
        },
        {
            "initiator_id": 3074457346246864126,
            "time": "2020-01-08T06:27:23.000Z",
            "event": "app_loaded",
            "device_type": "desktop",
            "derived_tstamp_day": "2020-01-08",
        },
    ]
    df_data = spark_session.createDataFrame(data)

    result_df = split_app_loaded_df(df_data).toPandas()
    print(result_df)
    expected_result_df = spark_session.createDataFrame(expected_result).toPandas()

    pd.testing.assert_frame_equal(
        result_df,
        expected_result_df,
        check_like=True,
        check_dtype=False,
    )
