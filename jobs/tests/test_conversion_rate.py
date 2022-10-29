import pandas as pd

from jobs.conversion_rate.user_conversion_rate_model import (
    dedup_user_registration_data,
    generate_user_conversion_data,
    get_next_week_conversion_rate,
)


def test_dedup_user_registration_data(spark_session):
    """test dedup_user_registration_data() function"""
    data = [
        {
            "event": "registered",
            "time": "2020-01-08T06:21:14.000Z",
            "initiator_id": 3074457347135400447,
            "derived_tstamp_day": "2020-01-08",
            "channel": "invited",
        },
        {
            "event": "registered",
            "time": "2020-01-08T06:20:54.000Z",
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

    expected_result = [
        {
            "event": "registered",
            "time": "2020-01-08T06:25:10.000Z",
            "initiator_id": 3074457347135385819,
        },
        {
            "event": "registered",
            "time": "2020-01-08T06:20:54.000Z",
            "initiator_id": 3074457347135400447,
            "channel": "invited",
        },
    ]

    df_data = spark_session.createDataFrame(data)

    result_df = dedup_user_registration_data(df_data).toPandas()
    expected_result_df = spark_session.createDataFrame(expected_result).toPandas()

    pd.testing.assert_frame_equal(
        result_df,
        expected_result_df,
        check_like=True,
        check_dtype=False,
    )


def test_generate_user_conversion_data(spark_session):
    """test generate_user_conversion_data() function"""
    u_df = [
        {"event": "registered", "time": "2020-11-02T14:19:36.000Z", "initiator_id": 1},
        {"event": "registered", "time": "2020-11-02T14:19:36.000Z", "initiator_id": 2},
        {"event": "registered", "time": "2020-11-03T14:19:36.000Z", "initiator_id": 3},
        {"event": "registered", "time": "2020-12-27T14:19:36.000Z", "initiator_id": 4},
    ]

    a_df = [
        {
            "event": "app_loaded",
            "time": "2020-11-03T14:19:36.000Z",
            "initiator_id": 1,
            "device_type": "desktop",
        },
        {
            "event": "app_loaded",
            "time": "2020-11-11T14:19:36.000Z",
            "initiator_id": 2,
            "device_type": "desktop",
        },
        {
            "event": "app_loaded",
            "time": "2020-11-12T14:19:36.000Z",
            "initiator_id": 2,
            "device_type": "desktop",
        },
        {
            "event": "app_loaded",
            "time": "2020-11-17T14:19:36.000Z",
            "initiator_id": 3,
            "device_type": "desktop",
        },
        {
            "event": "app_loaded",
            "time": "2021-01-01T14:19:36.000Z",
            "initiator_id": 4,
            "device_type": "desktop",
        },
    ]

    expected_result = [
        {
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-03T14:19:36.000Z",
            "initiator_id": 1,
            "week_diff": 0,
        },
        {
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-11T14:19:36.000Z",
            "initiator_id": 2,
            "week_diff": 1,
        },
        {
            "rt": "2020-11-03T14:19:36.000Z",
            "at": "2020-11-17T14:19:36.000Z",
            "initiator_id": 3,
            "week_diff": 2,
        },
        {
            "rt": "2020-12-27T14:19:36.000Z",
            "at": "2021-01-01T14:19:36.000Z",
            "initiator_id": 4,
            "week_diff": 1,
        },
    ]

    u_data = spark_session.createDataFrame(u_df)
    a_data = spark_session.createDataFrame(a_df)

    result_df = generate_user_conversion_data(u_data, a_data).toPandas()
    expected_result_df = spark_session.createDataFrame(expected_result).toPandas()

    pd.testing.assert_frame_equal(
        result_df,
        expected_result_df,
        check_like=True,
        check_dtype=False,
    )


def test_get_next_week_conversion_rate(spark_session):
    """test get_next_week_conversion_rate() function"""
    data = [
        {
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-03T14:19:36.000Z",
            "initiator_id": 1,
            "week_diff": 0,
        },
        {
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-11T14:19:36.000Z",
            "initiator_id": 2,
            "week_diff": 1,
        },
        {
            "rt": "2020-11-03T14:19:36.000Z",
            "at": "2020-11-17T14:19:36.000Z",
            "initiator_id": 3,
            "week_diff": 2,
        },
        {
            "rt": "2020-12-27T14:19:36.000Z",
            "at": "2021-01-01T14:19:36.000Z",
            "initiator_id": 4,
            "week_diff": 1,
        },
    ]

    expected_result = 0.5

    df_data = spark_session.createDataFrame(data)

    result = get_next_week_conversion_rate(df_data)

    assert result == expected_result
