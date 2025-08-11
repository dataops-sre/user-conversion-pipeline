import pandas as pd

from jobs.conversion_rate.user_conversion_rate_model import (
    dedup_user_registration_data,
    generate_user_conversion_data,
    calculate_weekly_summary,
    get_conversion_rate_week_after_registration,
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


def test_calculate_weekly_summary(spark_session):
    """
    Tests the calculate_weekly_summary() function.

    This test simulates the output of `generate_user_conversion_data` and uses it
    as input to verify the weekly aggregation logic.
    """
    input_data = [
        {
            "initiator_id": 1,
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-03T14:19:36.000Z",
            "week_diff": 0,
        },
        {
            "initiator_id": 2,
            "rt": "2020-11-02T14:19:36.000Z",
            "at": "2020-11-11T14:19:36.000Z",
            "week_diff": 1,
        },
        {
            "initiator_id": 3,
            "rt": "2020-11-03T14:19:36.000Z",
            "at": None,
            "week_diff": None,
        },
        {
            "initiator_id": 4,
            "rt": "2020-12-28T10:00:00.000Z",
            "at": "2020-12-29T10:00:00.000Z",
            "week_diff": 0,
        },
    ]

    expected_result = [
        {
            "registration_week": "2020-45",
            "total_registered": 3,
            "total_converted": 1,
            "conversion_rate": 33,
        },
        {
            "registration_week": "2020-53",
            "total_registered": 1,
            "total_converted": 1,
            "conversion_rate": 100,
        },
    ]

    # 3. Create Spark DataFrames
    input_df = spark_session.createDataFrame(input_data)

    expected_df = spark_session.createDataFrame(expected_result)

    # 4. Run the function being tested
    result_df = calculate_weekly_summary(input_df)

    # 5. Compare the actual result with the expected result using pandas
    result_pd = result_df.toPandas()
    expected_pd = expected_df.toPandas()

    pd.testing.assert_frame_equal(
        result_pd,
        expected_pd,
        check_like=True,  # Ignores column order
        check_dtype=False,  # Allows for int32 vs int64 differences etc.
    )


def test_get_conversion_rate_week_after_registration(spark_session):
    """test get_conversion_rate_week_after_registration() function"""
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

    df_data = spark_session.createDataFrame(data)

    result = get_conversion_rate_week_after_registration(df_data)
    expected_result = 0.5

    assert result == expected_result
