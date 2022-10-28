"""
Event split spark app configuration
Parse input env vars
"""
import os, sys, logging
from pyspark.sql.types import (
    LongType,
    StringType,
    StructType,
    TimestampType,
)
from datetime import datetime

APP_NAME = os.getenv("APP_NAME", "events_split")
INPUT_DATA_PATH = os.getenv(
    "INPUT_DATA_PATH", "./data-input"
)

OUTPUT_DATA_PATH_PREFIX = os.getenv("OUTPUT_DATA_PATH_PREFIX", "./data-output")

input_data_schema = StructType() \
      .add("browser_version",StringType(),True) \
      .add("campaign",StringType(),True) \
      .add("channel",StringType(),True) \
      .add("device_type",StringType(),True) \
      .add("event",StringType(),True) \
      .add("initiator_id",LongType(),True) \
      .add("timestamp",TimestampType(),True)

## Logging configs
logging_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=logging_format, datefmt=logging_datefmt, level=logging.INFO)
logger = logging.getLogger(APP_NAME)
