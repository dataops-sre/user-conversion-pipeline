"""
Event split spark app configuration
Parse input env vars
"""
import os, logging

APP_NAME = os.getenv("APP_NAME", "next_week_conversion_rate")

USER_REGISTRATION_DATA_PATH = os.getenv(
    "USER_REGISTRATION_DATA_PATH", "./data-output/user_registration"
)
APP_LOADED_DATA_PATH = os.getenv("APP_LOADED_DATA_PATH", "./data-output/app_loaded")

## Logging configs
logging_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=logging_format, datefmt=logging_datefmt, level=logging.INFO)
logger = logging.getLogger(APP_NAME)
