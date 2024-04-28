from loguru import logger
from datetime import datetime
import os
import sys
from config import *


#  Set log level
LOG_LEVEL = LogLevel.DEBUG


def setup_logger(log_file_name):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger.remove()
    log_level = "DEBUG"
    log_full_path = os.path.join(LOG_DIR, log_file_name)

    console_log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>"
    file_log_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"

    logger.add(sys.stdout, level=log_level, format=console_log_format, colorize=True, backtrace=True, diagnose=True)
    logger.add(log_full_path, level=log_level, format=file_log_format, colorize=True, backtrace=True, diagnose=True)


def logd(message):
    if LOG_LEVEL == LogLevel.DEBUG:
        logger.debug(message)


def loge(message):
    logger.error(message)


def logi(message):
    if LOG_LEVEL == LogLevel.INFO or LOG_LEVEL == LogLevel.DEBUG:
        logger.info(message)


def logw(message):
    if LOG_LEVEL == LogLevel.INFO or LOG_LEVEL == LogLevel.DEBUG or LOG_LEVEL == LogLevel.WARNING:
        logger.warning(message)


def create_log_file():
    today = datetime.today().strftime("%Y%m%d")
    log_file_name = f"fmp-screener-log-{today}.log"
    setup_logger(log_file_name)


create_log_file()