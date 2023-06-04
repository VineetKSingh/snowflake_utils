import logging
import configparser
import sys
import os
import inspect
import yaml
import socket

DEFAULT_LOG_PATH = '/var/log/snowflake'
DEFAULT_LOG_FILE_NAME = 'snowflake.log'

def apply_log_config(self, logger, log_path =None, log_file_name=None):
    if logger is None:
        raise Exception("Logger is required")
    else:
        logger.INFO("Searching for log config file")
        config_file = _search_log_config()
        try:
            with open(config_file) as f:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
        except Exception as e:
            logger.warn("Error in loading log config file; returning default  logger")
            logger.warn(e)
            raise e
    
    hostname = socket.gethostname().replace('.','_')
    log_file = None

    if log_path and not log_file_name:
        log_file = os.path.join(log_path, hostname + '_' + DEFAULT_LOG_FILE_NAME)
    elif not log_path and log_file_name:
        log_file = os.path.join(DEFAULT_LOG_PATH, hostname + '_' + log_file_name)
    else:
        log_file = os.path.join(DEFAULT_LOG_PATH,  hostname + '_' + DEFAULT_LOG_FILE_NAME)

    config["handlers"]["file_handler"]["filename"] = log_file
    logging.config.dictConfig(config)
    return logger


def _search_log_config():
    # get the calling frame
    calling_frame = inspect.currentframe().f_back
    calling_module = inspect.getmodule(calling_frame)
    # get the path of the calling module
    calling_module_path = os.path.dirname(calling_module.__file__)
    # search for the log config file in the calling module path
    log_config_file = os.path.join(calling_module_path, "snow_log.yaml")
    if os.path.isfile(log_config_file):
        return log_config_file
    else:
        # use embedded log config file
        return os.path.join(os.path.dirname(__file__), "snow_log.yaml")
