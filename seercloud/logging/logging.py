import copy
import os
import logging

LOGGER_LEVEL = 'info'
LOGGER_STREAM = 'ext://sys.stderr'
LOGGER_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
LOGGER_FORMAT_SHORT = "[%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
LOGGER_LEVEL_CHOICES = ["debug", "info", "warning", "error", "critical"]


def setup_logger(log_level = LOGGER_LEVEL,
                log_format = LOGGER_FORMAT,
                stream = LOGGER_STREAM, filename=None):

    if log_level is None or str(log_level).lower() == 'none':
        return

    if stream is None:
        stream = LOGGER_STREAM

    if filename is None:
        filename = os.devnull

    if type(log_level) is str:
        log_level = logging.getLevelName(log_level.upper())

    config_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': log_format
            },
        },
        'handlers': {
            'console_handler': {
                'level': log_level,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': stream
            },
            'file_handler': {
                'level': log_level,
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': filename,
                'mode': 'a',
            },
        },
        'loggers': {
            'seercloud': {
                'handlers': ['console_handler'],
                'level': log_level,
                'propagate': False
            },
        }
    }
    if filename is not os.devnull:
        config_dict['loggers']['seercloud']['handlers'] = ['file_handler']

    logging.config.dictConfig(config_dict)