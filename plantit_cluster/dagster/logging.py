import logging
import logging.handlers
from ast import literal_eval

from dagster import logger, Field


@logger(
    {
        'log_level': Field(str, is_required=False, default_value='INFO'),
        'name': Field(str, is_required=False, default_value='dagster'),
        'url': Field(str, is_required=True),
        'credentials': Field(str, is_required=True)
    },
    description='A logger that writes to the given HTTP endpoint.',
)
def http_logger(init_context):
    level = init_context.logger_config['log_level']
    name = init_context.logger_config['name']
    url = init_context.logger_config['url']
    credentials = literal_eval(init_context.logger_config['credentials'])

    klass = logging.getLoggerClass()
    logger_ = klass(name, level=level)

    handler = logging.handlers.HTTPHandler(url=url, method='POST', secure=True, credentials=credentials)

    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger_.addHandler(handler)

    return logger_
