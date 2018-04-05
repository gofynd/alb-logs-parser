'''
This file contains connector for where logs will be pushed. Currently LozIO connector is available
'''
import logging
import requests

from handlers import environment_variables

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def push_logs_to_logzio(data):
    '''
    :param data:
    :return:
    Logzio connector
    '''
    logzio_url = "{0}/?token={1}".format(
        environment_variables.LOGZIO_URL,
        environment_variables.LOGZIO_TOKEN
    )
    LOGGER.info('url for posting is %s' % (logzio_url))
    headers = {"Content-type": "text/plain"}
    result = requests.post(logzio_url, headers=headers, data=data)
    LOGGER.info(result.__dict__)
