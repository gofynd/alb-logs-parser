'''

'''
import logging
import datetime
import json
import boto3

from handlers import load_python_modules
from handlers import queries
from handlers import environment_variables
from handlers.connectors import push_logs_to_logzio
from pyathena import connect

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ATHENA_CLIENT = boto3.client("athena", region_name=environment_variables.REGION)
S3_CLIENT = boto3.client("s3")


def athena_query_execution(query, log_path):
    LOGGER.info('executing athena query')
    LOGGER.info('query is {0}'.format(query))
    LOGGER.info('log path is: {0}'.format(log_path))

    response = ATHENA_CLIENT.start_query_execution(
        QueryString=query, ResultConfiguration={'OutputLocation': log_path}
    )
    LOGGER.info('Query executed: {0}'.format(query))
    LOGGER.info('Execution ID: ' + response['QueryExecutionId'])


def create_athena_db():
    LOGGER.info('Creating Athena Table')
    output_file_location = "s3://" + \
                           environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME + '/' + \
                           environment_variables.ATHENA_TABLE_CREATION_LOGS_S3_PATH
    alb_logs_location = "s3://" + \
                        environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME + '/' + \
                        environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_PREFIX + '/'

    for query in queries.ATHENA_DB_CREATION_QUERIES:
        athena_query_execution(
            query.format(
                db_name=environment_variables.ATHENA_DB_NAME,
                table_name=environment_variables.ATHENA_TABLE_NAME,
                log_location=alb_logs_location
            ),
            output_file_location
        )


def repair_disk_athena(event, context):
    LOGGER.info('Loading Athena Partition')
    output_file_location = "s3://" + \
                           environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME + '/' + \
                           environment_variables.ATHENA_DISK_REPAIR_LOGS_S3_PATH
    athena_query_execution(
        queries.QUERY_ATHENA_DISK_REPAIR.format(
            db_name=environment_variables.ATHENA_DB_NAME,
            table_name=environment_variables.ATHENA_TABLE_NAME),
        output_file_location
    )


def fetch_data_from_athena(event, context):
    '''
    Func get triggered after every 5 minutes.
    '''
    LOGGER.info('Fetching data from athena')
    try:
        s3_object = S3_CLIENT.get_object(
            Bucket=environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME,
            Key=environment_variables.ATHENA_QUERY_EXECUTION_TIME_S3_KEY)
        last_execution_time = s3_object['Body'].read().decode('utf-8')
        LOGGER.info('last execution time found: {0}'.format(last_execution_time))
    except Exception:
        LOGGER.error('time not found taking difference of 5 mins from now')
        last_execution_time = str(datetime.datetime.now() - datetime.timedelta(minutes=5))

    s3_staging_dir = "s3://" + environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME + '/' + environment_variables.ATHENA_QUERY_EXECUTION_LOGS_S3_PATH

    cursor = connect(s3_staging_dir=s3_staging_dir, region_name=environment_variables.REGION).cursor()
    now = datetime.datetime.now()
    cursor.execute(
        queries.QUERY_FETCH_NON_200_URL.format(
            last_execution_time=last_execution_time,
            db_name=environment_variables.ATHENA_DB_NAME,
            table_name=environment_variables.ATHENA_TABLE_NAME,
            year=now.strftime('%Y'),
            month=now.strftime('%m'),
        )
    )
    LOGGER.info('Preparing data.')
    data = ''
    for row in cursor:
        log_dict = {}
        log_dict['url'] = row[0]
        log_dict['count'] = row[1]
        log_dict['status'] = row[2]
        log_dict['message'] = 'ElbLogs'
        data = data + json.dumps(log_dict) + '\n'
    if data:
        last_execution_time = str(datetime.datetime.now())
        # you can remove func pushLogsToLogzIO and create on for any other service.
        LOGGER.info('posting data to logzio')
        push_logs_to_logzio(data)
    else:
        LOGGER.error('No results from query')
        S3_CLIENT.put_object(
            Bucket=environment_variables.ALB_LOGS_DESTINATION_S3_BUCKET_NAME,
            Key=environment_variables.ATHENA_QUERY_EXECUTION_TIME_S3_KEY,
            Body=str(last_execution_time))
