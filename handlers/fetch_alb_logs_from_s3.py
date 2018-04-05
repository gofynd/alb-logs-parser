import logging
import boto3
from handlers.environment_variables import ALB_LOGS_DESTINATION_S3_BUCKET_PREFIX, ALB_LOGS_DESTINATION_S3_BUCKET_NAME, \
    ATHENA_IS_DB_CREATED_S3_KEY
from handlers.execute_athena_queries import create_athena_db

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")


def copy_alb_logs(event, context):
    '''
    This function will trigger on put event of Alb Logs s3 bucket.
    It copies zip file from destination bucket for source bucket for processing.
    '''
    LOGGER.info('function triggered')
    s3_destination_prefix = ALB_LOGS_DESTINATION_S3_BUCKET_PREFIX
    for record in event["Records"]:
        s3_meta_data = record["s3"]
        # getting source file name and path of the file.
        s3_source_bucket_name = s3_meta_data["bucket"]["name"]
        source_file_key = s3_meta_data["object"]["key"]
        # formatting destination path
        split_values = source_file_key.split('/')
        year_value = str(split_values[-4])
        month_value = str(split_values[-3])
        day_value = str(split_values[-2])
        file_name = str(split_values[-1])
        s3_destination_key = '{0}/year={1}/month={2}/day={3}/{4}'.format(
            s3_destination_prefix, year_value, month_value, day_value, file_name
        )
        copy_source = {
            'Bucket': s3_source_bucket_name,
            'Key': source_file_key
        }

        S3_CLIENT.copy_object(CopySource=copy_source,
                              Bucket=ALB_LOGS_DESTINATION_S3_BUCKET_NAME,
                              Key=s3_destination_key)

        # create athena table
        try:
            S3_CLIENT.get_object(Bucket=ALB_LOGS_DESTINATION_S3_BUCKET_NAME,
                                 Key=ATHENA_IS_DB_CREATED_S3_KEY)
        except Exception:
            create_athena_db()

        S3_CLIENT.put_object(
            Bucket=ALB_LOGS_DESTINATION_S3_BUCKET_NAME,
            Key=ATHENA_IS_DB_CREATED_S3_KEY,
            Body=str(True))
