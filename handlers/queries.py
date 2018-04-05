'''
Queries to be executed by Athena
'''
QUERY_CREATE_ATHENA_DB = """
CREATE DATABASE IF NOT EXISTS {db_name};
"""

QUERY_CREATE_ATHENA_TABLE = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
 type string,
 time string,
 elb string,
 client_ip string,
 client_port int,
 target_ip string,
 target_port int,
 request_processing_time double,
 target_processing_time double,
 response_processing_time double,
 elb_status_code string,
 target_status_code string,
 received_bytes bigint,
 sent_bytes bigint,
 request_verb string,
 request_url string,
 request_proto string,
 user_agent string,
 ssl_cipher string,
 ssl_protocol string,
 target_group_arn string,
 trace_id string,
 domain_name string,
 chosen_cert_arn string
 )
 PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1',
'input.regex' = '([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:\-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) (.*) (.*) (.*)' )
LOCATION '{log_location}'
"""


QUERY_ATHENA_DISK_REPAIR = """
MSCK REPAIR TABLE {db_name}.{table_name};
"""

QUERY_FETCH_NON_200_URL = """
  SELECT request_url, count(1) as count, elb_status_code
  FROM {db_name}.{table_name} Where (
  elb_status_code = '400' or elb_status_code = '404' 
  or elb_status_code = '405' or elb_status_code = '408' 
  or elb_status_code = '413' or elb_status_code = '415' 
  or elb_status_code = '460' or elb_status_code = '500' 
  or elb_status_code = '502' or elb_status_code = '503' 
  or elb_status_code = '504' ) 
  and year = '{year}' and month = '{month}' 
  and time >= '{last_execution_time}'
  GROUP BY request_url, elb_status_code

"""

ATHENA_DB_CREATION_QUERIES = (
    QUERY_CREATE_ATHENA_DB,
    QUERY_CREATE_ATHENA_TABLE,
    QUERY_ATHENA_DISK_REPAIR,
)
