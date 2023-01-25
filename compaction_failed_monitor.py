import base64
import mysql.connector
import time
import requests
import json
import yaml
import datetime
from ImplyAPI import ImplyAPI
import logging
import boto3
import mysql.connector
import statistics
from ast import literal_eval
from dateutil import parser
import pytz
from send_slack_message_module import send_slack_message_module

try:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(message)s')

    # Add a console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

except Exception as e:
    print("-- !!!! Error !!!! : Failed to create the logger.")
    raise


def secrets_validation(arguments, required_keys):
    try:
        for key in required_keys:
            if key not in arguments or arguments.get(key) == "":
                raise Exception("Required AWS secret '{}' is empty!!!".format(key))

    except Exception as e:
        raise e


def get_aws_secret_value(secret_name):
    try:
        secret_full_path = "mobile_data_infra/airflow/connections/{}".format(secret_name)
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_full_path)

        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret_value = get_secret_value_response['SecretString']
        else:
            secret_value = base64.b64decode(get_secret_value_response['SecretBinary'])

        return secret_value

    except Exception as e:
        logger.info("Failed to get aws secret value of secret: '{}'".format(secret_name))
        raise e


# -------------------------------------------------------------------
# Create a Secrets Manager client
# -------------------------------------------------------------------
session = boto3.session.Session()
secrets_manager_client = boto3.session.Session().client(service_name='secretsmanager', region_name="us-fullname")

logger.info("---- Get 'ism-data-in' RDS secrets ...")
management_rds_secrets = json.loads(get_aws_secret_value("rds"))
secrets_validation(management_rds_secrets, ['host', 'db_user', 'db_password'])

ism_data_infra_host = management_rds_secrets.get('host')
ism_data_infra_user = management_rds_secrets.get('db_user')
ism_data_infra_password = management_rds_secrets.get('db_password')

print_output = ""
conn = mysql.connector.connect(
    host=ism_data_infra_host,
    port=3306,
    user=ism_data_infra_user,
    password=ism_data_infra_password,
    database="dbname"
)

cursor = conn.cursor()
# query = ("SELECT cluster, data_source, COUNT(*) as failed_count FROM maint.druid_compaction_mng "
#          "WHERE status = 'FAILED' AND created_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR) "
#          "GROUP BY cluster, datasource")
query = ("SELECT cluster, data_source, COUNT(*) as failed_count FROM maint.druid_compaction_mng "
         "WHERE status = 'FAILED' AND created_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR) "
         "AND cluster in ('prod', 'prod1', 'prod2', 'prod3') "
         "GROUP BY cluster, datasource")

cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    cluster, data_source, failed_count = row
    # print(f"Cluster: {cluster}, Data source: {data_source}, {failed_count}")
    print_output += f"\n {cluster} --> {data_source} \n " \
                    f"num compaction jobs that failed in the last 24 hours: {failed_count}"

    message = print_output

    send_slack_message_module().write_to_slack_channel(['data-imply-daily-alerts'], message)
