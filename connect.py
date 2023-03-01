import boto3
import json
import os
import pymysql
import subprocess

ENDPOINT = "dsvtomtest.cluster-cjqoldqpaz53.us-east-1.rds.amazonaws.com"
PORT = 3306
USER = "iam"
REGION = "us-east-1"
DBNAME = "testdb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

dsv_json = subprocess.check_output(['dsv', 'secret', 'read', 'aws:aws-dynamic'])

tokens = json.loads(dsv_json)

access_key = tokens['data']['accessKey']
secret_access_key = tokens['data']['secretKey']
session_token = tokens['data']['sessionToken']

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    aws_session_token=session_token,
    region_name=REGION
)
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

try:
    conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, database=DBNAME,
                           ssl_ca='rds-ca-2019-root.pem')
    cur = conn.cursor()
    cur.execute("""SELECT * from Users""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))
