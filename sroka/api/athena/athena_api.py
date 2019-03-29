from configparser import NoOptionError
from urllib.parse import urlparse

import boto3
import pandas as pd
from botocore.exceptions import ClientError, EndpointConnectionError
from retrying import retry

import sroka.config.config as config


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=1 * 1000,
       wait_exponential_max=10 * 60 * 1000)
def poll_status(session, _id):
    try:
        result = session.get_query_execution(
            QueryExecutionId=_id
        )
    except ClientError:
        print("Invalid query_id")
        return None
    except EndpointConnectionError:
        print('Please check your credentials including aws_region in config.ini file')
        return None

    state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def download_file(s3, s3_bucket, s3_key, filename):
    if filename:
        try:
            s3.Bucket(s3_bucket).download_file(s3_key, filename)
        except FileNotFoundError:
            print('File or folder not found.')
            return None
        except ClientError:
            print('Please check your credentials including s3_bucket in config.ini file')
            return None
        print('saved to ' + filename)
        return None
    else:
        obj = s3.Object(s3_bucket, s3_key)
        try:
            obj = obj.get()
        except ClientError:
            print('Please check your credentials including s3_bucket in config.ini file')
            return pd.DataFrame([])
        df = pd.read_csv(obj['Body'])
        return df


def query_athena(query, filename=None):
    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError):
        print('No credentials were provided')
        return pd.DataFrame([])

    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key
    )

    athena = session.client('athena',
                            region_name=region)
    s3 = session.resource('s3')
    if not s3_bucket.startswith('s3://'):
        output_s3_bucket = 's3://' + s3_bucket
    else:
        output_s3_bucket = s3_bucket
        s3_bucket = s3_bucket.replace('s3://', '')
    try:
        result = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': output_s3_bucket,
            }
        )
    except ClientError:
        print('Please check your credentials including s3_bucket in config.ini file')
        return pd.DataFrame([])
    except EndpointConnectionError:
        print('Please check your credentials including aws_region in config.ini file')
        return pd.DataFrame([])
    query_id = result['QueryExecutionId']
    result = poll_status(athena, query_id)
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = query_id + '.csv'
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed')


def done_athena(query_id, filename=None):
    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError):
        print('No credentials were provided')
        return pd.DataFrame([])
    if s3_bucket.startswith('s3://'):
        s3_bucket = s3_bucket.replace('s3://', '')

    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key
    )

    s3 = session.resource('s3')
    athena = session.client('athena',
                            region_name=region)
    result = poll_status(athena, query_id)
    if result is None:
        return pd.DataFrame([])
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = urlparse(result['QueryExecution']['ResultConfiguration']['OutputLocation']).path[1:]
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed')
