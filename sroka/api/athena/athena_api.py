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
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidRequestException':
            print("Please check your query_id. Error message:")
        else:
            print("ClientError. Error message:")
        print(e)
        return None
    except EndpointConnectionError as e:
        print('Please check your credentials including aws_region in config.ini file and Internet connection.',
              'Error message:')
        print(e)
        return None

    state = result['QueryExecution']['Status']['State']

    if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
        return result
    else:
        print('Waiting for {} query to end'.format(_id))
        raise Exception


def download_file(s3, s3_bucket, s3_key, filename):
    if filename:
        try:
            s3.Bucket(s3_bucket).download_file(s3_key, filename)
        except FileNotFoundError as e:
            print('File or folder not found. Error message:')
            print(e)
            return None
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidRequestException':
                print("Please check your query. Error message:")
            elif e.response['Error']['Code'] == '':
                print('')
            else:
                print('Please check your credentials including s3_bucket in config.ini file. Error message:')
            print(e)
            return None
        print('saved to ' + filename)
        return None
    else:
        obj = s3.Object(s3_bucket, s3_key)
        try:
            obj = obj.get()
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidRequestException':
                print("Please check your query. Error message:")
            else:
                print('Please check your credentials including s3_bucket in config.ini file. Error message:')
            print(e)
            return None
        df = pd.read_csv(obj['Body'])
        return df


def query_athena(query, filename=None):
    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError) as e:
        print('No credentials were provided. Error message:')
        print(e)
        if filename:
            return None
        else:
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
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidRequestException':
            print("Please check your query. Error message:")
        else:
            print('Please check your credentials including s3_bucket in config.ini file. Error message:')
        print(e)
        if filename:
            return None
        else:
            return pd.DataFrame([])
    except EndpointConnectionError as e:
        print('Please check your credentials including aws_region in config.ini file and Internet connection.',
              'Error message:')
        print(e)
        if filename:
            return None
        else:
            return pd.DataFrame([])
    query_id = result['QueryExecutionId']
    result = poll_status(athena, query_id)
    if result is None:
        if filename:
            return None
        else:
            return pd.DataFrame([])
    elif result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = query_id + '.csv'
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed. Reason:')
        print(result['QueryExecution']['Status']['StateChangeReason'])
        if filename:
            return None
        else:
            return pd.DataFrame([])


def done_athena(query_id, filename=None):
    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError) as e:
        print('All or part of credentials were not provided. Please verify config.ini file. Error message:')
        print(e)
        if filename:
            return None
        else:
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
        if filename:
            return None
        else:
            return pd.DataFrame([])
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = urlparse(result['QueryExecution']['ResultConfiguration']['OutputLocation']).path[1:]
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed. Reason:')
        print(result['QueryExecution']['Status']['StateChangeReason'])
        return pd.DataFrame([])
