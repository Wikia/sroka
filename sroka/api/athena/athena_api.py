from configparser import NoOptionError
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

import sroka.config.config as config
from sroka.api.athena.athena_api_helpers import poll_status, download_file, return_on_exception, \
    input_check


def query_athena(query, filename=None):

    if not input_check(query, [str]):
        return return_on_exception(filename)

    if not input_check(filename, [str, type(None)]):
        return return_on_exception(filename)

    if filename == '':
        print('Filename cannot be empty')
        return return_on_exception(filename)

    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError) as e:
        print('No credentials were provided. Error message:')
        print(e)
        return return_on_exception(filename)

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
        return return_on_exception(filename)

    except EndpointConnectionError as e:
        print('Please check your credentials including aws_region in config.ini file and Internet connection.',
              'Error message:')
        print(e)
        return return_on_exception(filename)

    query_id = result['QueryExecutionId']
    result = poll_status(athena, query_id)
    if result is None:
        return return_on_exception(filename)

    elif result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = query_id + '.csv'
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed. Reason:')
        print(result['QueryExecution']['Status']['StateChangeReason'])
        return return_on_exception(filename)


def done_athena(query_id, filename=None):

    if not input_check(query_id, [str]):
        return return_on_exception(filename)

    if not input_check(filename, [str, type(None)]):
        return return_on_exception(filename)

    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError) as e:
        print('All or part of credentials were not provided. Please verify config.ini file. Error message:')
        print(e)
        return return_on_exception(filename)

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
        return return_on_exception(filename)
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = urlparse(result['QueryExecution']['ResultConfiguration']['OutputLocation']).path[1:]
        return download_file(s3, s3_bucket, s3_key, filename)
    else:
        print('Query did not succeed. Reason:')
        print(result['QueryExecution']['Status']['StateChangeReason'])
        return return_on_exception(filename)
