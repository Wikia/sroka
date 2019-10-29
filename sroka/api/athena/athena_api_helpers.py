import pandas as pd
from botocore.exceptions import ClientError, EndpointConnectionError
from retrying import retry


def return_on_exception(filename):
    if filename:
        return None
    else:
        return pd.DataFrame([])


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
