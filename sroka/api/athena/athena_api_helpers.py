import pandas as pd
from botocore.exceptions import ClientError, EndpointConnectionError
from retrying import retry


def input_check(input_to_check, expected_types):
    for expected_type in expected_types:
        if type(input_to_check) == expected_type:
            if expected_type == str and len(input_to_check) == 0:
                print('Function input must be a nonempty string.')
                return False
            return True
    print('Function input must be a string.')
    return False


def return_on_exception(filename):
    return None if filename or filename == '' else pd.DataFrame([])


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
        try:
            df = pd.read_csv(obj['Body'])
        except ValueError as e:
            print('Something went wrong with query output formatting. Error message:')
            print(e)
            return None
        return df
