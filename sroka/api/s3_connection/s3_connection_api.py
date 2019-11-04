import re
import warnings
from io import BytesIO, StringIO

import boto3
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, ParamValidationError
from pandas.errors import EmptyDataError

import sroka.config.config as config

warnings.filterwarnings('ignore')


def _download_data(key_prefix, s3, bucket_name, prefix, sep, skip_empty_files=True,
                   first_row_columns=True):
    if first_row_columns:
        header_setting = 'infer'
    else:
        header_setting = None
    df_list = []
    if prefix is False:
        file = s3.Object(bucket_name, key_prefix)
        try:
            data = StringIO(str(file.get()['Body'].read(), 'utf-8'))
        except UnicodeDecodeError:
            data = BytesIO(file.get()['Body'].read())
        except ClientError:
            print('File not found on s3')
            return pd.DataFrame([])
        try:
            df_list.append(pd.read_csv(data, error_bad_lines=False, warn_bad_lines=False, sep=sep,
                                       header=header_setting))
        except UnicodeDecodeError:
            df_list.append(pq.read_pandas(data).to_pandas())
        except EmptyDataError:
            print('File is empty')
            return pd.DataFrame([])

    else:
        bucket = s3.Bucket(bucket_name)

        try:
            for file in bucket.objects.filter(Prefix=key_prefix):
                if 'SUCCESS' not in file.key:
                    tmp = StringIO(str(file.get()['Body'].read(), 'utf-8'))
                    try:
                        data = pd.read_csv(tmp, error_bad_lines=False, warn_bad_lines=False, sep=sep,
                                           header=header_setting)
                        df_list.append(data)
                    except EmptyDataError:
                        if skip_empty_files is False:
                            print('Encountered an empty file: ', file.key)
                            return pd.DataFrame([])

        except UnicodeDecodeError:
            for file in bucket.objects.filter(Prefix=key_prefix):
                if 'SUCCESS' not in file.key:
                    data = BytesIO(file.get()['Body'].read())
                    df_list.append(pq.read_pandas(data).to_pandas())
        except ClientError:
            print('File not found on s3')
            return pd.DataFrame([])

    if not df_list:
        print('No matching file found')
        return pd.DataFrame([])
    data = pd.concat(df_list)
    return data


def s3_download_data(s3_filename, prefix=False, output_file=None, sep=',', skip_empty_files=True,
                     first_row_columns=True):
    key_id = config.get_value('aws', 'aws_access_key_id')
    access_key = config.get_value('aws', 'aws_secret_access_key')
    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key
    )

    s3 = session.resource('s3')

    s3_file_pattern = re.compile(r's3://([^/]+)/?(.*)')

    match = s3_file_pattern.match(s3_filename)
    bucket_name = match.group(1)

    key_prefix = match.group(2)

    if type(sep) == str and len(sep) == 1:

        data = _download_data(key_prefix, s3, bucket_name, prefix, sep, skip_empty_files,
                              first_row_columns)

        if output_file:
            data.to_csv(output_file, sep=sep)

        return data

    else:
        print('Separator must be a 1-character string')


def s3_upload_data(data, bucket, path, sep=','):
    key_id = config.get_value('aws', 'aws_access_key_id')
    access_key = config.get_value('aws', 'aws_secret_access_key')
    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key
    )

    if type(sep) == str and len(sep) == 1:

        csv_buffer = StringIO()

        if type(data) == pd.core.frame.DataFrame or type(data) == np.ndarray:

            if type(data) == pd.core.frame.DataFrame:
                data.to_csv(csv_buffer, sep=sep)
            elif type(data) == np.ndarray:
                np.savetxt(csv_buffer, data, delimiter=sep, fmt='%s')

            s3 = session.resource('s3')
            data = csv_buffer.getvalue()

            try:
                s3.Bucket(bucket).put_object(Key=path, Body=data)
                print('Success. File saved at s3://{}/{}'.format(bucket, path))
            except TypeError:
                print('Bucket name must be a string')
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    print('The specified bucket does not exist')
            except ParamValidationError as e:
                print(e)

        else:
            print('Uploaded file must be pandas DataFrame or numpy array and not {}'.format(type(data)))

    else:
        print('Separator must be a 1-character string')
