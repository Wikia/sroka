import re
import warnings
from io import BytesIO, StringIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import sroka.config.config as config

warnings.filterwarnings('ignore')


def _download_data(key_prefix, s3, bucket_name, prefix, sep):
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
            df_list.append(pd.read_csv(data, error_bad_lines=False, warn_bad_lines=False, sep=sep))
        except UnicodeDecodeError:
            df_list.append(pq.read_pandas(data).to_pandas())

    else:
        bucket = s3.Bucket(bucket_name)

        try:
            for file in bucket.objects.filter(Prefix=key_prefix):
                if 'SUCCESS' not in file.key:
                    data = StringIO(str(file.get()['Body'].read(), 'utf-8'))
                    df_list.append(pd.read_csv(data, error_bad_lines=False, warn_bad_lines=False, sep=sep))
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


def s3_download_data(s3_filename, prefix=False, output_file=None, sep=','):
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
    data = _download_data(key_prefix, s3, bucket_name, prefix, sep)

    if output_file:
        data.to_csv(output_file, sep=sep)

    return data
