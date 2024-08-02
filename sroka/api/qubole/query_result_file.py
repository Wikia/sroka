import re
import tempfile

import boto3
from qds_sdk.account import Account
from qds_sdk.commands import Command, HiveCommand, PrestoCommand
from qds_sdk.exception import UnauthorizedAccess
from qds_sdk.qubole import Qubole

import sroka.config.config as config
from sroka.api.qubole.qubole_api import execute_with_handling_errors


def _download_to_local(s3, s3_path, fp, delim=None):
    s3_file_pattern = re.compile(r's3://([^/]+)/?(.*)')

    match = s3_file_pattern.match(s3_path)
    bucket_name = match.group(1)

    if s3_path.endswith('/') is False:
        key_name = match.group(2)
        file = s3.Object(bucket_name, key_name)
        fp.write(re.sub(r'\x01', delim, str(file.get()['Body'].read(), 'utf-8')))

    else:
        bucket = s3.Bucket(bucket_name)
        key_prefix = match.group(2)
        for file in bucket.objects.filter(Prefix=key_prefix):
            if 'SUCCESS' not in file.key:
                fp.write(re.sub(r'\x01', delim, str(file.get()['Body'].read(), 'utf-8')))


def _get_results(command, output, delimiter=None):
    conn = Qubole.agent()
    storage_credentials = conn.get(Account.credentials_rest_entity_path)

    session = boto3.Session(
            aws_access_key_id=storage_credentials['storage_access_key'],
            aws_secret_access_key=storage_credentials['storage_secret_key']
        )

    s3 = session.resource('s3')

    result_path = command.meta_data['results_resource']
    results = conn.get(result_path, {'inline': False, 'include_headers': 'false'})
    for s3_path in results['result_location']:
        _download_to_local(s3, s3_path, output, delimiter)


def get(query, delete_file=True, filepath='', delimiter=';', query_type='presto', cluster_label=None):

    with execute_with_handling_errors(config.get_value, 'qubole', 'api_token') as api_token:
        if api_token is None:
            return

    try:
        Qubole.configure(api_token=api_token)
    except UnauthorizedAccess:
        print("Invalid credentials were provided")
        return

    if isinstance(query, int):
        with execute_with_handling_errors(Command().find, id=query) as command:
            if command is None:
                return
    elif query_type == 'presto':
        with execute_with_handling_errors(PrestoCommand.run, query=query, label=cluster_label) as command:
            if command is None:
                return
    elif query_type == 'hive':
        with execute_with_handling_errors(HiveCommand.run, query=query, label=cluster_label) as command:
            if command is None:
                return
    else:
        print('Please verify your input.')
        return

    if filepath != '':
        file = open(filepath, 'w+')
    else:
        file = tempfile.NamedTemporaryFile(mode='w+', delete=delete_file)

    if command.status == 'done':
        _get_results(command, file, delimiter)
        file.seek(0)

        return file
    else:
        raise Exception('Could not retrieve query results (id: %s, status: %s)' % (command.id, command.status))
