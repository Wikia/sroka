import io
from configparser import NoOptionError
from contextlib import contextmanager

import pandas as pd
from qds_sdk.commands import Command, HiveCommand, PrestoCommand
from qds_sdk.exception import (
    ForbiddenAccess,
    ResourceInvalid,
    ResourceNotFound,
    UnauthorizedAccess,
)
from qds_sdk.qubole import Qubole

import sroka.config.config as config


@contextmanager
def execute_with_handling_errors(func, *args, **kwargs):
    result = None
    try:
        result = func(*args, **kwargs)
    except ResourceInvalid:
        print("Invalid resource")
    except ResourceNotFound:
        print('Resource not found')
    except UnauthorizedAccess:
        print("Invalid credentials were provided")
    except ForbiddenAccess:
        print("Forbidden access")
    except (KeyError, NoOptionError):
        print("No credentials were provided")
    finally:
        yield result


def request_qubole(input_query, query_type='presto', cluster_label=None):
    """Sends SQL query to Qubole and retrieves
    the data as pandas DataFrame.

    :param str input_query: query in chosen language (SQL)
    :param str query_type: query language specification {'presto' (default) or 'hive'}
    :param str cluster_label: Name of the Qubole cluster
    :return:  pandas DataFrame with response data.
    :rtype: pandas.DataFrame
    """
    with execute_with_handling_errors(config.get_value, 'qubole', 'api_token') as api_token:
        if api_token is None:
            return pd.DataFrame([])

    Qubole.configure(api_token=api_token)

    # run query
    if query_type == 'presto':
        with execute_with_handling_errors(PrestoCommand.run, query=input_query, label=cluster_label) as hc:
            if hc is None:
                return pd.DataFrame([])
    elif query_type == 'hive':
        with execute_with_handling_errors(HiveCommand.run, query=input_query, label=cluster_label) as hc:
            if hc is None:
                return pd.DataFrame([])
    else:
        print('Wrong query type')
        return pd.DataFrame([])

    print("Id: %s, Status: %s" % (str(hc.id), hc.status))

    try:
        hc.get_results(fp=open('./temp_qubole_output', 'wb'))
        with open('./temp_qubole_output', 'rb') as f:
            data = f.read()
        return qubole_output_to_df(data)

    except Exception as e:
        print(e)
        print("Oops!  There was a problem.  Try again...")
        return pd.DataFrame([])


def qubole_output_to_df(output):
    """Transforms Qubole output to pandas DataFrame

    :param bytes output: data returned by Qubole
    :return:  pandas DataFrame with response data.
    :rtype: pandas.DataFrame
    """
    lines = output.decode('utf-8').strip().split('\r\n')
    return pd.DataFrame([line.split('\t') for line in lines])


def done_qubole(query_id):
    """Sends query_id to Qubole and retrieves
    the data as pandas DataFrame.

    :param int query_id: query_id ready in Qubole
    :return:  pandas DataFrame with response data.
    :rtype: pandas.DataFrame
    """
    with execute_with_handling_errors(config.get_value, 'qubole', 'api_token') as api_token:
        if api_token is None:
            return pd.DataFrame([])

    Qubole.configure(api_token=api_token)

    with execute_with_handling_errors(Command().find, id=query_id) as res:
        if res is None:
            return pd.DataFrame([])

    print("Id: %s, Status: %s" % (str(res.id), res.status))

    try:
        response_buffer = io.BytesIO()
        res.get_results(response_buffer)
        return qubole_output_to_df(response_buffer.getvalue())

    except Exception as e:
        print(e)
        print("Oops!  There was a problem.  Try again...")
        return pd.DataFrame([])
