import pandas as pd
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud import bigquery

import sroka.config.config as config

KEY_FILE = config.get_file_path('google_bigquery')


def query_bigquery(input_query, filename=None):

    if filename:
        if not isinstance(filename, str):
            print('filename needs to be a string')
            return None

        if not isinstance(input_query, str):
            print('input_query needs to be a string')
            return None

        try:
            f = open(filename, 'w')
            f.close()
        except FileNotFoundError:
            print('file cannot be saved in selected directory')
            return None

    else:
        if not isinstance(input_query, str):
            print('input_query needs to be a string')
            return pd.DataFrame([])

    client = bigquery.Client.from_service_account_json(
        KEY_FILE)

    query_job = client.query(input_query)

    try:
        df = query_job.result().to_dataframe()

    except (NotFound, BadRequest) as error:
        print(error)
        if filename:
            return None
        return pd.DataFrame([])

    if filename:
        df.to_csv(filename)
        print('saved to ' + filename)
        return None
    else:
        return df


def done_bigquery(job_id, filename=None):

    if filename:
        if not isinstance(filename, str):
            print('filename needs to be a string')
            return None

        if not isinstance(job_id, str):
            print('input_query needs to be a string')
            return None

        try:
            f = open(filename, 'w')
            f.close()
        except FileNotFoundError:
            print('file cannot be saved in selected directory')
            return None

    else:
        if not isinstance(job_id, str):
            print('input_query needs to be a string')
            return pd.DataFrame([])

    client = bigquery.Client.from_service_account_json(
        KEY_FILE)
    try:
        query_job = client.get_job(job_id=job_id)
    except (BadRequest, NotFound) as error:
        print(error)
        if filename:
            return None
        return pd.DataFrame([])
    try:
        df = query_job.result().to_dataframe()
    except (Forbidden, NotFound) as error:
        print(error)
        if filename:
            return None
        return pd.DataFrame([])
    if filename:
        df.to_csv(filename)
        print('saved to ' + filename)
        return None
    else:
        return df
