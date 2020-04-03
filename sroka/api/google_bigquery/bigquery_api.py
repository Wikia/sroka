import pandas as pd
from google.cloud import bigquery
import sroka.config.config as config
from google.api_core.exceptions import Forbidden, NotFound, BadRequest


KEY_FILE = config.get_file_path('google_bigquery')


def query_bigquery(input_query, filename=None):

    if filename:
        if type(filename) != str:
            print('filename needs to be a string')
            return None

        if type(input_query) != str:
            print('input_query needs to be a string')
            return None

        try:
            f = open(filename, 'w')
            f.close()
        except FileNotFoundError:
            print('file cannot be saved in selected directory')
            return None

    else:
        if type(input_query) != str:
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
        if type(filename) != str:
            print('filename needs to be a string')
            return None

        if type(job_id) != str:
            print('input_query needs to be a string')
            return None

        try:
            f = open(filename, 'w')
            f.close()
        except FileNotFoundError:
            print('file cannot be saved in selected directory')
            return None

    else:
        if type(job_id) != str:
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
