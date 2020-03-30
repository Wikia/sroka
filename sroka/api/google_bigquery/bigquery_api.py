import pandas as pd
from google.cloud import bigquery
import sroka.config.config as config
from google.api_core.exceptions import Forbidden

KEY_FILE = config.get_file_path('google_bigquery')


def query_bigquery(input_query, filename=None):

    client = bigquery.Client.from_service_account_json(
        KEY_FILE)

    query_job = client.query(input_query)

    # try:
    df = query_job.result().to_dataframe()

    # except:
    #     return pd.DataFrame([])
    if filename:
        df.to_csv(df)
        print('saved to ' + filename)
        return None
    else:
        return df


def done_bigquery(job_id, filename=None):

    client = bigquery.Client.from_service_account_json(
        KEY_FILE)

    query_job = client.get_job(job_id=job_id)

    try:
        df = query_job.result().to_dataframe()
    except Forbidden as error:
        print(error)
        return pd.DataFrame([])
    if filename:
        df.to_csv(df)
        print('saved to ' + filename)
        return None
    else:
        return df
