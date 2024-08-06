from SPARQLWrapper import SPARQLWrapper, JSON
from sroka.api.sparql.sparql_helpers import get_options_from_config
from configparser import NoSectionError
import pandas as pd

def query_sparql(query, endpoint_url=None):
    try:
        options = get_options_from_config()

    except NoSectionError:
        print('Missing MySQL section in configuration')
        return pd.DataFrame([])
    if endpoint_url:
        options['endpoint_url'] = endpoint_url

    sparql = SPARQLWrapper(endpoint_url)

