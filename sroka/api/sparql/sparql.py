from SPARQLWrapper import SPARQLWrapper, JSON
from sroka.api.sparql.sparql_helpers import get_options_from_config
from configparser import NoSectionError
from sroka.api.helpers import save_to_file
import pandas as pd


def query_sparql(query, endpoint_url=None, filename=None):
    try:
        options = get_options_from_config()

    except NoSectionError:
        print('Missing MySQL section in configuration')
        return pd.DataFrame([])
    if endpoint_url:
        options['endpoint_url'] = endpoint_url

    sparql = SPARQLWrapper(options["endpoint_url"])
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.queryAndConvert()['results']['bindings']
    dict_results = []
    for result in results:
        new_result = {}
        for key in result:
            new_result[key] = result[key]['value']
        dict_results.append(new_result)

    df = pd.DataFrame.from_dict(dict_results)

    # If no filename is specified, return the data as a pandas Dataframe.
    # Otherwise, store it in a file.
    if not filename:
        return df
    else:
        save_to_file(df, filename)
