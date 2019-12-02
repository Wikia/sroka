import json
from configparser import NoOptionError

import pandas as pd
import urllib3

import sroka.config.config as config
from sroka.api.moat.moat_api_helpers import validate_input_dict


def get_data_from_moat(moat_dict, database_name):
    """
    Function that downloads data from MOAT through API to pandas DataFrame.

    Args:
        moat_dict (dict):  dictionary with keys: 'start' (str : str) (start date of analysis 'YYYYMMDD') - obligatory,
                                                 'end' (str : str) (end date of analysis 'YYYYMMDD') - obligatory,
                                                 'columns' (str : list of str) (metrics in list) - obligatory,
                                                 'level1' (str : str) (company specific) - optional,
                                                 'level2' (str : str) (company specific) - optional,
                                                 'level3' (str : str) (company specific) - optional,
                                                 'level4' (str : str) (company specific) - optional
        database_name (str): name of db. Values (names of db and id provided by MOAT) need to be defined in config file

    Returns:
        pandas DataFrame

    Full documentation of MOAT API is available at http://api.moat.com/docs.
    """

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not validate_input_dict(moat_dict):
        return pd.DataFrame([])

    try:
        token = config.get_value('moat', 'token')
    except (KeyError, NoOptionError):
        print('No credentials were provided')
        return pd.DataFrame([])

    try:
        db_id = config.get_value('moat_db', database_name)
    except (KeyError, NoOptionError):
        print('Such database name is not available. Please check config file')
        return pd.DataFrame([])

    moat_dict['columns'] = ','.join(moat_dict['columns'])

    moat_dict['brandId'] = db_id

    http = urllib3.PoolManager()
    auth_header = 'Bearer {}'.format(token)
    resp = http.request('GET', 'https://api.moat.com/1/stats.json',
                        fields=moat_dict,
                        headers={'Authorization': auth_header})
    try:
        data = json.loads(resp.data)
    except TypeError:
        data = json.loads(resp.data.decode('utf-8'))

    if 'error' in data.keys():
        print('Error: ' + data['error'])
        return pd.DataFrame([])

    if data['results']['details'] == [[]]:
        print('Data returned is empty')
        return pd.DataFrame([])

    df = pd.DataFrame(data['results']['details'])
    return df
