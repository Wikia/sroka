import urllib.parse
import urllib.request
from configparser import NoOptionError
from io import StringIO

import pandas as pd

import sroka.config.config as config


def get_data_from_rubicon(rubicon_dict, currency='USD'):
    """
    Function that download data from Rubicon db through API to pandas DataFrame.

    Args:
        rubicon_dict (dict):  dictionary with keys:
            'start' (str : str) (Start date in ISO-8601 format, including time zone.) - obligatory,
            'end' (str : str) (End date in ISO-8601 format, including time zone.) - obligatory,
            'dimensions' (str : list of str) (dimensions that we want included as columns in list) - obligatory,
            'metrics' (str : list of str) (metrics that we want included as columns in list) - obligatory
            'filters' (str: list of str) (filters that we want to be included in list) - obligatory
        currency (str): currency to be used

    Returns:
        pandas DataFrame

    Full documentation (additional features) of Rubicon API is available at:
        https://resources.rubiconproject.com/resource/publisher-resources/performance-analytics-api/
        (you need to be logged in with provided credentials)
    """

    try:
        username = config.get_value('rubicon', 'username')
        password = config.get_value('rubicon', 'password')
        rubicon_id = config.get_value('rubicon', 'id')
    except (KeyError, NoOptionError):
        print('No credentials were provided')
        return pd.DataFrame([])

    if 'start' not in rubicon_dict.keys() or 'end' not in rubicon_dict.keys():
        print('Required fields are not set')
        return pd.DataFrame([])

    rubicon_dict['currency'] = currency

    try:
        if not rubicon_dict['dimensions'] or not rubicon_dict['metrics'] or not rubicon_dict['filters']:
            print('Required fields are empty')
            return pd.DataFrame([])
        rubicon_dict['dimensions'] = ','.join(rubicon_dict['dimensions'])
        rubicon_dict['metrics'] = ','.join(rubicon_dict['metrics'])
        rubicon_dict['filters'] = ';'.join(rubicon_dict['filters'])
    except KeyError:
        print('Required fields are not set')
        return pd.DataFrame([])

    url = urllib.parse.unquote(
        'https://api.rubiconproject.com/analytics/v1/report/?account=publisher/{}&'.format(
            rubicon_id) + urllib.parse.urlencode(rubicon_dict))

    p = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, url, username, password)

    auth_handler = urllib.request.HTTPBasicAuthHandler(p)

    opener = urllib.request.build_opener(auth_handler)

    opener.addheaders = [('Accept', 'text/csv')]

    urllib.request.install_opener(opener)

    try:
        result = opener.open(url)

    except IOError as e:
        print('Something went wrong, please see error message:')
        print(e)
        return pd.DataFrame([])

    result_data = result.read().decode()
    df = pd.read_csv(StringIO(result_data))
    return df
