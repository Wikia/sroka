import os

import pandas as pd
from typing import Dict
from google.auth.exceptions import RefreshError
from googleapiclient import discovery
from googleapiclient.errors import HttpError

import sroka.config.config as config
from contextlib import contextmanager


class GADataNotYetAvailable(Exception):
    pass


@contextmanager
def __ga_access(input_dict):
    """
    Creates a GA client and handles the API errors.
    In case of errors, it prints the error message and propagates the exception to the caller.

    :param input_dict: request parameters - for validation
    :return: service
    """
    # Authenticate and construct service.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    key_file_location = config.get_file_path('google_analytics')
    authorized_user_file = os.path.expanduser('~/.cache/google_analytics.json')

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = discovery.build('analytics', 'v3', credentials=credentials)
    try:
        first_profile_id = get_first_profile_id(service)
        if not first_profile_id:
            print('Could not find a valid profile for this user.')
            return pd.DataFrame([])
        else:
            yield service
    except TypeError as error:
        # Handle errors in constructing a query.
        print(('There was an error in constructing your query : {}'.format(error)))
        raise

    except HttpError as error:
        # Handle API errors.
        print(('Arg, there was an API error : {} : {}'.format(error.resp.status, error._get_reason())))
        raise

    except RefreshError as error:
        # Handle Auth errors.
        print('The credentials have been revoked or expired, please re-run '
              'the application to re-authorize' + str(error))
        raise

    except KeyError as error:
        # Handle wrong or missing values in query.
        if error.args[0] == 'rows':
            print('Your query did not return any rows.')
        else:
            print('There is an error or missing value in your query: {}'.format(error))
        raise

    except AssertionError as error:
        # Handle errors in constructing a query.
        if not input_dict['dimensions']:
            print('Your query is missing dimensions.')
        else:
            print(('There was an error in constructing your query : {}'.format(error)))
        raise

    except Exception as error:
        print(('There was an error while handling the request: {}'.format(error)))


def get_first_profile_id(service):

    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        first_account_id = accounts.get('items')[0].get('id')
        webproperties = service.management().webproperties().list(
            accountId=first_account_id).execute()

        if webproperties.get('items'):
            first_webproperty_id = webproperties.get('items')[0].get('id')
            profiles = service.management().profiles().list(
                accountId=first_account_id,
                webPropertyId=first_webproperty_id).execute()

            if profiles.get('items'):
                return profiles.get('items')[0].get('id')

    return None


def get_top_keywords(service, input_dict):
    to_remove = []
    for key in input_dict.keys():
        if input_dict[key] == '':
            to_remove.append(key)
        if key not in ['ids',
                       'start_date',
                       'end_date',
                       'metrics',
                       'segment',
                       'filters',
                       'sort',
                       'dimensions',
                       'sampling_level']:
            print("{} field is not used.".format(key))
    for key in to_remove:
        del input_dict[key]
    return service.data().ga().get(
        ids=input_dict['ids'] if 'ids' in input_dict.keys() else None,
        start_date=input_dict['start_date'] if 'start_date' in input_dict.keys() else None,
        end_date=input_dict['end_date'] if 'end_date' in input_dict.keys() else None,
        metrics=input_dict['metrics'] if 'metrics' in input_dict.keys() else None,
        segment=input_dict['segment'] if 'segment' in input_dict.keys() else None,
        filters=input_dict['filters'] if 'filters' in input_dict.keys() else None,
        sort=input_dict['sort'] if 'sort' in input_dict.keys() else None,
        dimensions=input_dict['dimensions'] if 'dimensions' in input_dict.keys() else None,
        samplingLevel=input_dict['sampling_level'] if 'sampling_level' in input_dict.keys() else None
    ).execute()


def print_results(results):
    # Print data table.
    if results.get('rows', []):
        for row in results.get('rows'):
            print(row)
    else:
        print('No Rows Found')


def __print_sample_size(print_sample_size, results):
    if print_sample_size:
        if not isinstance(print_sample_size, bool):
            raise TypeError('print_sample_size must be boolean, not {}'.format(type(print_sample_size)))
        elif results['containsSampledData']:
            sample_size = round(int(results['sampleSize']) / int(results['sampleSpace']) * 100, 2)
        else:
            sample_size = 100
        print('Results calculated based on sample size ', sample_size, '%')


def ga_request(input_dict, print_sample_size=False, sampling_level='HIGHER_PRECISION'):
    try:
        with __ga_access(input_dict) as service:
            input_dict['sampling_level'] = sampling_level
            results = get_top_keywords(service, input_dict)
            columns = results['query']['dimensions'].split(',') + results['query']['metrics']
            df = pd.DataFrame(results['rows'], columns=columns)

            for column in df.columns:
                try:
                    df[column] = pd.to_numeric(df[column])
                except ValueError:
                    pass
            df.columns = [x[3:] for x in list(df.columns)]

            __print_sample_size(print_sample_size, results)

            return df
    except:
        # the error message is displayed by context manager
        # this except clause is here just to keep the old API behavior (an empty DataFrame in case of an error)
        return pd.DataFrame([])


def ga_request_all_data(
        input_dict: Dict,
        start_index: int = 1,
        page_size: int = 10000,
        max_pages: int = None,
        print_sample_size: bool = False,
        sampling_level: str = 'HIGHER_PRECISION'):
    """
    Retrieves all available data from GA using pagination.
    Raises a GADataNotYetAvailable exception if there are no rows in the GA response.
    It propagates all GA exceptions to the caller.

    In case of invalid input parameters, it prints the error message and returns an empty DataFrame.

    :param input_dict:
        GA filters
    :param start_index:
        the index of the first element to be retrieved (integer, optional, default = 1, note that indexes start from 1!)
    :param page_size:
        the number of elements retrieved in a single request (integer, optional, default = 10000, min value = 1)
    :param max_pages:
        the max number of pages to retrieve, None if all available pages (integer, optional, default = None, min value = 1)
    :param print_sample_size:
        if True, prints the sample size of every request (boolean, optional, default = False)
    :param sampling_level:
        the GA sampling level (optional, default = HIGHER_PRECISION, valid values: 'DEFAULT', 'FASTER', 'HIGHER_PRECISION')

    :return: a Pandas data frame
    """
    if not isinstance(input_dict, Dict):
        print('input_dict={}'.format(input_dict))
        print('input_dict must be a dictionary. You can find valid keys/values in the GA documentation')
        return pd.DataFrame([])

    if not isinstance(start_index, int) or start_index < 1:
        print('start_index={}'.format(start_index))
        print('start_index must be a positive integer! The indexing starts from 1 not from 0!')
        return pd.DataFrame([])

    if not isinstance(page_size, int) or page_size < 1:
        print('page_size={}'.format(page_size))
        print('page_size must be an integer. The minimal page size is 1.')
        return pd.DataFrame([])

    if max_pages is not None and (not isinstance(max_pages, int) or max_pages < 1):
        print('max_pages={}'.format(max_pages))
        print('max_pages must be an integer. The minimal value is 1. Use None = retrieve all available pages.')
        return pd.DataFrame([])

    if not isinstance(print_sample_size, bool):
        print('print_sample_size={}'.format(print_sample_size))
        print('print_sample_size must be a boolean value')
        return pd.DataFrame([])

    if not isinstance(sampling_level, str) or sampling_level not in ['DEFAULT', 'FASTER', 'HIGHER_PRECISION']:
        print('sampling_level={}'.format(sampling_level))
        print('sampling_level has three valid values: DEFAULT, FASTER, HIGHER_PRECISION')
        return pd.DataFrame([])

    with __ga_access(input_dict) as service:
        if 'start_index' in input_dict.keys() or 'max_results' in input_dict.keys():
            print('This function overwrites start_index and max_results parameters! ' +
                  'Do not include them in the input_dict parameter.')
        input_dict = dict(input_dict)
        input_dict['max_results'] = page_size
        input_dict['samplingLevel'] = sampling_level

        all_rows = []
        fetched_rows_count = page_size
        current_index = start_index
        number_of_retrieved_pages = 0
        results = None
        is_first_page = True
        while fetched_rows_count == page_size and (max_pages is None or number_of_retrieved_pages < max_pages):
            input_dict['start_index'] = current_index
            results = service.data().ga().get(**input_dict).execute()
            if 'rows' not in results and is_first_page:
                raise GADataNotYetAvailable('There were no rows in the GA response!')
            elif 'rows' not in results:
                # special case for the number of available rows equal to a multiple of the page size
                break

            rows = results['rows']
            fetched_rows_count = len(rows)
            current_index += fetched_rows_count
            all_rows = all_rows + rows
            number_of_retrieved_pages += 1
            is_first_page = False

            __print_sample_size(print_sample_size, results)
            print('fetched {} of {} rows'.format(current_index - 1, results["totalResults"]))

        if not results or not all_rows:
            return pd.DataFrame([])

        columns = results['query']['dimensions'].split(',') + results['query']['metrics']
        columns = [x.strip() for x in columns]
        return pd.DataFrame(all_rows, columns=columns)
