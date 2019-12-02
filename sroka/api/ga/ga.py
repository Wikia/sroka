import os

import pandas as pd
from google.auth.exceptions import RefreshError
from googleapiclient import discovery
from googleapiclient.errors import HttpError

import sroka.config.config as config


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


def ga_request(input_dict, print_sample_size=False, sampling_level='HIGHER_PRECISION'):
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
            if print_sample_size:
                if not isinstance(print_sample_size, bool):
                    raise TypeError('print_sample_size must be boolean, not {}'.format(type(print_sample_size)))
                elif results['containsSampledData']:
                    sample_size = round(int(results['sampleSize']) / int(results['sampleSpace']) * 100, 2)
                else:
                    sample_size = 100
                print('Results calculated based on sample size ', sample_size, '%')
            return df

    except TypeError as error:
        # Handle errors in constructing a query.
        print(('There was an error in constructing your query : {}'.format(error)))
        return pd.DataFrame([])

    except HttpError as error:
        # Handle API errors.
        print(('Arg, there was an API error : {} : {}'.format(error.resp.status, error._get_reason())))
        return pd.DataFrame([])

    except RefreshError as error:
        # Handle Auth errors.
        print('The credentials have been revoked or expired, please re-run '
              'the application to re-authorize' + str(error))
        return pd.DataFrame([])

    except KeyError as error:
        # Handle wrong or missing values in query.
        if error.args[0] == 'rows':
            print('Your query did not return any rows.')
        else:
            print('There is an error or missing value in your query: {}'.format(error))
        return pd.DataFrame([])

    except AssertionError as error:
        # Handle errors in constructing a query.
        if not input_dict['dimensions']:
            print('Your query is missing dimensions.')
        else:
            print(('There was an error in constructing your query : {}'.format(error)))
        return pd.DataFrame([])
