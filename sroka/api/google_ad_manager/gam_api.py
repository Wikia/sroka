import gzip
import tempfile
from configparser import NoOptionError

import pandas as pd
from googleads import ad_manager, errors

import sroka.config.config as config

# import variable_validators as validator

KEY_FILE = config.get_file_path('google_ad_manager')

# GAM API information.
try:
    APPLICATION_NAME = config.get_value('google_ad_manager', 'application_name')
except (KeyError, NoOptionError):
    APPLICATION_NAME = 'Application name'


def dict_type_checker(dict_argument, argument_name, mandatory=True):
    if mandatory:
        if type(dict_argument) != dict:
            print(f"""{argument_name} needs to be a dict""")
            return "Incorrect type"
    else:
        if dict_argument and type(dict_argument) != dict:
            print(f"""{argument_name} needs to be a dict""")
            return "Incorrect type"


def int_type_checker(int_argument, argument_name, mandatory=True):
    if mandatory:
        if type(int_argument) != int:
            print(f"""{argument_name} needs to be an integer""")
            return "Incorrect type"
    else:
        if int_argument and type(int_argument) != int:
            print(f"""{argument_name} needs to be an integer""")
            return "Incorrect type"


def list_type_checker(list_argument, argument_name, mandatory=True):
    if mandatory:
        if type(list_argument) != list:
            print(f"""{argument_name} needs to be a list""")
            return "Incorrect type"
    else:
        if list_argument and type(list_argument) != list:
            print(f"""{argument_name} needs to be a list""")
            return "Incorrect type"


def str_type_checker(str_argument, argument_name, mandatory=True):
    if mandatory:
        if type(str_argument) != str:
            print(f"""{argument_name} needs to be a string""")
            return "Incorrect type"
    else:
        if str_argument and type(str_argument) != str:
            print(f"""{argument_name} needs to be a string""")
            return "Incorrect type"


def init_gam_connection(network_code=None):
    if not network_code:
        try:
            network_code = config.get_value('google_ad_manager', 'network_code')
        except (KeyError, NoOptionError):
            print('No network code was provided')
            return pd.DataFrame([])
    yaml_string = "ad_manager: " + "\n" + \
                  "  application_name: " + APPLICATION_NAME + "\n" + \
                  "  network_code: " + str(network_code) + "\n" + \
                  "  path_to_private_key_file: " + KEY_FILE + "\n"

    # Initialize the GAM client.
    gam_client = ad_manager.AdManagerClient.LoadFromString(yaml_string)
    return gam_client


def get_data_from_admanager(query, dimensions, columns, start_date, end_date, custom_field_id=None,
                            dimension_attributes=None, network_code=None):
    if not custom_field_id:
        custom_field_id = []

    if not dimension_attributes:
        dimension_attributes = []

    list_of_types = [str_type_checker(query, "query"),
                     list_type_checker(dimensions, "dimensions"),
                     list_type_checker(columns, "columns"),
                     dict_type_checker(start_date, "start_date"),
                     dict_type_checker(end_date, "end_date"),
                     list_type_checker(custom_field_id, "custom_field_id", False),
                     list_type_checker(dimension_attributes, "dimmension_attributes", False),
                     int_type_checker(network_code, "network_code", False)]

    if "Incorrect type" in list_of_types:
        return

    gam_client = init_gam_connection(network_code)

    # Create statement object to filter for an order.

    filter_statement = {'query': query}

    # Create report job.
    report_job = {
        'reportQuery': {
            'dimensions': dimensions,
            'statement': filter_statement,
            'columns': columns,
            'customFieldIds': custom_field_id,
            'dimensionAttributes': dimension_attributes,
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date,
            'endDate': end_date,
            'adUnitView': "FLAT"
        }
    }

    report_downloader = gam_client.GetDataDownloader()

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.GoogleAdsServerFault as e:
        if 'AuthenticationError.NETWORK_NOT_FOUND' in str(e):
            print('Provided network code was not found.')
        elif 'AuthenticationError.NETWORK_CODE_REQUIRED' in str(e):
            print('Default value of network code is missing from ', config.default_config_filepath)
        else:
            print('Failed to generate report. Error was: {}'.format(e))
        return

    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: {}'.format(e))
        return

    export_format = 'CSV_DUMP'

    report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)

    # Download report data.
    report_downloader.DownloadReportToFile(
        report_job_id, export_format, report_file, use_gzip_compression=True)

    report_file.close()

    # Display results.
    print('Report job with id {} downloaded to:\n{}'.format(
        report_job_id, report_file.name))
    with gzip.open(report_file.name) as file:
        data = pd.read_csv(file)
    return data


def get_users_from_admanager(query, dimensions, network_code=None):
    list_of_types = [str_type_checker(query, "query"),
                     list_type_checker(dimensions, "dimensions"),
                     int_type_checker(network_code, "network_code", False)]

    if "Incorrect type" in list_of_types:
        return

    user_df = pd.DataFrame()

    dimensions_df = pd.DataFrame()

    # .Where() handles filtering with admanager method, while statement_query handles queries with WHERE clause

    statement_query = query.upper().replace("WHERE ", "")

    statement = ad_manager.StatementBuilder().Where(statement_query)

    gam_client = init_gam_connection(network_code)

    user_service = gam_client.GetService('UserService')

    try:
        while True:
            response = user_service.getUsersByStatement(statement.ToStatement())
            if 'results' in response and len(response['results']):
                for user in response['results']:

                    for dimension in dimensions:
                        try:
                            dimension_value = [user[dimension]]
                            dimensions_df[dimension] = dimension_value
                        except KeyError as e:
                            print('Failed to generate user list. Incorrect dimension: {}'.format(e))
                            return

                    user_df = user_df.append(dimensions_df, sort=False)
                statement.offset += statement.limit
            else:
                break
        return user_df

    except errors.GoogleAdsServerFault as e:
        if 'AuthenticationError.NETWORK_NOT_FOUND' in str(e):
            print('Provided network code was not found.')
        elif 'AuthenticationError.NETWORK_CODE_REQUIRED' in str(e):
            print('Default value of network code is missing from ', config.default_config_filepath)
        else:
            print('Failed to generate user list. Error was: {}'.format(e))
        return

    except errors.AdManagerReportError as e:
        print('Failed to generate user list. Error was: {}'.format(e))
        return


def get_companies_from_admanager(query, dimensions, network_code=None):
    list_of_types = [str_type_checker(query, "query"),
                     list_type_checker(dimensions, "dimensions"),
                     int_type_checker(network_code, "network_code", False)]

    if "Incorrect type" in list_of_types:
        return

    company_df = pd.DataFrame()

    dimensions_df = pd.DataFrame()

    # .Where() handles filtering with admanager method, while statement_query handles queries with WHERE clause

    statement_query = query.upper().replace("WHERE ", "")

    statement = ad_manager.StatementBuilder().Where(statement_query)

    gam_client = init_gam_connection(network_code)

    company_service = gam_client.GetService('CompanyService')

    try:
        while True:
            response = company_service.getCompaniesByStatement(statement.ToStatement())
            if 'results' in response and len(response['results']):
                for company in response['results']:

                    for dimension in dimensions:
                        try:
                            dimension_value = [company[dimension]]
                            dimensions_df[dimension] = dimension_value
                        except KeyError as e:
                            print('Failed to generate company list. Incorrect dimension: {}'.format(e))
                            return

                    company_df = company_df.append(dimensions_df, sort=False)
                statement.offset += statement.limit
            else:
                break
        return company_df

    except errors.GoogleAdsServerFault as e:
        if 'AuthenticationError.NETWORK_NOT_FOUND' in str(e):
            print('Provided network code was not found.')
        elif 'AuthenticationError.NETWORK_CODE_REQUIRED' in str(e):
            print('Default value of network code is missing from ', config.default_config_filepath)
        else:
            print('Failed to generate company list. Error was: {}'.format(e))
        return

    except errors.AdManagerReportError as e:
        print('Failed to generate company list. Error was: {}'.format(e))
        return
