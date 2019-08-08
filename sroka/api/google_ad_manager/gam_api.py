import gzip
import tempfile
from configparser import NoOptionError

import pandas as pd
from googleads import ad_manager, errors

import sroka.config.config as config

KEY_FILE = config.get_file_path('google_ad_manager')

# GAM API information.
try:
    APPLICATION_NAME = config.get_value('google_ad_manager', 'application_name')
except (KeyError, NoOptionError):
    APPLICATION_NAME = 'Application name'


def get_data_from_admanager(query, dimensions, columns, start_date, end_date, custom_field_id=None, network_code=None):

    if not custom_field_id:
        custom_field_id = []

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

    # Create statement object to filter for an order.

    filter_statement = {'query': query}

    # Create report job.
    report_job = {
      'reportQuery': {
          'dimensions': dimensions,
          'statement': filter_statement,
          'columns': columns,
          'customFieldIds': custom_field_id,
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
