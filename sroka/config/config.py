from collections import defaultdict

import configparser
import json
import os
import stat

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

default_config_filepath = os.path.expanduser('~/.sroka_config/')

config_overwrite = defaultdict(dict)


def set_config_overwrite(group, key, value):
    """
    Overwrite file config.

    Useful if you wish to overwrite some values from file config
    or replace it completely.

    :param [str] group: Overwrite a value in this group
    :param [str] key: Overwrite value with this key
    :param value: Value to set

    >>> set_config_overwrite('aws', 'aws_access_secret_key', 'secret_key_XXX')
    """
    config_overwrite[group][key] = value


def set_google_credentials(authorized_user_file,
                           key_file_location,
                           scope):

    try:
        credentials = Credentials.from_authorized_user_file(authorized_user_file, scopes=[scope])
    except FileNotFoundError:
        flow = InstalledAppFlow.from_client_secrets_file(key_file_location, [scope])
        credentials = flow.run_console()
        os.makedirs(os.path.expanduser('~/.cache/'), exist_ok=True)
        with open(authorized_user_file, 'w') as file:
            json.dump({
                'client_id': credentials._client_id,
                'client_secret': credentials._client_secret,
                'refresh_token': credentials._refresh_token
            }, file)
        os.chmod(authorized_user_file, 0o600)
    return credentials


def get_value(group, key):
    """Function that gets configuration from config file."""

    config_path = default_config_filepath + "config.ini"
    if 'CONFIG_FILE_PATH' in os.environ:
        config_path = os.environ['CONFIG_FILE_PATH']

    config = configparser.ConfigParser()
    readable_by_others = stat.S_IRGRP | stat.S_IROTH
    is_protected = not bool(os.stat(config_path).st_mode & readable_by_others)
    if not is_protected:
        raise Exception("Configuration file {config} is not protected, " +
                        "make sure you're the only one allowed to read " +
                        "it by executing `chmod 600 {config}`".format(config=config_path))
    config.read(config_path)

    return config_overwrite.get(group, {}).get(key, config.get(group, key))


def has_value(group, key):
    try:
        get_value(group, key)
        return True
    except (configparser.NoSectionError, configparser.NoOptionError):
        return False


def get_file_path(group):
    """Function that gets configuration files for APIs."""
    if group == 'google_ad_manager':
        return os.environ.get('CONFIG_AD_MANAGER', os.path.join(default_config_filepath, 'ad_manager.json'))
    if group == 'google_analytics':
        return os.environ.get('CONFIG_GOOGLE_ANALYTICS', os.path.join(default_config_filepath, 'client_secrets.json'))
    if group == 'google_drive':
        return os.environ.get('CONFIG_GOOGLE_DRIVE', os.path.join(default_config_filepath, 'credentials.json'))


def setup_env_variables(filepath=None):
    """Function that sets configuration file path."""
    config_filepath = filepath
    if not os.path.exists(config_filepath):
        raise Exception('Config file {} was not found!'.format(config_filepath))
    os.environ['CONFIG_FILE_PATH'] = config_filepath


def setup_admanager_config(filepath=None):
    """Function that sets configuration for Ad Manager file path."""
    config_filepath = filepath
    if not os.path.exists(config_filepath):
        raise Exception('Config file {} was not found!'.format(config_filepath))
    os.environ['CONFIG_AD_MANAGER'] = config_filepath


def setup_client_secret(filepath=None):
    """Function that sets configuration for Google Analytics file path."""
    config_filepath = filepath
    if not os.path.exists(config_filepath):
        raise Exception('Config file {} was not found!'.format(config_filepath))
    os.environ['CONFIG_GOOGLE_ANALYTICS'] = config_filepath


def setup_google_sheets_credentials(filepath=None):
    """Function that sets configuration for Google Sheets file path."""
    config_filepath = filepath
    if not os.path.exists(config_filepath):
        raise Exception('Config file {} was not found!'.format(config_filepath))
    os.environ['CONFIG_GOOGLE_DRIVE'] = config_filepath
