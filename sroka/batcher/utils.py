from sroka.config import config
from zeep.helpers import serialize_object
import googleads.ad_manager as ad_manager
import os

PACKAGE_ROOT = os.path.dirname(os.path.realpath(__file__))


def _run_if_exists(fun, path):
    if os.path.isfile(path):
        fun(path)


def set_sroka_directory(sroka_config_directory: str):
    """If this doesn't work, move sroka configs below."""
    _run_if_exists(
        config.setup_env_variables,
        os.path.abspath(os.path.join(sroka_config_directory, "config.ini")),
    )
    _run_if_exists(
        config.setup_admanager_config,
        os.path.abspath(os.path.join(sroka_config_directory, "ad_manager.json")),
    )
    _run_if_exists(
        config.setup_client_secret,
        os.path.abspath(os.path.join(sroka_config_directory, "client_secrets.json")),
    )
    _run_if_exists(
        config.setup_google_sheets_credentials,
        os.path.abspath(os.path.join(sroka_config_directory, "credentials.json")),
    )
    _run_if_exists(
        config.setup_bigquery_config,
        os.path.abspath(
            os.path.join(sroka_config_directory, "bigquery_credentials.json")
        ),
    )
