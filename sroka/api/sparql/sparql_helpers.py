import sroka.config.config as config
from configparser import NoOptionError

def get_options_from_config():
    options = dict()

    try:
        options["endpoint_url"] = config.get_value('sparql', 'endpoint_url')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    return options
