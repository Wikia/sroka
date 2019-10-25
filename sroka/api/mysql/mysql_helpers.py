import sroka.config.config as config
from configparser import NoOptionError


def get_options_from_config():
    # Set the options in a dictionary, in order to pass only the
    # options that were provided in the configuration file to the
    # MySQL connector. Passing empty values would trigger exceptions.
    options = dict()

    try:
        options["host"] = config.get_value('mysql', 'host')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    try:
        options["port"] = config.get_value('mysql', 'port')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    try:
        options["user"] = config.get_value('mysql', 'user')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    try:
        options["password"] = config.get_value('mysql', 'password')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    try:
        options["unix_socket"] = config.get_value('mysql', 'unix_socket')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    try:
        options["database"] = config.get_value('mysql', 'database')
    except (KeyError, NoOptionError):  # Do nothing, this value is optional.
        pass

    return options


# Ensures that either the options are set to connect to a remote database
# through a host/port combination, or through a unix socket.
def validate_options(options: dict):
    # If no connection option is given, there is not enough information to
    # connect to the database.
    if "unix_socket" not in options and "host" not in options:
        print('Invalid Configuration: In order to connect to the MySQL database, the host/port options or the unix_socket option must be set.')
        return False

    # If both keys are present and both have values, the configuration is
    # ambiguous.
    if "unix_socket" in options and "host" in options and \
            options["unix_socket"] != "" and options["host"] != "":
        print('Invalid Configuration: In order to connect to the MySQL database, the host/port options or the unix_socket option must be set.')
        return False
    return True
