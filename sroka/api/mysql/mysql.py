from configparser import NoOptionError

import mysql.connector
import pandas as pd
from retrying import retry

import sroka.config.config as config


@retry(stop_max_attempt_number=1,
       wait_exponential_multiplier=1 * 2,
       wait_exponential_max=1 * 2 * 2)
def query_mysql(query, filename=None):
    try:
        # Set the options in a dictionary, in order to pass only the
        # options that were provided in the configuration file to the
        # MySQL connector. Passing empty values would trigger exceptions.
        options = dict(
            host=config.get_value('mysql', 'host'),
            user=config.get_value('mysql', 'user'),
            password=config.get_value('mysql', 'password'),
            port=config.get_value('mysql', 'port'),
            unix_socket=config.get_value('mysql', 'unix_socket'),
            database=config.get_value('mysql', 'database'),
        )

    except (KeyError, NoOptionError):
        print('No connection information was provided')
        return pd.DataFrame([])

    # Connect while passing only the arguments that were set in the
    # configuration, since some are mutually exclusive or optional.
    connection = mysql.connector.connect(**{k: v for k, v in options.items() if v != ""})

    # Get the MySQL connector cursor, which allows interacting with the
    # MySQL server.
    cursor = connection.cursor()

    # Execute the given query.
    cursor.execute(query)

    # Cycle through the returned elements to build a pandas DataFrame.
    df = pd.DataFrame(cursor, columns=cursor.column_names)

    # Close the connection to the MySQL server.
    cursor.close()
    connection.close()

    if filename:
        df.to_csv(filename)
    else:
        return df
