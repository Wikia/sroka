import mysql.connector
import pandas as pd
from configparser import NoSectionError
from mysql.connector.errors import DatabaseError, OperationalError, InternalError
from retrying import retry
from sroka.api.mysql.mysql_helpers import validate_options, get_options_from_config
from sroka.api.helpers import save_to_file


@retry(stop_max_attempt_number=1,
       wait_exponential_multiplier=1 * 2,
       wait_exponential_max=1 * 2 * 2)
def query_mysql(query: str, filename=None,
                host=None, port=None,
                unix_socket=None, user=None,
                password=None, database=None
                ):
    try:
        options = get_options_from_config()

    except NoSectionError:
        print('Missing MySQL section in configuration')
        return pd.DataFrame([])

    if host:
        options['host'] = host
    if port:
        options['port'] = port
    if unix_socket:
        options['unix_socket'] = unix_socket
    if user:
        options['user'] = user
    if password:
        options['password'] = password
    if database:
        options['database'] = database

    if not validate_options(options):
        return pd.DataFrame([])

    try:
        # Connect while passing only the arguments that were set in the
        # configuration, since some are mutually exclusive or optional.
        connection = mysql.connector.connect(**{k: v for k, v in options.items() if v != ""})

        # Get the MySQL connector cursor, which allows interacting with the
        # MySQL server.
        cursor = connection.cursor()

        # Execute the query.
        cursor.execute(query)

    except OperationalError as e:
        print('Operational MySQL Error: {}'.format(e))
        return pd.DataFrame([])
    except InternalError as e:
        print('Internal MySQL Error: {}'.format(e))
        return pd.DataFrame([])
    except DatabaseError as e:
        print('Database MySQL Error: {}'.format(e))
        return pd.DataFrame([])

    # Cycle through the returned elements to build a pandas DataFrame.
    df = pd.DataFrame(cursor, columns=cursor.column_names)

    # Close the connection to the MySQL server.
    cursor.close()
    connection.close()

    # If no filename is specified, return the data as a pandas Dataframe.
    # Otherwise, store it in a file.
    if not filename:
        return df
    else:
        save_to_file(df, filename)
