import os
import mysql.connector
import pandas as pd
from configparser import NoSectionError
from pathlib import Path
from mysql.connector.errors import DatabaseError, OperationalError, InternalError
from retrying import retry
from sroka.api.mysql.mysql_helpers import validate_options, get_options_from_config


@retry(stop_max_attempt_number=1,
       wait_exponential_multiplier=1 * 2,
       wait_exponential_max=1 * 2 * 2)
def query_mysql(query: str, filename=None):
    try:
        options = get_options_from_config()

    except NoSectionError:
        print('Missing MySQL section in configuration')
        return pd.DataFrame([])

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

    # Store the path in a cross-platform pathlib object to ensure compatibility
    # with DOS & UNIX-based operating systems.
    path = Path(filename)

    # Get the parent directory of the given path, if it exists.
    directory_path = str(path.parent.resolve())

    # If the given path points to a folder, attempt to create it. If it already
    # exists, the `exist_ok` option ensures that no exception will be thrown.
    if directory_path != "":
        os.makedirs(directory_path, exist_ok=True)

    # Export the data in a CSV file.
    try:
        df.to_csv(filename)
    except OSError as e:
        print('Unable to write on filesystem: {}'.format(e))

