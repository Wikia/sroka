# MySQL connector

## Configuration

Here are the configuration variables that the `mysql` connector supports:

* `host`: The host address on which the MySQL server you want to access runs. Can be an IP or a domain (e.g.: `127.0.0.1`, `localhost`, `test.com/mysql`, etc.). Mutually exclusive with the `unix_socket` option.
* `port`: The port on which the MySQL server is exposed. Must be used with the `host` option. Not necessary when the `unix_socket` is defined.
* `unix_socket`: The path to the unix socket on which the MySQL server connection is exposed. (e.g.: `/var/run/mysql/mysql.sock`) Mutually exclusive with the `host` option.
* `user`: The username to connect on the database with.
* `password`: The password for the user to connect with. Optional.
* `database`: The database on which to connect. Optional.

## Methods

### `query_mysql(input_query, filename, host=None, port=None, unix_socket=None, user=None, password=None, database=None)`

#### Arguments

* string `input_query` - query to run
* string `filename` - path to the file in which to store the results (optional, if `filename=None`, results are returned as a [`pandas`](https://pandas.pydata.org/pandas-docs/stable/) [`DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html))
* string `host` - see description in `Configuration` section
* string `port` - see description in `Configuration` section
* string `unix_socket` - see description in `Configuration` section
* string `user` - see description in `Configuration` section
* string `password` - see description in `Configuration` section
* string `database` - see description in `Configuration` section

#### Returns

* A [`DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) containing the query results or `None` if the data was saved to a file.

#### Usage

```python
from sroka.api.mysql.mysql import query_mysql

## Results saved to the file `results.csv`.
query_mysql("""
    SELECT * FROM table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""", 'results.csv')

## Results saved to the variable `dataframe`, as a pandas.DataFrame.
dataframe = query_mysql("""
    SELECT * FROM table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""")

## Queries database with configuration different from config.ini file
dataframe = query_mysql("""
    SELECT * FROM table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""")

```

