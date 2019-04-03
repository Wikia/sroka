# Qubole SDK

## Methods


### `request_qubole(input_query, query_type='presto', cluster_label=None)`


#### Arguments

* string `input_query` - pass query in order to run new query
* string `query_type` - defines whether the query is interpreted as Presto (`presto`) or Hive (`hive`) (default: `presto`)
* string `cluster_label` - name of the Qubole cluster node to use for a query

#### Returns

* pandas.DataFrame - Table with results

## Usage

```python
from sroka.api.qubole.qubole_api import request_qubole

presto_query = """
        SELECT *
        FROM
        db.table
        WHERE YEAR = '2019'
        AND MONTH = '03'
        LIMIT 50;
        """

data_presto = request_qubole(presto_query, query_type='presto')
```

### `done_qubole(query_id)`

#### Arguments

* string `query_id` - pass no of query in order to download data from already done query

#### Returns

* pandas.DataFrame - Table with results

## Usage

```python
from sroka.api.qubole.qubole_api import done_qubole
data = done_qubole(123456789)
```


### `get(query, [delete_file=True], [filepath=''], [delimiter=';'], [query_type='presto'], [cluster_label=None])`

This function does not return pandas DataFrame. It's main usage is for big data sets that are
not available as csv from Qubole UI and need to be downloaded directly from s3.

#### Arguments

* int|string `query` - pass query id (as int) in order to get existing query results or query text (as string) in order to run new query
* boolean `delete_file` - decides whether remove temporary file after running whole script (default: `True`)
* string `filepath` - decides where to put results (if it's not set, result file will be placed in tmp directory)
* string `delimiter` - data delimiter (default: `';'`)
* string `query_type` - defines whether the query is interpreted as Presto (`presto`) or Hive (`hive`) (default: `presto`)
* string `cluster_label` - name of the Qubole cluster node to use for a query

#### Returns

* [`tempfile.NamedTemporaryFile`]


## Usage

```python
from sroka.api.qubole.query_result_file import get

output = get(query=123456789)

print(output.read())
```

```python
from sroka.api.qubole.query_result_file import get
import pandas as pd

get(query=123456789, filepath='./123456789.csv', delete_file=True,
delimiter='\t')

data = pd.read_csv('./123456789.csv', header=None, sep='\t', low_memory=False)
data.head()
```
