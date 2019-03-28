# Athena API

## Methods


### `query_athena(input_query, filename)`


#### Arguments

* string `input_query` - pass query in order to run new query
* string `filename` - directory with filename where results should be stored 
(optional, if filename=None results are returned as pandas DataFrame)

#### Returns

* DataFrame with results or None (if data saved in file)

#### Usage

```python
from sroka.api.athena.athena_api import query_athena

query_athena("""
    SELECT * FROM db.table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""", 'test3.csv')

query_athena("""
    SELECT * FROM db.table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""")
```

### `done_athena(query_id, filename)`

#### Arguments

* string `query_id` - pass no of query in order to download data from already done query
* string `filename` - directory with filename where results should be stored 
(optional, if filename=None results are returned as pandas DataFrame)

#### Returns

* DataFrame with results or None (if data saved in file)

#### Usage

```python
from sroka.api.athena.athena_api import done_athena

done_athena('1111111-222-3333-44444-55555555')

done_athena('1111111-222-3333-44444-55555555', 'test2.csv')
```

