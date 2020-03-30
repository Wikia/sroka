# Google BigQuery API

## Methods


### `query_bigquery(input_query, filename)`


#### Arguments

* string `input_query` - pass query in order to run new query
* string `filename` - directory with filename where results should be stored 
(optional, if filename=None results are returned as pandas DataFrame)

#### Returns

* DataFrame with results or None (if data saved in file)

#### Usage

```python
from sroka.api.google_bigquery.bigquery_api import query_bigquery

query_bigquery("""
    SELECT * FROM db.table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""", 'test3.csv')

query_bigquery("""
    SELECT * FROM db.table
    WHERE year='2018' and month='10' and day='07'
    LIMIT 10
""")
```

### `done_bigquery(job_id, filename)`

#### Arguments

* string `job_id` - pass no of query in order to download data from already done query 
(without project name and region, everythin after `.`)
* string `filename` - directory with filename where results should be stored 
(optional, if filename=None results are returned as pandas DataFrame)

#### Returns

* DataFrame with results or None (if data saved in file)

#### Usage

```python
from sroka.api.google_bigquery.bigquery_api import done_bigquery

done_bigquery('XXXXXXXX_XXXXXXXXXXX')

done_bigquery('XXXXXXXX_XXXXXXXXXXX', 'test2.csv')
```

