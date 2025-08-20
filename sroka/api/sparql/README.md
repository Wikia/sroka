# SPARQL connector

## Configuration

Here are the configuration variables that the `sparql` connector supports:
* `endpoint_url`: Endpoint url for SPARQL database. Optional. It needs to be defined either in `config.ini` file or directly in called function.

## Methods

### `query_sparql(query, endpoint_url=None, filename=None)`

#### Arguments

* string `query` - SPARQL query to run
* string `endpoint_url` -  Endpoint URL for SPARQL database
* string `filename` - path to the file in which to store the results (optional, if `filename=None`, results are returned as a [`pandas`](https://pandas.pydata.org/pandas-docs/stable/) [`DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html))


#### Returns

* A [`DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) containing the query results or `None` if the data was saved to a file.

#### Usage

```python
from sroka.api.sparql.sparql import query_sparql

## Results saved to the file `results.csv`. Endpoint URL taken from config.
query_sparql("""
SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object .
}   
""", filename='results.csv')

## Results saved to the variable `dataframe`, as a pandas.DataFrame.
dataframe = query_sparql("""
SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object .
}
""")

## Queries database with configuration different from config.ini file
dataframe = query_sparql("""
SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object .
}
""", endpoint_url='https://query.database.org/sparql')

```

