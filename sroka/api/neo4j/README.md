# neo4j graph database API

## Methods


### `neo4j_query_data(cypher, parameters=None)`


#### Arguments

* string `cypher` - cypher query (required)
* dictionary `parameters` - parameters for cypher query (optional)

#### Returns

* pandas DataFrame

## Usage
```python

from sroka.api.neo4j.neo4j_api import neo4j_query_data

df = neo4j_query_data("MATCH (n:Community {vertical: {N}}) RETURN n.dbname, n.locale LIMIT 10", {"N": "TV"})

```
