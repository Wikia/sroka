import pandas as pd
from py2neo import Graph
from py2neo import ClientError

import sroka.config.config as config


def neo4j_query_data(cypher, parameters=None, **kwparameters):

    if type(cypher) != str:
        print('Cypher query needs to be a string')
        return pd.DataFrame([])

    if len(cypher) == 0:
        print('Cypher query cannot be empty')
        return pd.DataFrame([])

    if parameters and type(parameters) != dict:
        print('Parameters need to be a dictionary')
        return pd.DataFrame([])

    neo4j_username = config.get_value('neo4j', 'neo4j_username')
    neo4j_password = config.get_value('neo4j', 'neo4j_password')
    neo4j_address = config.get_value('neo4j', 'neo4j_address')

    secure_graph = Graph("bolt://{}:{}@{}".format(neo4j_username, neo4j_password, neo4j_address))

    try:
        results = secure_graph.run(cypher, parameters, **kwparameters)
    except ClientError as e:
        print('There was an issue with the cypher query')
        print(e)
        return pd.DataFrame([])
    except AttributeError as e:
        print('There was an authentication issue')
        print(e)
        return pd.DataFrame([])

    results_pandas = pd.DataFrame(results.data())

    return results_pandas
