from .db import sqlite_connector,getTables,getTableSchema

"""
    Transform database from sql to neo4j.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def db_relations(uri: str):
    try:
        moteur, metada = sqlite_connector(uri)
        return uri
    except Exception as e:
        print(f'ERROR: ', e)
        exit(1)
    