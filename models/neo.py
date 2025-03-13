from helper.relations_extractor import db_relations

"""
    Transform database from sql to neo4j.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def neo(uri: str):
    all_relations = db_relations(uri)
    print(all_relations)