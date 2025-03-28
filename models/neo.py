from helper.relations_extractor import db_relations,summary_relation
from helper.neo4j_db import load_neo, erase_neo_db, insert_noeud_from_table
from helper.db import connector

"""
    Transform database from sql to neo4j.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def neo(uri: str):
    # construire la matrice des relation
    all_relations = db_relations(uri)

    # dataframe contenant pour chaque table ses relations 
    table_summary_relation = summary_relation(all_relations)
    
    print(table_summary_relation)
    
    """"
    # connextion a notre bd sql
    driver_sql,metadata = connector(uri)
    # connexion a la bd neo4j
    driver_neo = load_neo()
    # formatage de la bd no4j
    erase_neo_db(driver_neo)
    
    # recuperer les nom des tables non [associative]
    tables = all_relations.columns.to_list()
    # filtrer les dictionniare de la bd pour garder unique ceux qui sont des noeuds (ici on veut aussi les metadonnée des tables notement les colonnes et leur type)
    tables_for_noeud = dict(filter(lambda item: item[1] in tables, metadata.tables.items()))
    
    # Start transformation
    with driver_neo.session() as session:
        # creer des noeuds pour toute les tables qui ne sont pas [associative]
        for table_name, table_struct in tables_for_noeud:
            res = insert_noeud_from_table(table_name, table_struct, session, driver_sql)
            if res:
              print(f"{table_struct} has ended the transformation successfully...............")
            else:
                print(f"Something went wrong for {table_name} please try again.....")
                
        # Creer des relation entre les noeuds
    """
 