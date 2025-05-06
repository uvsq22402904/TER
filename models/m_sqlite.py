from helper.neo4j_db import load_neo,get_all_etiquette,get_data_from_label
from helper.db import connector, create_table, bulk_insert_data
from helper.relations_extractor import get_neo_matrice_relations

"""
    Transform database from neo4j to sql.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def m_sqlite(uri: str):
    # connextion a notre bd sql
    driver_sql,metadata = connector(uri)
    
    # connexion a la bd neo4j
    driver_neo = load_neo()
    
    
    with driver_neo.session() as session:
        # get all labels 
        labels_data = get_all_etiquette(session)
        """
        
        print("[INFO] Début de la creation des tables")
        
        for table, data in labels_data.items():
            # insert all labels as table in sql
            create_table(table, data, driver_sql)
            print(f"[SUCCESS] Table created:  {table}")
        
        print("[INFO] Fin de la creation des tables")
        
        print("[INFO] Début de l'insertion des données")
        
        for table, data in labels_data.items():
            # get all data for each label
            labels_datas = get_data_from_label(table, session)
            print(f"[INFO] Au total {len(labels_datas)} ligne(s) à inserées")
            
            row_datas = [label_to_row(row) for row in labels_datas]
            
            bulk_insert_data(driver_sql, table, row_datas, data)
            
            print(f"[SUCCESS] Données inserées dans la table :  {table}")
          
        print("[INFO] Fin de l'insertion des données")
        
        
        print("[INFO] Début de la creation des liens")
        """
        # get matice of relations
        matrice_relations = get_neo_matrice_relations(session, labels_data.keys())
        """
        for table, data in labels_data.items():
            print(table)
        # insert relation for each data
        print("[INFO] Fin de la creation des liens")
        """
        
        
def label_to_row(data:dict):
    return {k: v for k, v in data.items() if k != "_types"}