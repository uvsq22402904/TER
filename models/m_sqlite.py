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
        
        # get matrice of relations
        matrice_relations = get_neo_matrice_relations(session, labels_data.keys())
        
        # Create foreign key relationships based on the matrix
        for source_table in matrice_relations.index:
            for target_table in matrice_relations.columns:
                if matrice_relations.loc[source_table, target_table] > 0:
                    # Create foreign key relationship
                    create_foreign_key(driver_sql, source_table, target_table)
                    print(f"[SUCCESS] Created foreign key from {source_table} to {target_table}")
        
        print("[INFO] Fin de la creation des liens")
        
        return True

def label_to_row(label_data):
    """
    Convert Neo4j node properties to a dictionary suitable for SQLite insertion.
    Removes internal Neo4j properties and _types metadata.
    """
    row = label_data.copy()
    # Remove internal Neo4j properties
    if '_types' in row:
        del row['_types']
    return row

def create_foreign_key(driver_sql, source_table, target_table):
    """
    Create a foreign key relationship between two tables.
    """
    try:
        # Add a foreign key column to the source table
        with driver_sql.connect() as conn:
            conn.execute(f"""
                ALTER TABLE {source_table}
                ADD COLUMN {target_table}_id INTEGER
            """)
            
            # Create the foreign key constraint
            conn.execute(f"""
                ALTER TABLE {source_table}
                ADD CONSTRAINT fk_{source_table}_{target_table}
                FOREIGN KEY ({target_table}_id)
                REFERENCES {target_table}(id)
            """)
    except Exception as e:
        print(f"[ERROR] Failed to create foreign key from {source_table} to {target_table}: {e}")