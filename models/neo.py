from helper.relations_extractor import db_relations,summary_relation
from helper.neo4j_db import add_link, load_neo, erase_neo_db, insert_noeud_from_table
from helper.db import connector, get_all, get_all_relations,get_single_table_relations

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
    

    # connextion a notre bd sql
    driver_sql,metadata = connector(uri)
    # connexion a la bd neo4j
    driver_neo = load_neo()
    # formatage de la bd no4j
    erase_neo_db(driver_neo)
    
    # recuperer les nom des tables non [associative]
    tables = all_relations.columns.to_list()
    
    # filtrer les dictionniare de la bd pour garder unique ceux qui sont des noeuds (ici on veut aussi les metadonnée des tables notement les colonnes et leur type)
    tables_for_noeud = {key: value for key, value in metadata.tables.items() if key in tables}
    
    # Start transformation
    
    with driver_neo.session() as session:
        
        # creer des noeuds pour toute les tables qui ne sont pas [associative]
        for table_name, table_struct in tables_for_noeud.items():
            res = insert_noeud_from_table(table_name, table_struct, session, driver_sql)
            if res:
              print(f"{table_struct} has ended the transformation successfully...............")
            else:
                print(f"Something went wrong for {table_name} please try again.....")
                
        print(f"[INFO] Début de la création des liens")
        
        # construction de liens direct entrte les noeuds (clé étrangère simple)
        insert_simple_relation(table_summary_relation, metadata, driver_sql, session)
        # Creer des relations entre les noeuds
        insert_many_to_many_relation(table_summary_relation, metadata, driver_sql, session)
        
        print(f"[INFO] Fin de la création des liens")
        
                
          

def insert_simple_relation(table_summary_relation, metadata, driver, session_neo):
    for source_table, relations in table_summary_relation.items():
        # Filtrer les relations INNER uniquement
        inner_relations = {
            target_table: details for target_table, details in relations.items()
            if details.get("type") == "inner"
        }

        for target_table, relation_info in inner_relations.items():
            # Récupérer les métadonnées et la relation FK → PK
            rel_data = get_all_relations(driver, source_table, target_table)
            key_map = {row['from']: row['to'] for _, row in rel_data.iterrows()}

            print(f"[INFO] start creating link : {source_table} -----------> {target_table}")

            # Récupérer les lignes de la table source
            source_rows = get_all(driver, source_table)

            for _, row in source_rows.iterrows():
                # Extraire les clés étrangères de la source
                foreign_keys = {k: row[k] for k in key_map.keys()}
                # Adapter les noms de colonnes à ceux de la cible
                target_keys = {key_map[k]: v for k, v in foreign_keys.items()}

                # Création du lien dans Neo4j
                add_link(
                    session_neo,
                    source_table,
                    foreign_keys,
                    target_table,
                    target_keys,
                    relation_info["name"]
                )

            print(f"[SUCCESS] link created : {source_table} -----------> {target_table}")
         

def insert_many_to_many_relation(table_summary_relation, metadata, driver, session_neo):
    for source_table, relations in table_summary_relation.items():
        # Filtrer les relations INNER uniquement
        inner_relations = {
            target_table: details for target_table, details in relations.items()
            if details.get("type") == "join"
        }  
        
        for target_table, relation_info in inner_relations.items() :
            # Récupérer les métadonnées et la relation FK → PK
            rel_data = get_single_table_relations(driver, relation_info["table"])
            key_map = {
                row['table']: {"to": row['to'], "from": row['from']}
                for _, row in rel_data.iterrows()
            }
            
            print(f"[INFO] Début de la creation du lien : {source_table} -----------> {target_table}")
            
            # Récupérer les lignes de la table source
            source_rows = get_all(driver, relation_info["table"])
            for _, row in source_rows.iterrows():
                # Extraire les clés étrangères de la source
                foreign_keys = {table_key["to"]: row[table_key["from"]] for r_table, table_key in key_map.items() if r_table == source_table}
                target_keys = {table_key["to"]: row[table_key["from"]] for r_table, table_key in key_map.items() if r_table == target_table}
                

                # Création du lien dans Neo4j
                
                add_link(
                    session_neo,
                    source_table,
                    foreign_keys,
                    target_table,
                    target_keys,
                    relation_info["name"],
                    row
                )
                

            print(f"[SUCCESS] Lien crée : {source_table} -----------> {target_table}")
                