from helper.neo4j_db import load_neo, get_all_etiquette, get_data_from_label
from helper.db import connector, create_table, bulk_insert_data
from helper.relations_extractor import get_neo_matrice_relations
from helper.constraints_extractor import extract_constraints, apply_constraints_to_neo4j
from sqlalchemy import text

"""
    Transform database from neo4j to sql.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def m_sqlite(uri: str):
    # connexion a notre bd sql
    driver_sql, metadata = connector(uri)
    
    # connexion a la bd neo4j
    driver_neo = load_neo()
    
    with driver_neo.session() as session:
        # get all labels 
        labels_data = get_all_etiquette(session)
        
        print("[INFO] Début de la creation des tables")
        
        # Créer les tables et leurs contraintes
        for table, data in labels_data.items():
            # Créer la table avec sa structure
            create_table(table, data, driver_sql)
            print(f"[SUCCESS] Table created: {table}")
            
            # Extraire et appliquer les contraintes
            constraints = extract_constraints(table, driver_sql)
            apply_constraints_to_neo4j(constraints, table, session)
            print(f"[SUCCESS] Constraints applied to: {table}")
        
        print("[INFO] Fin de la creation des tables")
        
        print("[INFO] Début de l'insertion des données")
        
        # Insérer les données dans les tables
        for table, data in labels_data.items():
            # get all data for each label
            labels_datas = get_data_from_label(table, session)
            print(f"[INFO] Au total {len(labels_datas)} ligne(s) à inserées")
            
            row_datas = [label_to_row(row) for row in labels_datas]
            bulk_insert_data(driver_sql, table, row_datas, data)
            
            print(f"[SUCCESS] Données inserées dans la table : {table}")
        
        print("[INFO] Fin de l'insertion des données")
        
        print("[INFO] Début de la creation des liens")
        
        # Récupérer la matrice des relations
        matrice_relations = get_neo_matrice_relations(session, labels_data.keys())
        
        if matrice_relations is not None and len(matrice_relations) > 0:
            # Créer les relations simples (one-to-many)
            insert_simple_relations(matrice_relations, driver_sql, session)
            
            # Créer les relations many-to-many
            insert_many_to_many_relations(matrice_relations, driver_sql, session)
        else:
            print("[WARNING] Aucune relation trouvée dans la base Neo4j")
        
        print("[INFO] Fin de la creation des liens")

def label_to_row(data: dict):
    """Convertit les données d'un nœud Neo4j en ligne SQLite."""
    return {k: v for k, v in data.items() if k != "_types"}

def check_relation_exists(session_neo, source_table, target_table, relation_name):
    """Vérifie si une relation existe dans Neo4j."""
    query = f"""
    MATCH (s:{source_table})-[r:{relation_name}]->(t:{target_table})
    RETURN count(r) as count
    """
    result = session_neo.run(query)
    record = result.single()
    return record and record['count'] > 0

def insert_simple_relations(relations_dict, driver_sql, session_neo):
    """Crée les relations one-to-many dans la base SQLite."""
    for source_table, relations in relations_dict.items():
        for target_table, relation_info in relations.items():
            if relation_info["type"] == "inner":
                print(f"[INFO] Création de la relation : {source_table} -----------> {target_table}")
                
                # Vérifier si la relation existe dans Neo4j
                if not check_relation_exists(session_neo, source_table, target_table, relation_info['name']):
                    print(f"[WARNING] La relation {relation_info['name']} n'existe pas dans Neo4j")
                    continue
                
                try:
                    with driver_sql.connect() as conn:
                        # Récupérer les données de la relation
                        query = f"""
                        MATCH (s:{source_table})-[r:{relation_info['name']}]->(t:{target_table})
                        RETURN s, t
                        """
                        result = session_neo.run(query)
                        
                        # Créer les clés étrangères
                        for record in result:
                            source_node = record['s']
                            target_node = record['t']
                            
                            # Mettre à jour la table source avec la clé étrangère
                            update_query = text(f"""
                            UPDATE {source_table}
                            SET {target_table}_id = :target_id
                            WHERE id = :source_id
                            """)
                            conn.execute(update_query, {
                                "target_id": target_node['id'],
                                "source_id": source_node['id']
                            })
                        
                        print(f"[SUCCESS] Relation créée : {source_table} -----------> {target_table}")
                except Exception as e:
                    print(f"[ERREUR] Erreur lors de la creation de la relation {source_table} -> {target_table} : {e}")

def insert_many_to_many_relations(relations_dict, driver_sql, session_neo):
    """Crée les relations many-to-many dans la base SQLite."""
    processed_relations = set()  # Pour garder une trace des relations déjà traitées
    
    for source_table, relations in relations_dict.items():
        for target_table, relation_info in relations.items():
            if relation_info["type"] == "join":
                # Créer le nom de la table d'association (toujours dans le même ordre pour éviter la duplication)
                tables = sorted([source_table, target_table])
                assoc_table = f"{tables[0]}_{tables[1]}_association"
                
                # Si la relation a déjà été traitée, on passe à la suivante
                if assoc_table in processed_relations:
                    continue
                
                processed_relations.add(assoc_table)
                print(f"[INFO] Création de la relation many-to-many : {source_table} <----> {target_table} (table: {assoc_table})")
                
                # Vérifier si la relation existe dans Neo4j
                if not check_relation_exists(session_neo, source_table, target_table, relation_info['name']):
                    print(f"[WARNING] La relation {relation_info['name']} n'existe pas dans Neo4j")
                    continue
                
                try:
                    with driver_sql.connect() as conn:
                        # Démarrer une transaction explicite
                        trans = conn.begin()
                        
                        try:
                            # Créer la table d'association avec toutes les contraintes dès le début
                            create_table_query = f"""
                            CREATE TABLE IF NOT EXISTS {assoc_table} (
                                {source_table}_id INTEGER NOT NULL,
                                {target_table}_id INTEGER NOT NULL,
                                PRIMARY KEY ({source_table}_id, {target_table}_id),
                                FOREIGN KEY ({source_table}_id) REFERENCES {source_table}(id) ON DELETE CASCADE,
                                FOREIGN KEY ({target_table}_id) REFERENCES {target_table}(id) ON DELETE CASCADE
                            )
                            """
                            conn.execute(text(create_table_query))
                            
                            # Récupérer les données de la relation avec leurs propriétés
                            query = f"""
                            MATCH (s:{source_table})-[r:{relation_info['name']}]->(t:{target_table})
                            RETURN s, t, r
                            """
                            print(f"[DEBUG] Exécution de la requête Neo4j: {query}")
                            result = session_neo.run(query)
                            
                            # Compter le nombre de relations trouvées
                            count = 0
                            for record in result:
                                count += 1
                                source_node = record['s']
                                target_node = record['t']
                                relation_props = record['r']
                                
                                print(f"[DEBUG] Relation trouvée: {source_table}(id={source_node['id']}) -> {target_table}(id={target_node['id']})")
                                
                                # Préparer les colonnes et valeurs pour l'insertion
                                columns = [f"{source_table}_id", f"{target_table}_id"]
                                values = [source_node['id'], target_node['id']]
                                
                                # Ajouter les propriétés de la relation si elles existent
                                if relation_props:
                                    for prop_name, prop_value in relation_props.items():
                                        if prop_name not in ['id']:  # Éviter les conflits avec les IDs
                                            # Ajouter la colonne si elle n'existe pas déjà
                                            if prop_name not in columns:
                                                alter_query = f"ALTER TABLE {assoc_table} ADD COLUMN {prop_name} TEXT"
                                                conn.execute(text(alter_query))
                                                columns.append(prop_name)
                                            values.append(prop_value)
                                
                                # Construire et exécuter la requête d'insertion
                                placeholders = ', '.join([':' + col for col in columns])
                                insert_query = text(f"""
                                INSERT INTO {assoc_table} ({', '.join(columns)})
                                VALUES ({placeholders})
                                """)
                                
                                # Créer le dictionnaire de paramètres
                                params = dict(zip(columns, values))
                                conn.execute(insert_query, params)
                            
                            # Valider la transaction
                            trans.commit()
                            print(f"[DEBUG] Nombre total de relations insérées: {count}")
                            print(f"[SUCCESS] Relation many-to-many créée : {source_table} <----> {target_table} (table: {assoc_table})")
                            
                        except Exception as e:
                            # En cas d'erreur, annuler la transaction
                            trans.rollback()
                            raise e
                            
                except Exception as e:
                    print(f"[ERREUR] Erreur lors de la creation de la table {assoc_table} : {e}")