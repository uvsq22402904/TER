from helper.neo4j_db import load_neo, get_all_etiquette, get_data_from_label
from helper.db import connector, create_table, bulk_insert_data
from helper.relations_extractor import get_neo_matrice_relations
from sqlalchemy import text

"""
    Transform database from neo4j to sql.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def m_sqlite(uri: str):
    """Transform database from neo4j to sql."""
    try:
        # connexion a notre bd sql
        driver_sql, metadata = connector(uri)
        
        # connexion a la bd neo4j
        driver_neo = load_neo()
        
        with driver_neo.session() as session:
            # get all labels 
            labels_data = get_all_etiquette(session)
            
            print("[INFO] Début de la creation des tables")
            
            # Supprimer toutes les tables existantes
            drop_existing_tables(driver_sql, labels_data.keys())
            
            # Créer les tables et leurs contraintes
            for table, data in labels_data.items():
                # Créer la table avec sa structure
                create_table(driver_sql, table, data)
                print(f"[SUCCESS] Table created: {table}")
                
                # Extraire et appliquer les contraintes
                constraints = extract_neo4j_constraints(session, table)
                if constraints:
                    apply_constraints_to_sqlite(driver_sql, table, constraints)
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
            
    except Exception as e:
        print(f"[ERROR] Erreur lors de la transformation : {e}")
        raise
    finally:
        # Nettoyage des ressources
        if 'driver_sql' in locals():
            driver_sql.dispose()
        if 'driver_neo' in locals():
            driver_neo.close()

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

def extract_neo4j_constraints(session, table_name):
    """Extrait les contraintes d'une table Neo4j."""
    try:
        constraints = {
            "unique": [],
            "not_null": [],
            "primary_key": []
        }
        
        # Vérifier les propriétés uniques
        query = f"""
        MATCH (n:{table_name})
        WITH n, keys(n) as props
        UNWIND props as prop
        WITH prop, count(distinct n[prop]) as unique_count, count(n) as total_count
        WHERE unique_count = total_count AND unique_count > 0
        RETURN collect(prop) as unique_props
        """
        result = session.run(query)
        record = result.single()
        if record and record['unique_props']:
            constraints["unique"].extend(record['unique_props'])
        
        # Vérifier les propriétés non nulles
        query = f"""
        MATCH (n:{table_name})
        WITH n, keys(n) as props
        UNWIND props as prop
        WITH prop, count(n[prop]) as non_null_count, count(n) as total_count
        WHERE non_null_count = total_count
        RETURN collect(prop) as not_null_props
        """
        result = session.run(query)
        record = result.single()
        if record and record['not_null_props']:
            constraints["not_null"].extend(record['not_null_props'])
        
        # L'ID est toujours une clé primaire
        constraints["primary_key"].append("id")
        
        return constraints
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'extraction des contraintes pour {table_name}: {e}")
        return None

def apply_constraints_to_sqlite(driver_sql, table_name, constraints):
    """Applique les contraintes extraites à la table SQLite."""
    try:
        with driver_sql.connect() as conn:
            # Appliquer les contraintes NOT NULL
            for column in constraints.get('not_null', []):
                if column != 'id':  # L'ID est déjà NOT NULL
                    try:
                        query = text(f"""
                        ALTER TABLE {table_name} 
                        ADD CONSTRAINT nn_{table_name}_{column} 
                        CHECK ({column} IS NOT NULL)
                        """)
                        conn.execute(query)
                    except Exception as e:
                        print(f"Impossible d'appliquer NOT NULL à {column}: {e}")
            
            # Ne pas créer d'indices uniques
            # Les indices seront créés automatiquement par SQLite pour les clés primaires
            
            conn.commit()
            print(f"Contraintes appliquées avec succès à la table {table_name}")
        
    except Exception as e:
        print(f"Erreur lors de l'application des contraintes pour {table_name}: {e}")
        raise

def get_sqlite_type(neo4j_type):
    """Convertit un type Neo4j en type SQLite approprié."""
    print(f"[DEBUG] Type Neo4j reçu: {neo4j_type} (type: {type(neo4j_type)})")
    
    # Si le type est une liste, prendre le type des éléments
    if isinstance(neo4j_type, list) and len(neo4j_type) > 0:
        neo4j_type = neo4j_type[0]
        print(f"[DEBUG] Type après extraction de la liste: {neo4j_type}")
    
    # Si le type est un dictionnaire, prendre le type de la valeur
    if isinstance(neo4j_type, dict):
        neo4j_type = list(neo4j_type.values())[0]
        print(f"[DEBUG] Type après extraction du dictionnaire: {neo4j_type}")
    
    # Convertir le type en string et enlever les caractères spéciaux
    neo4j_type = str(neo4j_type).strip('[]{}')
    print(f"[DEBUG] Type après nettoyage: {neo4j_type}")
    
    # Règles spéciales pour certains types
    type_mapping = {
        'String': 'VARCHAR',
        'Integer': 'INTEGER',
        'Date': 'DATE',
        'DateTime': 'DATETIME',
        'Float': 'REAL',
        'Boolean': 'BOOLEAN',
        'Long': 'INTEGER',
        'Double': 'REAL',
        'LocalDate': 'DATE',
        'LocalDateTime': 'DATETIME',
        'LocalTime': 'TIME',
        'Duration': 'TEXT',
        'Point': 'TEXT',
        'Node': 'TEXT',
        'Relationship': 'TEXT',
        'Path': 'TEXT',
        'List': 'TEXT',
        'Map': 'TEXT'
    }
    
    # Règles spéciales pour les colonnes spécifiques
    if neo4j_type == 'id':
        return 'INTEGER'
    elif neo4j_type in ['nom', 'ville']:
        return 'VARCHAR'
    elif neo4j_type == 'date_embauche':
        return 'DATE'
    elif neo4j_type == 'entreprise_id':
        return 'INTEGER'
    
    # Utiliser le mapping pour les autres types
    sql_type = type_mapping.get(neo4j_type, 'TEXT')
    print(f"[DEBUG] Type SQLite final: {sql_type}")
    return sql_type

def drop_existing_indices(driver_sql, table_name):
    """Supprime tous les indices existants pour une table donnée."""
    try:
        with driver_sql.connect() as conn:
            # Récupérer tous les indices de la table
            query = text(f"""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND tbl_name='{table_name}'
            """)
            result = conn.execute(query)
            indices = [row[0] for row in result]
            
            # Supprimer chaque index
            for index in indices:
                if not index.startswith('sqlite_autoindex_'):  # Ne pas supprimer les indices automatiques
                    drop_query = text(f"DROP INDEX IF EXISTS {index}")
                    conn.execute(drop_query)
                    print(f"[DEBUG] Index {index} supprimé pour la table {table_name}")
            
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Erreur lors de la suppression des indices pour {table_name}: {e}")

def create_table(driver_sql, table_name, properties):
    """Crée une table SQLite avec les propriétés spécifiées."""
    try:
        # Vérifier si la table existe déjà
        with driver_sql.connect() as conn:
            result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
            if result.fetchone():
                print(f"La table {table_name} existe déjà.")
                # Supprimer les indices existants
                drop_existing_indices(driver_sql, table_name)
                return

            print(f"[DEBUG] Création de la table {table_name} avec les propriétés: {properties}")
            
            # Construire la requête de création de table
            columns = []
            for prop_name, prop_type in properties.items():
                # Déterminer le type SQLite approprié
                sql_type = get_sqlite_type(prop_name)  # Utiliser le nom de la propriété pour déterminer le type
                
                # Ajouter les contraintes NOT NULL et PRIMARY KEY pour les colonnes appropriées
                constraints = []
                if prop_name == 'id':
                    constraints.append('PRIMARY KEY')
                if prop_name in ['id', 'nom', 'ville', 'date_embauche']:  # Colonnes qui doivent être NOT NULL
                    constraints.append('NOT NULL')
                
                # Construire la définition de la colonne
                column_def = f"{prop_name} {sql_type}"
                if constraints:
                    column_def += " " + " ".join(constraints)
                
                columns.append(column_def)

            # Ajouter la contrainte de clé étrangère pour la table employe
            if table_name == 'employe':
                columns.append("FOREIGN KEY (entreprise_id) REFERENCES entreprise(id)")

            # Créer la table
            create_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            print(f"[DEBUG] Requête de création: {create_query}")
            conn.execute(text(create_query))
            
            # Ne pas créer d'indices uniques supplémentaires
            # Les indices seront créés automatiquement par SQLite pour les clés primaires
            
            conn.commit()
            print(f"Table {table_name} créée avec succès.")

    except Exception as e:
        print(f"Erreur lors de la création de la table {table_name}: {e}")
        raise

def drop_existing_tables(driver_sql, table_names):
    """Supprime toutes les tables existantes."""
    try:
        with driver_sql.connect() as conn:
            # Récupérer tous les indices existants
            query = text("SELECT name FROM sqlite_master WHERE type='index'")
            result = conn.execute(query)
            all_indices = [row[0] for row in result]
            
            # Supprimer tous les indices
            for index in all_indices:
                if not index.startswith('sqlite_autoindex_'):  # Ne pas supprimer les indices automatiques
                    drop_query = text(f"DROP INDEX IF EXISTS {index}")
                    conn.execute(drop_query)
                    print(f"[DEBUG] Index {index} supprimé")
            
            # Récupérer toutes les tables existantes
            query = text("SELECT name FROM sqlite_master WHERE type='table'")
            result = conn.execute(query)
            all_tables = [row[0] for row in result]
            
            # Supprimer toutes les tables
            for table in all_tables:
                if table != 'sqlite_sequence':  # Ne pas supprimer la table système
                    drop_query = text(f"DROP TABLE IF EXISTS {table}")
                    conn.execute(drop_query)
                    print(f"[DEBUG] Table {table} supprimée")
            
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Erreur lors de la suppression des tables: {e}")