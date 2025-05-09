from helper.neo4j_db import load_neo,get_all_etiquette,get_data_from_label,get_relations
from helper.db import connector, create_table, bulk_insert_data
from helper.relations_extractor import get_neo_matrice_relations
import pandas as pd

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
        
        # First pass: Create tables with proper structure
        for table, data in labels_data.items():
            # Ensure we have an ID column
            if 'id' not in data:
                data['id'] = 'INTEGER'
            # insert all labels as table in sql
            create_table(table, data, driver_sql)
            print(f"[SUCCESS] Table created:  {table}")
        
        print("[INFO] Fin de la creation des tables")
        
        print("[INFO] Début de l'insertion des données")
        
        # Second pass: Insert data preserving IDs
        for table, data in labels_data.items():
            # get all data for each label
            labels_datas = get_data_from_label(table, session)
            print(f"[INFO] Au total {len(labels_datas)} ligne(s) à inserées")
            
            # Process data to ensure ID preservation
            row_datas = []
            for row in labels_datas:
                processed_row = label_to_row(row)
                # Ensure we have an ID
                if 'id' not in processed_row and 'neo4j_id' in processed_row:
                    processed_row['id'] = processed_row['neo4j_id']
                row_datas.append(processed_row)
            
            bulk_insert_data(driver_sql, table, row_datas, data)
            
            print(f"[SUCCESS] Données inserées dans la table :  {table}")
          
        print("[INFO] Fin de l'insertion des données")
        
        print("[INFO] Début de la creation des liens")
        
        # get matrice of relations
        matrice_relations = get_neo_matrice_relations(session, labels_data.keys())
        
        # Create foreign key relationships based on the matrix
        for source_table in matrice_relations.index:
            for target_table in matrice_relations.columns:
                relation_info = matrice_relations.loc[source_table, target_table]
                if relation_info and relation_info.get('count', 0) > 0:
                    # Get the actual relationships between these tables
                    relations = get_relations(session, source_table, target_table)
                    if relations:
                        # Check relationship type from the first relation
                        first_relation = relations[0]
                        relation_type = first_relation.get('type', '')
                        
                        # Handle different types of relationships
                        if relation_type.startswith('link_to_') and '_through_' in relation_type:
                            # This is a many-to-many relationship
                            create_junction_table(driver_sql, source_table, target_table, relation_info)
                            populate_junction_table(driver_sql, source_table, target_table, relations, relation_info)
                        else:
                            # This is a one-to-many relationship
                            create_foreign_key(driver_sql, source_table, target_table, relation_info)
                            update_foreign_keys(driver_sql, source_table, target_table, relations)
                        
                        print(f"[SUCCESS] Created relationship from {source_table} to {target_table}")
        
        print("[INFO] Fin de la creation des liens")
        
        return True

def create_junction_table(driver_sql, source_table, target_table, relation_info):
    """
    Create a junction table for many-to-many relationships.
    """
    try:
        # Extract the association table name from the relationship type
        relation_type = relation_info.get('types', [''])[0]
        if '_through_' in relation_type:
            assoc_table = relation_type.split('_through_')[1]
        else:
            assoc_table = f"{source_table}_{target_table}_junction"
            
        with driver_sql.connect() as conn:
            # Create junction table with proper constraints
            conn.execute(f"""
                CREATE TABLE {assoc_table} (
                    {source_table}_id INTEGER NOT NULL,
                    {target_table}_id INTEGER NOT NULL,
                    PRIMARY KEY ({source_table}_id, {target_table}_id),
                    FOREIGN KEY ({source_table}_id) REFERENCES {source_table}(id) ON DELETE CASCADE,
                    FOREIGN KEY ({target_table}_id) REFERENCES {target_table}(id) ON DELETE CASCADE
                )
            """)
            
            # Add relationship properties as columns with proper types
            for prop in relation_info.get('properties', []):
                # Determine SQLite type based on Neo4j property type
                sqlite_type = get_sqlite_type(prop, relation_info)
                conn.execute(f"""
                    ALTER TABLE {assoc_table}
                    ADD COLUMN {prop} {sqlite_type}
                """)
    except Exception as e:
        print(f"[ERROR] Failed to create junction table for {source_table} and {target_table}: {e}")

def populate_junction_table(driver_sql, source_table, target_table, relations, relation_info):
    """
    Populate the junction table with relationship data.
    """
    try:
        # Get the correct table name
        relation_type = relation_info.get('types', [''])[0]
        if '_through_' in relation_type:
            assoc_table = relation_type.split('_through_')[1]
        else:
            assoc_table = f"{source_table}_{target_table}_junction"
            
        with driver_sql.connect() as conn:
            for relation in relations:
                # Prepare column names and values
                columns = [f"{source_table}_id", f"{target_table}_id"]
                values = [relation.get('source_id'), relation.get('target_id')]
                
                # Add relationship properties
                for prop in relation_info.get('properties', []):
                    if prop in relation:
                        columns.append(prop)
                        values.append(relation[prop])
                
                # Insert the relationship
                placeholders = ', '.join(['?' for _ in values])
                conn.execute(f"""
                    INSERT INTO {assoc_table} ({', '.join(columns)})
                    VALUES ({placeholders})
                """, values)
    except Exception as e:
        print(f"[ERROR] Failed to populate junction table for {source_table} and {target_table}: {e}")

def create_foreign_key(driver_sql, source_table, target_table, relation_info):
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
            
            # Create the foreign key constraint with proper referential integrity
            conn.execute(f"""
                ALTER TABLE {source_table}
                ADD CONSTRAINT fk_{source_table}_{target_table}
                FOREIGN KEY ({target_table}_id)
                REFERENCES {target_table}(id) ON DELETE SET NULL
            """)
            
            # Add relationship properties as columns with proper types
            for prop in relation_info.get('properties', []):
                # Determine SQLite type based on Neo4j property type
                sqlite_type = get_sqlite_type(prop, relation_info)
                conn.execute(f"""
                    ALTER TABLE {source_table}
                    ADD COLUMN {prop} {sqlite_type}
                """)
    except Exception as e:
        print(f"[ERROR] Failed to create foreign key from {source_table} to {target_table}: {e}")

def update_foreign_keys(driver_sql, source_table, target_table, relations):
    """
    Update the foreign key values based on the Neo4j relationships.
    """
    try:
        with driver_sql.connect() as conn:
            for relation in relations:
                # Extract source and target IDs from the relationship
                source_id = relation.get('source_id')
                target_id = relation.get('target_id')
                
                if source_id and target_id:
                    # Prepare update statement with relationship properties
                    update_cols = [f"{target_table}_id = ?"]
                    values = [target_id]
                    
                    # Add relationship properties
                    for key, value in relation.items():
                        if key not in ['source_id', 'target_id', 'type']:
                            update_cols.append(f"{key} = ?")
                            values.append(value)
                    
                    # Update the foreign key value and properties
                    conn.execute(f"""
                        UPDATE {source_table}
                        SET {', '.join(update_cols)}
                        WHERE id = ?
                    """, values + [source_id])
    except Exception as e:
        print(f"[ERROR] Failed to update foreign keys from {source_table} to {target_table}: {e}")

def label_to_row(label_data):
    """
    Convert Neo4j node properties to a dictionary suitable for SQLite insertion.
    Removes internal Neo4j properties and _types metadata.
    """
    row = label_data.copy()
    # Remove internal Neo4j properties
    if '_types' in row:
        del row['_types']
    # Ensure we have an ID
    if 'neo4j_id' in row and 'id' not in row:
        row['id'] = row['neo4j_id']
    return row

def get_sqlite_type(prop_name: str, relation_info: dict) -> str:
    """
    Determine the appropriate SQLite type for a Neo4j property.
    
    Args:
        prop_name (str): Name of the property
        relation_info (dict): Relationship information containing property types
        
    Returns:
        str: SQLite type (INTEGER, REAL, TEXT, etc.)
    """
    # Get property type from relation info
    prop_type = relation_info.get('property_types', {}).get(prop_name, '')
    
    # Map Neo4j types to SQLite types
    type_mapping = {
        'int': 'INTEGER',
        'long': 'INTEGER',
        'float': 'REAL',
        'double': 'REAL',
        'boolean': 'INTEGER',
        'string': 'TEXT',
        'date': 'TEXT',
        'datetime': 'TEXT',
        'point': 'TEXT',
        'duration': 'TEXT',
        'localdatetime': 'TEXT',
        'localtime': 'TEXT',
        'time': 'TEXT'
    }
    
    # Convert Neo4j type to SQLite type
    base_type = prop_type.lower().split('[')[0]  # Handle array types
    return type_mapping.get(base_type, 'TEXT')  # Default to TEXT if type unknown