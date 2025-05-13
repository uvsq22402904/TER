from typing import Dict, List, Tuple
from sqlalchemy import Engine
import pandas as pd
from neo4j import GraphDatabase, Session

def extract_constraints(table_name: str, db_engine: Engine) -> Dict[str, List[Dict]]:
    """
    Extrait toutes les contraintes d'intégrité d'une table donnée en utilisant les commandes PRAGMA de SQLite.
    
    Args:
        table_name (str): Nom de la table
        db_engine (Engine): Moteur de base de données SQLAlchemy
        
    Returns:
        Dict[str, List[Dict]]: Dictionnaire contenant toutes les contraintes par type
    """
    constraints = {
        'primary_keys': [],
        'foreign_keys': [],
        'unique': [],
        'not_null': [],
        'check': [],
        'default': [],
        'collate': [],
        'autoincrement': [],
        'indexes': []
    }
    
    # 1. Informations de base de la table
    table_info = pd.read_sql_query(f"PRAGMA table_info({table_name});", db_engine)
    
    # 2. Analyse détaillée de chaque colonne
    for _, row in table_info.iterrows():
        column_name = row['name']
        column_type = row['type'].upper()
        
        # Clés primaires
        if row['pk'] > 0:
            constraints['primary_keys'].append({
                'column': column_name,
                'order': row['pk']  # Ordre dans la clé primaire composite
            })
        
        # NOT NULL
        if row['notnull'] == 1:
            constraints['not_null'].append({
                'column': column_name,
                'type': column_type
            })
        
        # DEFAULT
        if row['dflt_value'] is not None:
            constraints['default'].append({
                'column': column_name,
                'value': row['dflt_value'],
                'type': column_type
            })
        
        # AUTOINCREMENT
        if 'INTEGER' in column_type and row['pk'] > 0:
            constraints['autoincrement'].append({
                'column': column_name,
                'type': column_type
            })
        
        # COLLATE
        if 'COLLATE' in column_type:
            collation = column_type.split('COLLATE')[1].strip()
            constraints['collate'].append({
                'column': column_name,
                'collation': collation
            })
        
        # CHECK (extrait des contraintes CHECK de la définition de la colonne)
        if 'CHECK' in column_type:
            check_condition = column_type.split('CHECK')[1].strip('()')
            constraints['check'].append({
                'column': column_name,
                'condition': check_condition
            })
    
    # 3. Clés étrangères avec toutes leurs propriétés
    fk_info = pd.read_sql_query(f"PRAGMA foreign_key_list({table_name});", db_engine)
    for _, row in fk_info.iterrows():
        constraints['foreign_keys'].append({
            'column': row['from'],
            'references': f"{row['table']}.{row['to']}",
            'on_delete': row['on_delete'],
            'on_update': row['on_update'],
            'match': row['match'],
            'id': row['id'],
            'seq': row['seq']
        })
    
    # 4. Indexes (y compris les index implicites)
    index_list = pd.read_sql_query(f"PRAGMA index_list({table_name});", db_engine)
    for _, index_row in index_list.iterrows():
        index_name = index_row['name']
        index_info = pd.read_sql_query(f"PRAGMA index_info('{index_name}');", db_engine)
        
        columns = []
        for _, idx_row in index_info.iterrows():
            col_name = table_info.iloc[idx_row['cid']]['name']
            columns.append({
                'name': col_name,
                'order': idx_row['seqno']
            })
        
        constraints['indexes'].append({
            'name': index_name,
            'unique': index_row['unique'] == 1,
            'columns': columns,
            'origin': index_row['origin'],  # 'c'=CREATE, 'u'=UNIQUE, 'pk'=PRIMARY KEY
            'partial': index_row['partial'] == 1
        })
    
    return constraints

def apply_constraints_to_neo4j(constraints: Dict[str, List[Dict]], table_name: str, session: Session) -> None:
    """
    Applique les contraintes extraites aux nœuds Neo4j.
    
    Args:
        constraints (Dict[str, List[Dict]]): Dictionnaire des contraintes extraites
        table_name (str): Nom de la table/étiquette Neo4j
        session (Session): Session Neo4j
    """
    # Créer les contraintes pour les clés primaires
    for pk in constraints['primary_keys']:
        query = f"""
        CREATE CONSTRAINT {table_name}_pk_{pk['column']} IF NOT EXISTS
        FOR (n:{table_name})
        REQUIRE n.{pk['column']} IS UNIQUE
        """
        session.run(query)
    
    # Créer les contraintes pour les clés étrangères
    for fk in constraints['foreign_keys']:
        ref_table, ref_column = fk['references'].split('.')
        # Contrainte NOT NULL
        query = f"""
        CREATE CONSTRAINT {table_name}_fk_{fk['column']} IF NOT EXISTS
        FOR (n:{table_name})
        REQUIRE n.{fk['column']} IS NOT NULL
        """
        session.run(query)
        
        # Contrainte de relation - uniquement si la relation n'est pas many-to-many
        if not any(index['unique'] for index in constraints['indexes'] if fk['column'] in [col['name'] for col in index['columns']]):
            query = f"""
            CREATE CONSTRAINT {table_name}_rel_{fk['column']} IF NOT EXISTS
            FOR ()-[r:RELATES_TO]-()
            WHERE r.{fk['column']} IS NOT NULL
            """
            session.run(query)
    
    # Créer les contraintes pour les colonnes NOT NULL
    for nn in constraints['not_null']:
        query = f"""
        CREATE CONSTRAINT {table_name}_nn_{nn['column']} IF NOT EXISTS
        FOR (n:{table_name})
        REQUIRE n.{nn['column']} IS NOT NULL
        """
        session.run(query)
    
    # Créer les contraintes pour les colonnes UNIQUE
    for unique in constraints['unique']:
        columns = unique['columns']
        constraint_name = f"{table_name}_unique_{'_'.join(columns)}"
        query = f"""
        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
        FOR (n:{table_name})
        REQUIRE ({', '.join([f'n.{col}' for col in columns])}) IS UNIQUE
        """
        session.run(query)
    
    # Créer les contraintes CHECK
    for check in constraints['check']:
        query = f"""
        CREATE CONSTRAINT {table_name}_check_{check['name']} IF NOT EXISTS
        FOR (n:{table_name})
        REQUIRE n.{check['condition']}
        """
        session.run(query)
    
    # Créer des index pour améliorer les performances
    for index in constraints['indexes']:
        if index['unique']:
            columns = [col['name'] for col in index['columns']]
            query = f"""
            CREATE INDEX {table_name}_idx_{'_'.join(columns)} IF NOT EXISTS
            FOR (n:{table_name})
            ON ({', '.join([f'n.{col}' for col in columns])})
            """
            session.run(query)
    
    # Les contraintes suivantes Neo4j ne les supporte pas directement :
    # - DEFAULT
    # - COLLATE
    # - AUTOINCREMENT
    # - TRIGGERS
    # - VIEWS
    # - TEMPORARY
    # - WITHOUT ROWID 