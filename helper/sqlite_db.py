import pandas as pd
from typing import Dict
from sqlalchemy import Table, Engine

# sqlite
def sqliteIsJoinTable(table: str, db_engine: Engine):
    # Récupérer les clés étrangères de la table
    fk_query = f"PRAGMA foreign_key_list({table});"
    fk_df = pd.read_sql_query(fk_query, db_engine)
    # Vérifier si la table contient au moins 2 clés étrangères
    if len(fk_df) >= 2:
        schema_query = f"PRAGMA table_info({table});"
        schema_df = pd.read_sql_query(schema_query, db_engine)
        
        # Vérifier si la table ne contient pas de clé primaire unique
        pk_columns = schema_df[schema_df['pk'] > 0]['name'].tolist()
        
        return len(pk_columns) == len(fk_df)
            
    return False

def sqlite_connector(path: str):
    """
    Établit une connexion à une base de données SQLite.
    
    :param path: Chemin du fichier de la base de données SQLite.
    :return: Tuple contenant l'objet moteur SQLAlchemy et les métadonnées.
    """
    return f"sqlite:///{path}"  # Construction de l'URI pour la connexion SQLite
  
def sqliteSchema(table: str, db_engine: Engine):
    # Utilisation de la commande PRAGMA pour récupérer les informations sur la table
    columns_df = pd.read_sql_query(f"PRAGMA table_info({table});", db_engine)
    relations_df = pd.read_sql_query(f"PRAGMA foreign_key_list({table});", db_engine)
            
    # Ajout d'un indicateur de clé étrangère aux colonnes
    if not relations_df.empty:
        relations_df = relations_df[['from', 'table', 'to']]  # Extraire les colonnes importantes
        relations_df.columns = ['column_name', 'referenced_table', 'referenced_column']
        columns_df = columns_df.merge(relations_df, left_on='name', right_on='column_name', how='left')
        
    return columns_df

def sqliteGetRelationsMatrice(tables: Dict[str, Table], db_engine: Engine) -> pd.DataFrame:
    table_names = list(tables.keys())
    matrix = pd.DataFrame('', index=table_names, columns=table_names)    

    for src_name, src_table in tables.items():
        # Relations directes par ForeignKey
        for fk in src_table.foreign_keys:
            try:
                target_name = fk.column.table.name
                ref = f"[{src_name}.{fk.parent.name}]"
                if matrix.at[src_name, target_name] == '':
                    matrix.at[src_name, target_name] = ref
                else:
                    matrix.at[src_name, target_name] += f", {ref}"
            except Exception as e:
                print(f"Erreur de lecture FK de {src_name}: {e}")

        # Détection simple de table d'association (2 FK vers 2 tables différentes)
        try:
            fk_tables = {fk.column.table.name for fk in src_table.foreign_keys}
            if len(fk_tables) == 2 and all(fk.parent.primary_key for fk in src_table.foreign_keys):
                table_a, table_b = list(fk_tables)
                for fk in src_table.foreign_keys:
                    col_table = fk.column.table.name
                    other = table_b if col_table == table_a else table_a
                    ref = f"[{src_name}.{fk.parent.name}]"
                    if matrix.at[col_table, other] == '':
                        matrix.at[col_table, other] = ref
                    else:
                        matrix.at[col_table, other] += f", {ref}"
        except Exception as e:
            print(f"Erreur dans la détection de table d'association pour {src_name}: {e}")

    return matrix
