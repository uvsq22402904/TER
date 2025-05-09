from typing import TypedDict, Dict
from .db import connector,getTables,isJoinTable,getRelationsMatrice
import pandas as pd
import numpy as np
from .Draw import drawLineInConsole
from neo4j import Session
from .neo4j_db import get_relations

"""
    Transform database from sql to neo4j.

    Args:
        uri (str): the database uri.

    Returns:
        bool: true when is correct an false otherwise.
"""
def db_relations(uri: str):
    try:
        moteur, metada = connector(uri)
        tables = getTables(metada)
        relationMatrix = getRelationsMatrice(tables, moteur)
        tables_names = list(tables.keys())
        isAssociationTables = pd.DataFrame('', index=tables_names, columns=["isAssociation"]) 
        
        for table_n, table in tables.items():
            isAssociationTables.loc[table_n, "isAssociation"] = isJoinTable(table_n, moteur)
            
        to_drops = isAssociationTables[isAssociationTables["isAssociation"] == True].index.tolist()
        matrixWithoutRelationTable = relationMatrix.drop(index = to_drops, columns=to_drops)
        
        print("Association table : ")
        drawLineInConsole(isAssociationTables)
        print("Relation matrix without association table : ")
        drawLineInConsole(matrixWithoutRelationTable)

        return matrixWithoutRelationTable
    except Exception as e:
        print(f'ERROR: ', e)
        exit(1)

class SummaryInfo(TypedDict):
    name: str
    table: str
    key: str
    type: str

def summary_relation(relations_table: pd.DataFrame) -> Dict[str, Dict[str, SummaryInfo]]:
    """
    Résume les relations entre tables à partir d'une matrice (DataFrame).

    Args:
        relations_table (pd.DataFrame): Matrice de relations entre les tables
            où chaque cellule contient "[table.colonne]" ou "[assoc_table.colonne]".

    Returns:
        Dict[str, Dict[str, SummaryInfo]]: Un dictionnaire où la clé est la table source
            et la valeur est un dictionnaire de relations vers les autres tables.
    """
    summary_rel: Dict[str, Dict[str, SummaryInfo]] = {}

    cols = relations_table.columns
    lines = relations_table.index

    for line in lines:
        relation_data: Dict[str, SummaryInfo] = {}

        for col in cols:
            cell = relations_table.at[line, col]
            if not cell:
                continue

            try:
                # Nettoyage du format [table.colonne]
                clean = cell.strip("[]").split(".")
                if len(clean) != 2:
                    continue

                table, attr = clean
                relation_data[col] = {
                    "name": f"link_to_{col}" if table == line else f"link_to_{col}_through_{table}",
                    "table": table,
                    "key": attr,
                    "type": "inner" if table == line else "join"
                }

            except Exception as e:
                print(f"Erreur de parsing pour {line} → {col} : {e}")
                continue

        summary_rel[line] = relation_data

        # Optionnel : affichage du résumé de cette table
        print(f"Relations de la table : {line}")
        drawLineInConsole(pd.DataFrame(relation_data))

    return summary_rel

def get_neo_matrice_relations(session: Session, tables: list[str]):
    """
    Create a matrix of relationships between Neo4j labels.
    
    Args:
        session (Session): Neo4j session
        tables (list[str]): List of table names/labels
        
    Returns:
        pd.DataFrame: Matrix showing relationships between tables with their types
    """
    try:
        # Initialize empty matrix with dictionaries to store relationship info
        matrix = pd.DataFrame({col: [{} for _ in tables] for col in tables}, index=tables)
        
        # For each pair of tables, check if there are relationships
        for source_table in tables:
            for target_table in tables:
                if source_table != target_table:
                    # Get relationships between these tables
                    relations = get_relations(session, source_table, target_table)
                    if relations and len(relations) > 0:
                        # Store relationship information
                        matrix.loc[source_table, target_table] = {
                            'count': len(relations),
                            'types': list(set(r.get('type', 'RELATES_TO') for r in relations)),
                            'properties': list(set(
                                prop for r in relations 
                                for prop in r.keys() 
                                if prop not in ['source_id', 'target_id', 'type']
                            ))
                        }
        
        return matrix
    except Exception as e:
        print(f"[ERROR] Failed to create relationship matrix: {e}")
        return pd.DataFrame({col: [{} for _ in tables] for col in tables}, index=tables)
    
    