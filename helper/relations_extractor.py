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
    Construit une matrice des relations entre les tables à partir de Neo4j.
    
    Args:
        session (Session): Session Neo4j active
        tables (list[str]): Liste des noms de tables
        
    Returns:
        Dict[str, Dict[str, dict]]: Dictionnaire des relations entre les tables
    """
    try:
        relations_dict = {}
        
        # Pour chaque paire de tables possible
        for source_table in tables:
            relations_dict[source_table] = {}
            
            for target_table in tables:
                if source_table == target_table:
                    continue
                    
                # Vérifier les relations dans les deux sens
                forward_query = f"""
                MATCH (s:{source_table})-[r]->(t:{target_table})
                RETURN type(r) as relation_type, count(r) as count
                """
                forward_result = session.run(forward_query)
                forward_record = forward_result.single()
                
                backward_query = f"""
                MATCH (s:{target_table})-[r]->(t:{source_table})
                RETURN type(r) as relation_type, count(r) as count
                """
                backward_result = session.run(backward_query)
                backward_record = backward_result.single()
                
                # Afficher les relations trouvées pour le débogage
                if forward_record and forward_record['count'] > 0:
                    print(f"[DEBUG] Relation trouvée {source_table} -> {target_table}: {forward_record['relation_type']} ({forward_record['count']} relations)")
                if backward_record and backward_record['count'] > 0:
                    print(f"[DEBUG] Relation trouvée {target_table} -> {source_table}: {backward_record['relation_type']} ({backward_record['count']} relations)")
                
                # Si des relations existent dans les deux sens, c'est une relation many-to-many
                if (forward_record and forward_record['count'] > 0) and (backward_record and backward_record['count'] > 0):
                    # Utiliser le type de relation réel de Neo4j
                    relation_type = forward_record['relation_type']
                    relations_dict[source_table][target_table] = {
                        "name": relation_type,  # Utiliser le type de relation réel
                        "type": "join"
                    }
                # Si une relation existe dans un seul sens, c'est une relation one-to-many
                elif forward_record and forward_record['count'] > 0:
                    relation_type = forward_record['relation_type']
                    relations_dict[source_table][target_table] = {
                        "name": relation_type,
                        "type": "inner"
                    }
                elif backward_record and backward_record['count'] > 0:
                    relation_type = backward_record['relation_type']
                    relations_dict[target_table][source_table] = {
                        "name": relation_type,
                        "type": "inner"
                    }
        
        return relations_dict
            
    except Exception as e:
        print(f"[ERROR] Échec de construction de la matrice : {e}")
        return None
    
    