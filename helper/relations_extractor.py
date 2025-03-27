from .db import connector,getTables,isJoinTable,getRelationsMatrice
import pandas as pd
from .Draw import drawLineInConsole

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
    