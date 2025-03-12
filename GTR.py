from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
import pandas as pd
import os

def configurer_sqlalchemy(uri_base_donnees):
    moteur = create_engine(uri_base_donnees)
    metadonnees = MetaData()  # Correction : suppression de bind
    return moteur, metadonnees

def configurer_neo4j(uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j):
    driver = GraphDatabase.driver(uri_neo4j, auth=(utilisateur_neo4j, mot_de_passe_neo4j))
    return driver

def recuperer_noeuds(driver):
    with driver.session() as session:
        requete = """
        MATCH (n) 
        RETURN DISTINCT labels(n)[0] AS table, 
                        collect(properties(n)) AS donnees
        """
        result = session.run(requete)

        donnees = {}
        types_colonnes = {}

        for record in result:
            table = record["table"]
            lignes = record["donnees"]

            if lignes:
                # Récupérer les types si disponibles
                exemple_ligne = lignes[0]
                if "_types" in exemple_ligne:
                    types_colonnes[table] = {entry.split(":")[0]: entry.split(":")[1] for entry in exemple_ligne["_types"]}

                    for ligne in lignes:
                        ligne.pop("_types", None)  # Supprimer _types des données

            donnees[table] = lignes

        return donnees, types_colonnes


def recuperer_relations(driver):
    with driver.session() as session:
        requete = """
        MATCH (a)-[r]->(b)
        RETURN DISTINCT labels(a)[0] AS table_source, properties(a) AS prop_source,
                        labels(b)[0] AS table_cible, properties(b) AS prop_cible,
                        type(r) AS relation
        """
        result = session.run(requete)
        return [(record["table_source"], record["prop_source"],
                 record["table_cible"], record["prop_cible"],
                 record["relation"]) for record in result]

def convertir_type(sql_type):
    """Convertit un type SQL (string) en SQLAlchemy"""
    if "INTEGER" in sql_type:
        return Integer
    elif "VARCHAR" in sql_type or "TEXT" in sql_type or "CHAR" in sql_type:
        return String
    else:
        return String  # Type par défaut si inconnu

def creer_tables(moteur, metadonnees, donnees, relations, types_colonnes):
    tables = {}

    # Création des tables (sans FK)
    for table, lignes in donnees.items():
        colonnes = [Column("id", Integer, primary_key=True)]
        
        if lignes and table in types_colonnes:
            for cle, type_sql in types_colonnes[table].items():
                if cle != "id":
                    colonnes.append(Column(cle, convertir_type(type_sql)))

        tables[table] = Table(table, metadonnees, *colonnes)

    # Ajout des FK
    for table_source, prop_source, table_cible, prop_cible, relation in relations:
        if "id" in prop_source and "id" in prop_cible:
            if table_source in tables and table_cible in tables:
                fk_col = Column(f"fk_{table_cible}", Integer, ForeignKey(f"{table_cible}.id"))
                if fk_col.name not in tables[table_source].columns:
                    tables[table_source].append_column(fk_col)

    metadonnees.create_all(moteur)  # Création des tables en base
    return tables


def inserer_donnees(moteur, tables, donnees):
    with moteur.connect() as connexion:
        for table, lignes in donnees.items():
            if lignes:
                df = pd.DataFrame(lignes)
                df.to_sql(table, moteur, if_exists='append', index=False)



def transformer_graphe_en_relationnel(uri_base_donnees, uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j):

    # Supprimer la base de données SQLite si elle existe
    db_path = "Sortie.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"{db_path} supprimé.")

    moteur, metadonnees = configurer_sqlalchemy(uri_base_donnees)


    

    driver = configurer_neo4j(uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j)
    
    donnees, types_colonnes = recuperer_noeuds(driver)
    relations = recuperer_relations(driver)
    
    tables = creer_tables(moteur, metadonnees, donnees, relations, types_colonnes)
    inserer_donnees(moteur, tables, donnees)
    
    driver.close()
    print("Transformation terminée !")


if __name__ == "__main__":
    URI_BASE_DONNEES = 'sqlite:///Sortie.db'  
    URI_NEO4J = "bolt://localhost:7687"
    UTILISATEUR_NEO4J = "neo4j"
    MOT_DE_PASSE_NEO4J = "password"
    
    transformer_graphe_en_relationnel(URI_BASE_DONNEES, URI_NEO4J, UTILISATEUR_NEO4J, MOT_DE_PASSE_NEO4J)
