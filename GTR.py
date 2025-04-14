from sqlalchemy import PrimaryKeyConstraint, create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy import Integer, BigInteger, SmallInteger, Float, Numeric, Boolean
from sqlalchemy import String, Date, DateTime, LargeBinary
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
import pandas as pd
import os
from sqlalchemy import Date  # Import du type Date

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
    """Convertit un type SQL en SQLAlchemy"""
    sql_type = sql_type.upper()  # Normalisation en majuscules

    if "INTEGER" in sql_type:
        return Integer
    elif "BIGINT" in sql_type:
        return BigInteger
    elif "SMALLINT" in sql_type:
        return SmallInteger
    elif "REAL" in sql_type or "FLOAT" in sql_type or "DOUBLE" in sql_type:
        return Float
    elif "DECIMAL" in sql_type or "NUMERIC" in sql_type:
        return Numeric
    elif "BOOLEAN" in sql_type:
        return Boolean
    elif "VARCHAR" in sql_type or "TEXT" in sql_type or "CHAR" in sql_type:
        return String
    elif "DATE" == sql_type:  # Exact match pour éviter conflit avec DATETIME
        return Date
    elif "DATETIME" in sql_type or "TIMESTAMP" in sql_type:
        return DateTime
    elif "BLOB" in sql_type:
        return LargeBinary
    else:
        print(f"⚠ Type SQL non reconnu : {sql_type} → String utilisé par défaut.")
        return String  # Sécurité : utilise String si inconnu


def creer_tables(moteur, metadonnees, donnees, relations, types_colonnes):
    tables = {}

    # 1. Création des tables de base avec leurs colonnes
    for table, lignes in donnees.items():
        colonnes = []
        is_association_table = table.endswith("_association")

        if not is_association_table:
            print(f"Table {table} n'est pas une table d'association. Ajout de la colonne 'id'.")
            if table not in types_colonnes or 'id' not in types_colonnes[table]:
                colonnes.append(Column("id", Integer, primary_key=True))
        else:
            print(f"Table {table} est une table d'association many-to-many. Pas de colonne 'id' ajoutée.")

        if lignes and table in types_colonnes:
            for cle, type_sql in types_colonnes[table].items():
                if cle == 'id' and not is_association_table:
                    colonnes.append(Column(cle, convertir_type(type_sql), primary_key=True))
                elif cle != 'id':
                    colonnes.append(Column(cle, convertir_type(type_sql)))

        tables[table] = Table(table, metadonnees, *colonnes)

    # 2. Création des tables d'association from relations
    tables_dassociation = {}
    for table_source, prop_source, table_cible, prop_cible, relation in relations:
        if table_source not in tables or table_cible not in tables:
            continue

        if relation == "many-to-many" or table_source.endswith("_association") or table_cible.endswith("_association"):
            nom_table_association = f"{table_source}_{table_cible}"
            if nom_table_association not in tables_dassociation and nom_table_association not in tables:
                tables_dassociation[nom_table_association] = Table(
                    nom_table_association, metadonnees,
                    Column(f"{table_source}_id", Integer, ForeignKey(f"{table_source}.id"), primary_key=True),
                    Column(f"{table_cible}_id", Integer, ForeignKey(f"{table_cible}.id"), primary_key=True),
                    PrimaryKeyConstraint(f"{table_source}_id", f"{table_cible}_id")
                )
                print(f"Table d'association pour {table_source} et {table_cible} créée sous le nom {nom_table_association}.")

    # 3. Renommage des tables *_association
    tables_renamed = {}
    for name, table in list(tables.items()):
        if name.endswith('_association'):
            new_name = name.removesuffix('_association')
            table.name = new_name
            tables_renamed[new_name] = table
        else:
            tables_renamed[name] = table

    tables = tables_renamed

    # 4. Création effective des tables (seulement maintenant)
    metadonnees.create_all(moteur)

    # 5. Création des tables d'association many-to-many
    for table_association in tables_dassociation.values():
        if table_association.name not in tables:
            table_association.create(moteur)

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
