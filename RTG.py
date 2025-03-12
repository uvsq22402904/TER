from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
import pandas as pd

# Configuration des bases de données
def configurer_sqlalchemy(uri_base_donnees):
    try:
        moteur = create_engine(uri_base_donnees)
        metadonnees = MetaData()
        metadonnees.reflect(bind=moteur)
        print("Connexion réussie à la base relationnelle !")
        return moteur, metadonnees
    except SQLAlchemyError as e:
        print(f"Erreur de connexion à la base relationnelle : {e}")
        exit(1)

def configurer_neo4j(uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j):
    try:
        driver = GraphDatabase.driver(uri_neo4j, auth=(utilisateur_neo4j, mot_de_passe_neo4j))
        with driver.session() as session:
            session.run("RETURN 1")  # Test de connexion
        print("Connexion réussie à Neo4j !")
        return driver
    except neo4j_exceptions.Neo4jError as e:
        print(f"Erreur de connexion à Neo4j : {e}")
        exit(1)

def nettoyer_base_neo4j(driver):
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Base Neo4j nettoyée !")
    except neo4j_exceptions.Neo4jError as e:
        print(f"Erreur lors du nettoyage de la base Neo4j : {e}")

def recuperer_donnees_table(moteur, nom_table):
    try:
        return pd.read_sql_query(f"SELECT * FROM {nom_table}", moteur)
    except SQLAlchemyError as e:
        print(f"Erreur lors de la récupération des données de {nom_table} : {e}")
        return pd.DataFrame()

def creer_noeud(tx, etiquette, proprietes):
    try:
        requete = f"CREATE (n:{etiquette} $props)"
        tx.run(requete, props=proprietes)
    except neo4j_exceptions.Neo4jError as e:
        print(f"Erreur lors de la création d'un nœud {etiquette} : {e}")


def inserer_noeuds(driver, metadonnees, moteur):
    with driver.session() as session:
        for nom_table, table in metadonnees.tables.items():
            df = recuperer_donnees_table(moteur, nom_table)
            
            # Récupérer les types des colonnes
            types_colonnes = {col.name: str(col.type) for col in table.columns}
            
            for _, ligne in df.iterrows():
                proprietes = ligne.to_dict()
                proprietes["_types"] = [f"{k}:{v}" for k, v in types_colonnes.items()]
                session.write_transaction(creer_noeud, nom_table, proprietes)


def creer_relation(tx, etiquette1, prop1, etiquette2, prop2, type_relation):
    try:
        requete = (
            f"MATCH (a:{etiquette1}), (b:{etiquette2}) "  
            f"WHERE a.{list(prop1.keys())[0]} = $val1 AND b.{list(prop2.keys())[0]} = $val2 "
            f"CREATE (a)-[r:{type_relation}]->(b)"
        )
        tx.run(requete, val1=list(prop1.values())[0], val2=list(prop2.values())[0])
    except neo4j_exceptions.Neo4jError as e:
        print(f"Erreur lors de la création d'une relation {type_relation} : {e}")



def inserer_relations(driver, metadonnees, moteur):
    with driver.session() as session:
        for table in metadonnees.tables.values():
            for fk in table.foreign_keys:
                # Récupérer les informations de la clé étrangère
                table_parent = fk.column.table.name  # Table cible (parent de la relation)
                table_enfant = table.name  # Table source (enfant de la relation)
                parent_column = fk.column.name  # Nom de la colonne parent dans la table cible
                child_column = fk.parent.name  # Nom de la colonne enfant dans la table source
                
                # Type de relation dans Neo4j
                type_relation = f"RELIE_A_{table_parent.upper()}"

                # Requête SQL pour récupérer les données
                requete = f"""
                SELECT c.*, p.{parent_column} AS {parent_column}
                FROM {table_enfant} c
                JOIN {table_parent} p ON c.{child_column} = p.{parent_column}
                """
                df_rel = recuperer_donnees_table(moteur, table_enfant)
                
                # Création des relations dans Neo4j
                for _, ligne in df_rel.iterrows():
                    session.write_transaction(
                        creer_relation,
                        table_enfant, {child_column: ligne[child_column]},  # Colonne de la table enfant
                        table_parent, {parent_column: ligne[parent_column]},  # Colonne de la table parent
                        type_relation
                    )

def transformer_relationnel_en_graphe(uri_base_donnees, uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j):
    moteur, metadonnees = configurer_sqlalchemy(uri_base_donnees)
    driver = configurer_neo4j(uri_neo4j, utilisateur_neo4j, mot_de_passe_neo4j)
    
    nettoyer_base_neo4j(driver)
    inserer_noeuds(driver, metadonnees, moteur)
    inserer_relations(driver, metadonnees, moteur)
    
    driver.close()
    print("Transformation terminée !")

if __name__ == "__main__":
    URI_BASE_DONNEES = 'sqlite:///example.db'  
    URI_NEO4J = "bolt://localhost:7687"
    UTILISATEUR_NEO4J = "neo4j"
    MOT_DE_PASSE_NEO4J = "password"
    
    transformer_relationnel_en_graphe(URI_BASE_DONNEES, URI_NEO4J, UTILISATEUR_NEO4J, MOT_DE_PASSE_NEO4J)
