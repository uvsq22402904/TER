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
    
def est_table_jointure(table):
    # Vérifie si la table a au moins deux clés étrangères
    fk_count = len(table.foreign_keys)
    col_count = len(table.columns)
    return fk_count >= 2 and col_count == fk_count


def creer_noeud(tx, etiquette, proprietes, table):
    try:
        # Avant de créer un nœud, vérifie si c'est une table de jointure
        if est_table_jointure(table):
            # Ajouter '_association' au nom de la table de jointure
            etiquette_association = f"{etiquette}_association"
            print(f"{etiquette} est une table de jointure. Renommage en {etiquette_association}.")
            # Créer le nœud avec le nouveau nom
            requete = f"CREATE (n:{etiquette_association} $props)"
            tx.run(requete, props=proprietes)
        else:
            # Si ce n'est pas une table de jointure, créer le nœud avec le nom original
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
                session.write_transaction(creer_noeud, nom_table, proprietes, table)


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

def many_to_many(metadonnees, moteur, session):
    for table in metadonnees.tables.values():
            if est_table_jointure(table):
                fk_list = list(table.foreign_keys)
                if len(fk_list) >= 2:
                    fk1, fk2 = fk_list[0], fk_list[1]

                    table1 = fk1.column.table.name
                    col1 = fk1.parent.name
                    col1_ref = fk1.column.name

                    table2 = fk2.column.table.name
                    col2 = fk2.parent.name
                    col2_ref = fk2.column.name

                    df = recuperer_donnees_table(moteur, table.name)
                    for _, ligne in df.iterrows():
                        session.write_transaction(
                            creer_relation,
                            table1, {col1_ref: ligne[col1]},
                            table2, {col2_ref: ligne[col2]},
                            f"{table1.upper()}_A_{table2.upper()}"
                        )

def one_to_many(metadonnees, moteur, session):
    for table in metadonnees.tables.values():
            if est_table_jointure(table):
                continue  # ⛔ Évite de retravailler une table de jointure

            for fk in table.foreign_keys:
                table_parent = fk.column.table.name
                table_enfant = table.name
                parent_column = fk.column.name
                child_column = fk.parent.name

                type_relation = f"RELIE_A_{table_parent.upper()}"
                df_rel = recuperer_donnees_table(moteur, table_enfant)

                for _, ligne in df_rel.iterrows():
                    session.write_transaction(
                        creer_relation,
                        table_enfant, {child_column: ligne[child_column]},
                        table_parent, {parent_column: ligne[parent_column]},
                        type_relation
                    )



def inserer_relations(driver, metadonnees, moteur):
    with driver.session() as session:

        # 🔁 1. Relations Many-to-Many (tables de jointure)
        many_to_many(metadonnees, moteur, session)

        # 🔁 2. Relations One-to-Many (hors tables de jointure)
        one_to_many(metadonnees, moteur, session)

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
