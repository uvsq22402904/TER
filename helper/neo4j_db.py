from typing import Any, Dict
from neo4j import Driver, GraphDatabase, ManagedTransaction,Session
from neo4j.exceptions import Neo4jError
from enviroment import URI_HOST_NEO4J,URI_PORT_NEO4J,USERNAME_NEO4J,PASSWORD_NEO4J,URI_AGENT_NEO4J
from .db import get_all
import pandas as pd
import json
import re


def is_noeud_exist(transacManager: ManagedTransaction, etiquette1: str, prop1: dict) -> bool:
    """
    Vérifie si un nœud avec une certaine étiquette et une propriété donnée existe dans la base Neo4j.

    Args:
        transacManager (ManagedTransaction): Transaction Neo4j active
        etiquette1 (str): Étiquette du nœud à rechercher (ex: "Person")
        prop1 (dict): Dictionnaire avec une propriété unique pour identifier le nœud (ex: {"id": 1})

    Returns:
        bool: True si le nœud existe, False sinon
    """
    try:
        key1, val1 = next(iter(prop1.items()))

        query = (
            f"MATCH (a:{etiquette1}) "
            f"WHERE a.{key1} = $val1 "
            f"RETURN COUNT(a) AS count"
        )

        result = transacManager.run(query, val1=val1)
        count = result.single()["count"]
        return count > 0

    except Neo4jError as e:
        print(f"Erreur lors de la vérification de l'existence de {etiquette1} : {e}")
        return False
    
def create_noeud(transacManager: ManagedTransaction, etiquette: str, properties: dict[Any, Any]) -> None:
    """
    Crée un nœud dans la base Neo4j avec une étiquette et un ensemble de propriétés.

    Args:
        transacManager (ManagedTransaction): Transaction Neo4j active.
        etiquette (str): L'étiquette du nœud à créer (ex: "Person").
        properties (dict[Any, Any]): Dictionnaire de propriétés du nœud (ex: {"id": 1, "nom": "Alice"}).
    """
    try:
        if not properties:
            print(f"Avertissement : tentative de création d'un nœud {etiquette} sans propriétés.")

        query = f"CREATE (n:{etiquette} $props)"
        transacManager.run(query, props=properties)

    except Neo4jError as e:
        print(f"Erreur lors de la création d'un nœud {etiquette} : {e}")
    
def has_link(transacManager: ManagedTransaction, etiquette1: str, prop1: dict, etiquette2: str, prop2: dict, link: str):
    """
        Vérifie si une relation de type `link` existe entre deux nœuds.

        Args:
            transacManager: Transaction active Neo4j
            etiquette1 (str): Étiquette du premier nœud (source)
            prop1 (dict): Clé-valeur identifiant le premier nœud (ex: {'id': 1})
            etiquette2 (str): Étiquette du second nœud (cible)
            prop2 (dict): Clé-valeur identifiant le second nœud (ex: {'id': 42})
            link (str): Type de relation à vérifier (ex: 'WORKS_FOR')

        Returns:
            bool: True si la relation existe, False sinon
    """
    try:
        key1, val1 = next(iter(prop1.items()))
        key2, val2 = next(iter(prop2.items()))

        query = (
            f"MATCH (a:{etiquette1})-[r:{link}]->(b:{etiquette2}) "
            f"WHERE a.{key1} = $val1 AND b.{key2} = $val2 "
            f"RETURN COUNT(r) AS count"
        )

        result = transacManager.run(query, val1=val1, val2=val2)
        count = result.single()["count"]
        return count > 0

    except Neo4jError as e:
        print(f"Erreur lors de la vérification du lien {link} : {e}")
        return False
    
def add_link(transacManager: ManagedTransaction, etiquette1: str, prop1: dict, etiquette2: str, prop2: dict, link: str, linkProperties: dict = None):
    """
    Crée un lien entre deux nœuds identifiés par leurs propriétés.

    Args:
        transacManager: Transaction Neo4j
        etiquette1 (str): étiquette du nœud source (ex: 'Person')
        prop1 (dict): propriété unique du nœud source (ex: {'id': 1})
        etiquette2 (str): étiquette du nœud cible (ex: 'Company')
        prop2 (dict): propriété unique du nœud cible (ex: {'id': 42})
        link (str): nom de la relation (ex: 'WORKS_FOR')
        linkProperties (dict): propriétés à ajouter à la relation (facultatif)
    """
    try:
        key1, val1 = next(iter(prop1.items()))
        key2, val2 = next(iter(prop2.items()))

        query = (
            f"MATCH (a:{etiquette1}), (b:{etiquette2}) "
            f"WHERE a.{key1} = $val1 AND b.{key2} = $val2 "
            f"MERGE (a)-[r:{link}]->(b) "
        )

        params = {"val1": val1, "val2": val2}

        # Ajout dynamique des propriétés de la relation
        if linkProperties is not None:
            set_clauses = [f"r.{k} = ${k}" for k in linkProperties.keys()]
            query += "SET " + ", ".join(set_clauses)
            params.update(linkProperties)

        transacManager.run(query, **params)

    except Neo4jError as e:
        print(f"Erreur lors de la création d'une relation {link} : {e}")
        
def load_neo() -> Driver:
    """
    Crée et retourne une instance du driver Neo4j.

    Returns:
        Driver: instance de connexion Neo4j.
    """
    try:
        uri_neo4j = f"{URI_AGENT_NEO4J}://{URI_HOST_NEO4J}:{URI_PORT_NEO4J}"
        driver = GraphDatabase.driver(uri_neo4j, auth=(USERNAME_NEO4J, PASSWORD_NEO4J))
        return driver
    except Neo4jError as e:
        print(f"Erreur de connexion à Neo4j : {e}")
        exit(1)

def erase_neo_db(driver: Driver) -> None:
    """
    Supprime tous les nœuds et relations de la base de données Neo4j.

    Args:
        driver (Driver): instance du driver Neo4j.
    """
    try:
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
    except Neo4jError as e:
        print(f"Erreur lors du nettoyage de la base Neo4j : {e}")
        
def insert_noeud_from_table(table_name: str, table_struct, neo_session: Session, db_engine: Driver) -> bool:
    """
    Insère les données d'une table SQL dans Neo4j sous forme de nœuds.

    Args:
        table_name (str): Nom de la table SQL.
        table_struct (Table): Structure SQLAlchemy de la table (pour les types).
        neo_session (Session): Session active Neo4j.
        db_engine (Engine): Moteur de base de données SQL.

    Returns:
        bool: True si l'import a réussi, False sinon.
    """
    try:
        print(f"[INFO] Lecture des données de la table : {table_name}")
        table_datas: pd.DataFrame = get_all(db_engine, table_name)

        if table_datas.empty:
            print(f"[INFO] Aucune donnée à migrer depuis {table_name}.")
            return True

        # Récupération des types de colonnes depuis SQLAlchemy
        table_colums_type: Dict[str, str] = {
            col.name: str(col.type) for col in table_struct.columns
        }

        print(f"[INFO] Début de la migration vers Neo4j : {len(table_datas)} lignes à insérer...")

        for _, row in table_datas.iterrows():
            properties = row.to_dict()
            # Ajouter les types de colonnes comme méta-infos (facultatif)
            properties["_types"] = [f"{k}:{v}" for k, v in table_colums_type.items()]
            
            # Écriture transactionnelle dans Neo4j
            neo_session.execute_write(create_noeud, table_name, properties)

        print(f"[SUCCESS] Migration terminée pour la table {table_name}.")
        return True

    except Exception as e:
        print(f"[ERROR] Échec de l'import depuis {table_name} : {e}")
        return False

def get_relations(neo_session: Session, label1: str, label2: str):
    try:
        query = f"MATCH (n:`{label1}`) - [r] -> (m:{label2}) RETURN r"
        result = neo_session.run(query)
        datas = [dict(record["r"]._properties) for record in result]
        
            
        return datas

    except Exception as e:
        print(f"[ERROR] Échec de l'import des relation entre depuis {label1} et {label2}: {e}")
        return pd.DataFrame()

def clean_label(raw_label: str) -> str:
    match = re.search(r"`(.+?)`", raw_label)
    return match.group(1) if match else raw_label



def get_all_etiquette(neo_session: Session):
    """
    Scans all labels in the Neo4j database and extracts property types from the `_types` field in each node.

    Returns:
        A dictionary like:
        {
            "employe": {
                "id": "INTEGER",
                "nom": "VARCHAR",
                "date_embauche": "DATE"
            },
            ...
        }
    """
    
    try:
        label_types: Dict[str, Dict[str, str]] = {}

        # Step 1: Get all labels
        result = neo_session.run("CALL db.labels() YIELD label RETURN label")
        labels = [record["label"] for record in result]

        # Step 2: For each label, sample one node and read _types
        for label in labels:
            query = f"MATCH (n:`{label}`) RETURN n._types AS types LIMIT 1"
            result = neo_session.run(query)
            record = result.single()

            if record and record.get("types"):
                type_entries = record["types"]
                label_types[label] = {}

                for entry in type_entries:
                    if ":" in entry:
                        key, type_str = entry.split(":", 1)
                        label_types[label][key.strip()] = type_str.strip()
        return label_types
    except Exception as e:
        print(f"[ERROR] Échec de la recuperation des etiquettes : {e}")
        return False
    

def get_relation_matrice(labels: list[str], neo_session: Session):
    try:

            
        return True
    except Exception as e:
        print(f"[ERROR] Échec de construction de la matrice : {e}")
        return False
    
    
def get_data_from_label(label: str, neo_session: Session):
    try:
        result = neo_session.run(f"MATCH (n: {label}) RETURN n;")
        
        datas = [dict(record["n"]._properties) for record in result]
            
        return datas
    except Exception as e:
        print(f"[ERROR] Échec de la recuperation des données depuis {label} : {e}")
        return False
    
    
    