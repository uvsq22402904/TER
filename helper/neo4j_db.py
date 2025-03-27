from typing import Any
from neo4j import Driver, GraphDatabase, ManagedTransaction
from neo4j.exceptions import Neo4jError
from enviroment import URI_HOST_NEO4J,URI_PORT_NEO4J,USERNAME_NEO4J,PASSWORD_NEO4J


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

        # Construction de la clause SET pour les propriétés de la relation
        props_str = ''
        if linkProperties:
            props_assignments = [f"{k}: ${k}" for k in linkProperties]
            props_str = ' {' + ', '.join(props_assignments) + '}'
        else:
            linkProperties = {}

        query = (
            f"MATCH (a:{etiquette1}), (b:{etiquette2}) "
            f"WHERE a.{key1} = $val1 AND b.{key2} = $val2 "
            f"CREATE (a)-[r:{link}{props_str}]->(b)"
        )

        transacManager.run(query, val1=val1, val2=val2, **linkProperties)

    except Neo4jError as e:
        print(f"Erreur lors de la création d'une relation {link} : {e}")
        
def load_neo() -> Driver:
    """
    Crée et retourne une instance du driver Neo4j.

    Returns:
        Driver: instance de connexion Neo4j.
    """
    try:
        uri_neo4j = f"bolt://{URI_HOST_NEO4J}:{URI_PORT_NEO4J}"
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