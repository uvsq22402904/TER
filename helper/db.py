from sqlalchemy import MetaData, create_engine, Engine
from enviroment import DATABASE_TYPE
from .postgrey_db import postgreSchema
from .sqlite_db import sqliteIsJoinTable, sqliteSchema, sqlite_connector, sqliteGetRelationsMatrice, sqlite_get_all,sqlite_get_all_relations,sqlite_single_table_relations


# core
def connexion(uri: str):
    """
    Établit une connexion à la base de données et charge les métadonnées.
    
    :param uri: URI de la base de données.
    :return: Tuple contenant l'objet moteur SQLAlchemy et les métadonnées.
    """
    moteur = create_engine(uri)  # Création de l'objet moteur SQLAlchemy
    metadonnees = MetaData()  # Création de l'objet MetaData pour stocker les informations des tables
    metadonnees.reflect(bind=moteur)  # Chargement des métadonnées à partir de la base de données
    return moteur, metadonnees  # Retourne le moteur et les métadonnées

def connector(uri: str = "", user: str = None, pwd: str = None):
    match DATABASE_TYPE:
        case 'sqlite':
            if uri.strip() == "":
                raise ValueError(f"for {DATABASE_TYPE} engine uri can not be null or empty!")
            return connexion(sqlite_connector(uri))
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")
    
def getTables(metadata: MetaData):
    """
    Récupère les tables d'une base de données à partir des métadonnées.
    
    :param metadata: Objet MetaData contenant les informations de la base de données.
    :return: Dictionnaire contenant les tables et leurs métadonnées.
    """
    return dict(metadata.tables.items())

def getTableSchema(table: str, db_engine: Engine):
    """
    Obtient le schéma d'une table spécifique.
    
    :param table: Nom de la table.
    :param db_engine: Objet moteur SQLAlchemy pour exécuter la requête.
    :return: DataFrame contenant les informations sur les colonnes de la table.
    """
    match DATABASE_TYPE:
        case "sqlite":
            return sqliteSchema(table, db_engine)
        case "postgreSQL":
            # Requête pour PostgreSQL afin d'obtenir les colonnes et leurs informations
            return postgreSchema(table, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")

def isJoinTable(table: str, db_engine: Engine):
    match DATABASE_TYPE:
        case 'sqlite':
            return sqliteIsJoinTable(table, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")
        
def getRelationsMatrice(table: str, db_engine: Engine):
    match DATABASE_TYPE:
        case 'sqlite':
            return sqliteGetRelationsMatrice(table, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")
        
def get_all(db_engine: Engine, table: str):
    match DATABASE_TYPE:
        case 'sqlite':
            return sqlite_get_all(table, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")
        
def get_all_relations(db_engine: Engine, table1: str, table2: str):
    match DATABASE_TYPE:
        case 'sqlite':
            return sqlite_get_all_relations(table1, table2, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")
        
def get_single_table_relations(db_engine: Engine, table: str):
    match DATABASE_TYPE:
        case 'sqlite':
            return sqlite_single_table_relations(table, db_engine)
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")