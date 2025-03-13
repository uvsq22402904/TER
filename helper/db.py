from sqlalchemy import MetaData, create_engine, Engine
import pandas as pd
from enviroment import DATABASE_TYPE

def sqlite_connector(path: str):
    """
    Établit une connexion à une base de données SQLite.
    
    :param path: Chemin du fichier de la base de données SQLite.
    :return: Tuple contenant l'objet moteur SQLAlchemy et les métadonnées.
    """
    uri = f"sqlite:///{path}"  # Construction de l'URI pour la connexion SQLite
    return connexion(uri)  # Appelle la fonction connexion pour établir la connexion

def getTables(metadata: MetaData):
    """
    Récupère les tables d'une base de données à partir des métadonnées.
    
    :param metadata: Objet MetaData contenant les informations de la base de données.
    :return: Dictionnaire contenant les tables et leurs métadonnées.
    """
    return metadata.tables.items()

def getTableSchema(table: str, db_engine: Engine):
    """
    Obtient le schéma d'une table spécifique.
    
    :param table: Nom de la table.
    :param db_engine: Objet moteur SQLAlchemy pour exécuter la requête.
    :return: DataFrame contenant les informations sur les colonnes de la table.
    """
    match DATABASE_TYPE:
        case "sqlite":
            # Utilisation de la commande PRAGMA pour récupérer les informations sur la table
            return pd.read_sql_query(f"PRAGMA table_info({table});", db_engine)
        case "postgreSQL":
            # Requête pour PostgreSQL afin d'obtenir les colonnes et leurs informations
            return pd.read_sql_query(
                f"SELECT column_name, data_type, character_maximum_length, is_nullable "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table}';", 
                db_engine
            )
        case _:
            # Gestion d'une erreur si le moteur de base de données n'est pas reconnu
            raise ValueError(f"engineMotor {DATABASE_TYPE} does not exist !")

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
