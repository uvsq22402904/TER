from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker


DATABASE_URI = 'sqlite:///Sortie.db'  
#DATABASE_URI = 'sqlite:///example.db' 

#Connexion et récupération des métadonnées
engine = create_engine(DATABASE_URI)
metadata = MetaData()
metadata.reflect(bind=engine)

#Création d'une session
Session = sessionmaker(bind=engine)
session = Session()

def afficher_donnees():
    """Affiche les données de toutes les tables de la base."""
    for table_name, table in metadata.tables.items():
        print(f"\n📌 Données de la table {table_name}:")
        # Exécuter une requête pour récupérer toutes les lignes de la table
        with engine.connect() as connection:
            result = connection.execute(table.select())
            rows = result.fetchall()
            if rows:
                for row in rows:
                    print(dict(zip(result.keys(), row)))
            else:
                print("⚠️ Aucune donnée trouvée.")

def afficher_relations():
    """Affiche les relations entre les tables via les clés étrangères."""
    print("\n🔗 Relations entre les tables:")
    for table in metadata.tables.values():
        for fk in table.foreign_keys:
            print(f"🔄 {table.name}.{fk.parent.name} → {fk.column.table.name}.{fk.column.name}")

#Exécuter l'affichage
afficher_donnees()
afficher_relations()

#Fermer la session
session.close()
