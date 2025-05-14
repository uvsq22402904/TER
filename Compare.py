import sqlite3
import pandas as pd

def get_tables(db_path):
    """ Récupère toutes les tables d'une base de données SQLite """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()
    return tables

def get_table_structure(conn, table_name):
    """ Récupère la structure d'une table SQLite """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    structure = [(row[1], row[2]) for row in cursor.fetchall()]  # (nom_colonne, type)
    return structure

def load_table_data(conn, table_name):
    """Charge les données d'une table depuis la base de données."""
    try:
        # Vérifier si c'est une table d'association
        if "association" in table_name:
            # Pour les tables d'association, on trie par les deux colonnes de clé étrangère
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            # Prendre les deux premières colonnes qui sont les clés étrangères
            if len(columns) >= 2:
                order_by = f"ORDER BY {columns[0]}, {columns[1]}"
            else:
                order_by = ""
            df = pd.read_sql_query(f"SELECT * FROM {table_name} {order_by}", conn)
        else:
            # Pour les tables normales, on trie par id
            df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", conn)
        return df
    except Exception as e:
        print(f"Erreur lors du chargement des données de la table {table_name}: {e}")
        return None

def compare_tables(db1_path, db2_path):
    """Compare les tables entre deux bases de données."""
    try:
        # Connexion aux deux bases de données
        conn1 = sqlite3.connect(db1_path)
        conn2 = sqlite3.connect(db2_path)
        
        # Récupérer la liste des tables
        cursor1 = conn1.cursor()
        cursor1.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor1.fetchall()]
        
        for table in tables:
            print("=" * 60)
            print(f"===================== TABLE : {table} =====================")
            print("=" * 60)
            
            # Comparer la structure
            structure1 = get_table_structure(conn1, table)
            structure2 = get_table_structure(conn2, table)
            
            if structure1 == structure2:
                print("✔ Structure identique.")
            else:
                print("❌ Structure différente.")
                print("\nStructure dans la première base :")
                print(structure1)
                print("\nStructure dans la deuxième base :")
                print(structure2)
            
            # Comparer les données
            df1 = load_table_data(conn1, table)
            df2 = load_table_data(conn2, table)
            
            if df1 is not None and df2 is not None:
                if df1.equals(df2):
                    print("✔ Données identiques.")
                else:
                    print("❌ Données différentes.")
                    print("\nDifférences trouvées :")
                    print(pd.concat([df1, df2]).drop_duplicates(keep=False))
            else:
                print("❌ Erreur lors de la comparaison des données.")
            
            print()
        
        # Fermer les connexions
        conn1.close()
        conn2.close()
        
    except Exception as e:
        print(f"Erreur lors de la comparaison des bases de données : {e}")
        if 'conn1' in locals():
            conn1.close()
        if 'conn2' in locals():
            conn2.close()

# Exemple d'utilisation
db1_path = "data/example.db"
db2_path = "data/output.db"

compare_tables(db1_path, db2_path)