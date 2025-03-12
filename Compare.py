import sqlite3
import pandas as pd

def get_tables(db_path):
    """ Récupère toutes les tables d'une base de données SQLite """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return set(tables)

def get_table_structure(db_path, table_name):
    """ Récupère la structure d'une table SQLite """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    structure = [(row[1], row[2]) for row in cursor.fetchall()]  # (nom_colonne, type)
    conn.close()
    return structure

def load_table_data(db_path, table_name):
    """ Charge les données d'une table sous forme de DataFrame """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def compare_tables(db1, db2):
    """ Compare toutes les tables entre les deux bases SQLite """
    tables_db1 = get_tables(db1)
    tables_db2 = get_tables(db2)
    
    print("📌 Comparaison des tables...")
    
    # Vérifier les tables manquantes
    if tables_db1 != tables_db2:
        print(f"❌ Tables différentes :")
        print(f"Présentes uniquement dans {db1}: {tables_db1 - tables_db2}")
        print(f"Présentes uniquement dans {db2}: {tables_db2 - tables_db1}")

    common_tables = tables_db1 & tables_db2
    for table in common_tables:
        print(f"\n🔍 Comparaison de la table: {table}")

        # Comparaison des structures
        struct_db1 = get_table_structure(db1, table)
        struct_db2 = get_table_structure(db2, table)
        
        if struct_db1 != struct_db2:
            print(f"⚠️ Structure différente pour {table} :")
            print(f"{db1}: {struct_db1}")
            print(f"{db2}: {struct_db2}")
        else:
            print(f"✅ Structure identique")

        # Comparaison des données
        df1 = load_table_data(db1, table)
        df2 = load_table_data(db2, table)

        if df1.equals(df2):
            print(f"✅ Données identiques")
        else:
            print(f"❌ Données différentes")
            diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
            print(diff)

# Exemple d'utilisation
db1_path = "example.db"
db2_path = "Sortie.db"

compare_tables(db1_path, db2_path)
