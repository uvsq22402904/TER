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
    print("=" * 60)
    print(f" COMPARAISON DES BASES DE DONNÉES ".center(60, "="))
    print("=" * 60)

    tables_db1 = get_tables(db1)
    tables_db2 = get_tables(db2)

    # Vérifier les tables manquantes
    if tables_db1 != tables_db2:
        print("\nTables présentes uniquement dans chaque base :")
        print("-" * 60)
        print(f"{db1} : {tables_db1 - tables_db2}" if tables_db1 - tables_db2 else f"{db1} : Aucune table unique.")
        print(f"{db2} : {tables_db2 - tables_db1}" if tables_db2 - tables_db1 else f"{db2} : Aucune table unique.")
        print("-" * 60)

    common_tables = tables_db1 & tables_db2
    for table in sorted(common_tables):
        print("\n" + "=" * 60)
        print(f" TABLE : {table} ".center(60, "="))
        print("=" * 60)

        # Comparaison des structures
        struct_db1 = get_table_structure(db1, table)
        struct_db2 = get_table_structure(db2, table)
        
        if struct_db1 != struct_db2:
            print("\nStructure différente :")
            print("-" * 60)
            print(f"{db1}: {struct_db1}")
            print(f"{db2}: {struct_db2}")
            print("-" * 60)
        else:
            print("✔ Structure identique.")

        # Comparaison des données
        df1 = load_table_data(db1, table)
        df2 = load_table_data(db2, table)

        if df1.equals(df2):
            print("✔ Données identiques.")
        else:
            print("\n⚠ Données différentes :")
            print("-" * 60)
            diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
            print(diff.to_string(index=False))  # Affichage plus lisible des différences
            print("-" * 60)

# Exemple d'utilisation
db1_path = "data/example.db"
db2_path = "Sortie.db"

compare_tables(db1_path, db2_path)