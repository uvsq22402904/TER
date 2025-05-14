import sqlite3
from neo4j import GraphDatabase
from models.m_sqlite import m_sqlite
import pandas as pd
import os
import argparse
from datetime import datetime
import difflib
import sys

class SQLiteComparator:
    def __init__(self, db1_path, db2_path, output_format="console", output_file=None):
        """
        Initialise le comparateur de bases de données SQLite
        
        Args:
            db1_path: Chemin vers la première base de données
            db2_path: Chemin vers la deuxième base de données
            output_format: Format de sortie ("console", "html", "csv")
            output_file: Fichier de sortie pour HTML ou CSV
        """
        self.db1_path = db1_path
        self.db2_path = db2_path
        self.output_format = output_format
        self.output_file = output_file
        self.results = {
            "tables_only_in_db1": [],
            "tables_only_in_db2": [],
            "identical_tables": [],
            "different_tables": {},
            "summary": {}
        }

    def get_tables(self, db_path):
        """Récupère toutes les tables d'une base de données SQLite"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        return tables

    def get_table_structure(self, conn, table_name):
        """Récupère la structure d'une table SQLite"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        structure = [(row[1], row[2], row[3], row[5]) for row in cursor.fetchall()]  # (nom_colonne, type, not_null, pk)
        return structure

    def get_indices(self, conn, table_name):
        """Récupère les indices d'une table"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA index_list({table_name});")
        indices = []
        for idx_info in cursor.fetchall():
            idx_name = idx_info[1]
            cursor.execute(f"PRAGMA index_info({idx_name});")
            columns = [col_info[2] for col_info in cursor.fetchall()]
            indices.append((idx_name, columns, idx_info[2] == 1))  # (nom_index, colonnes, unique)
        return indices

    def load_table_data(self, conn, table_name):
        """Charge les données d'une table depuis la base de données"""
        try:
            # Déterminer la colonne de tri principale
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Chercher une clé primaire
            pk_columns = [col[1] for col in columns if col[5] > 0]  # col[5] > 0 signifie que c'est une partie de la clé primaire
            
            if pk_columns:
                # Utiliser toutes les colonnes de la clé primaire pour le tri
                order_by = f"ORDER BY {', '.join(pk_columns)}"
            else:
                # Si pas de clé primaire, utiliser la première colonne
                order_by = f"ORDER BY {columns[0][1]}" if columns else ""
            
            df = pd.read_sql_query(f"SELECT * FROM {table_name} {order_by}", conn)
            return df
        except Exception as e:
            print(f"Erreur lors du chargement des données de la table {table_name}: {e}")
            return None

    def compare_table_structures(self, structure1, structure2):
        """Compare les structures de deux tables et retourne les différences"""
        differences = []
        
        # Colonnes dans la première structure mais pas dans la deuxième
        cols1 = {col[0] for col in structure1}
        cols2 = {col[0] for col in structure2}
        
        only_in_1 = cols1 - cols2
        only_in_2 = cols2 - cols1
        
        if only_in_1:
            differences.append(f"Colonnes uniquement dans DB1: {', '.join(only_in_1)}")
        
        if only_in_2:
            differences.append(f"Colonnes uniquement dans DB2: {', '.join(only_in_2)}")
        
        # Comparer les définitions des colonnes communes
        common_cols = cols1.intersection(cols2)
        for col in common_cols:
            def1 = next(s for s in structure1 if s[0] == col)
            def2 = next(s for s in structure2 if s[0] == col)
            
            if def1 != def2:
                differences.append(f"Colonne '{col}' diffère: DB1={def1}, DB2={def2}")
        
        return differences

    def compare_indices(self, indices1, indices2):
        """Compare les indices de deux tables et retourne les différences"""
        differences = []
        
        idx_names1 = {idx[0] for idx in indices1}
        idx_names2 = {idx[0] for idx in indices2}
        
        only_in_1 = idx_names1 - idx_names2
        only_in_2 = idx_names2 - idx_names1
        
        if only_in_1:
            differences.append(f"Indices uniquement dans DB1: {', '.join(only_in_1)}")
        
        if only_in_2:
            differences.append(f"Indices uniquement dans DB2: {', '.join(only_in_2)}")
        
        # Comparer les définitions des indices communs
        common_idx = idx_names1.intersection(idx_names2)
        for idx_name in common_idx:
            def1 = next(idx for idx in indices1 if idx[0] == idx_name)
            def2 = next(idx for idx in indices2 if idx[0] == idx_name)
            
            if def1 != def2:
                differences.append(f"Index '{idx_name}' diffère: DB1={def1}, DB2={def2}")
        
        return differences

    def compare_data(self, df1, df2):
        """Compare les données de deux tables et retourne les différences"""
        if df1 is None or df2 is None:
            return "Impossible de comparer les données (erreur de chargement)"
        
        if len(df1) != len(df2):
            return f"Nombre d'enregistrements différent: DB1={len(df1)}, DB2={len(df2)}"
        
        # S'assurer que les colonnes sont dans le même ordre
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        common_cols = list(cols1.intersection(cols2))
        
        # Comparer uniquement les colonnes communes
        if common_cols:
            df1_common = df1[common_cols].copy()
            df2_common = df2[common_cols].copy()
            
            # Trier les DataFrames pour une comparaison cohérente
            df1_common = df1_common.sort_values(by=common_cols).reset_index(drop=True)
            df2_common = df2_common.sort_values(by=common_cols).reset_index(drop=True)
            
            if df1_common.equals(df2_common):
                return None  # Pas de différences
            else:
                # Trouver les enregistrements différents
                diff_indexes = []
                for i in range(len(df1_common)):
                    if i < len(df2_common) and not df1_common.iloc[i].equals(df2_common.iloc[i]):
                        diff_indexes.append(i)
                
                return f"{len(diff_indexes)} enregistrements différents sur {len(df1_common)}"
        else:
            return "Aucune colonne commune pour comparer les données"

    def compare_tables(self):
        """Compare toutes les tables entre les deux bases de données"""
        try:
            # Vérifier l'existence des fichiers
            if not os.path.exists(self.db1_path):
                raise FileNotFoundError(f"Base de données 1 introuvable: {self.db1_path}")
            if not os.path.exists(self.db2_path):
                raise FileNotFoundError(f"Base de données 2 introuvable: {self.db2_path}")
            
            # Connexion aux bases de données
            conn1 = sqlite3.connect(self.db1_path)
            conn2 = sqlite3.connect(self.db2_path)
            
            # Récupération des tables
            tables1 = self.get_tables(self.db1_path)
            tables2 = self.get_tables(self.db2_path)
            
            # Tables présentes dans DB1 mais pas dans DB2
            self.results["tables_only_in_db1"] = sorted(tables1 - tables2)
            
            # Tables présentes dans DB2 mais pas dans DB1
            self.results["tables_only_in_db2"] = sorted(tables2 - tables1)
            
            # Tables communes à comparer
            common_tables = sorted(tables1.intersection(tables2))
            
            # Compteurs pour la synthèse
            total_tables = 0
            identical_tables = 0
            different_tables = 0
            
            for table in common_tables:
                total_tables += 1
                
                # Comparer la structure
                structure1 = self.get_table_structure(conn1, table)
                structure2 = self.get_table_structure(conn2, table)
                struct_diffs = self.compare_table_structures(structure1, structure2)
                
                # Comparer les indices
                indices1 = self.get_indices(conn1, table)
                indices2 = self.get_indices(conn2, table)
                idx_diffs = self.compare_indices(indices1, indices2)
                
                # Comparer les données si les structures sont identiques
                data_diff = None
                if not struct_diffs:
                    df1 = self.load_table_data(conn1, table)
                    df2 = self.load_table_data(conn2, table)
                    data_diff = self.compare_data(df1, df2)
                
                # Déterminer si la table est identique ou différente
                if not struct_diffs and not idx_diffs and not data_diff:
                    self.results["identical_tables"].append(table)
                    identical_tables += 1
                else:
                    self.results["different_tables"][table] = {
                        "structure_differences": struct_diffs,
                        "index_differences": idx_diffs,
                        "data_differences": data_diff
                    }
                    different_tables += 1
            
            # Synthèse
            self.results["summary"] = {
                "total_tables": total_tables,
                "identical_tables": identical_tables,
                "different_tables": different_tables,
                "tables_only_in_db1": len(self.results["tables_only_in_db1"]),
                "tables_only_in_db2": len(self.results["tables_only_in_db2"])
            }
            
            # Fermeture des connexions
            conn1.close()
            conn2.close()
            
            return self.results
            
        except Exception as e:
            print(f"Erreur lors de la comparaison des bases de données: {e}")
            if 'conn1' in locals():
                conn1.close()
            if 'conn2' in locals():
                conn2.close()
            raise

    def output_results(self):
        """Génère la sortie des résultats selon le format demandé"""
        if self.output_format == "console":
            self._output_console()
        elif self.output_format == "html":
            self._output_html()
        elif self.output_format == "csv":
            self._output_csv()
        else:
            print(f"Format de sortie non supporté: {self.output_format}")

    def _output_console(self):
        """Affiche les résultats dans la console"""
        print("\n" + "=" * 80)
        print(f"COMPARAISON DES BASES DE DONNÉES SQLITE")
        print(f"DB1: {self.db1_path}")
        print(f"DB2: {self.db2_path}")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Synthèse
        print("\n[SYNTHÈSE]")
        print(f"Total des tables communes: {self.results['summary']['total_tables']}")
        print(f"Tables identiques: {self.results['summary']['identical_tables']}")
        print(f"Tables différentes: {self.results['summary']['different_tables']}")
        print(f"Tables uniquement dans DB1: {self.results['summary']['tables_only_in_db1']}")
        print(f"Tables uniquement dans DB2: {self.results['summary']['tables_only_in_db2']}")
        
        # Tables manquantes
        if self.results["tables_only_in_db1"]:
            print("\n[TABLES UNIQUEMENT DANS DB1]")
            for table in self.results["tables_only_in_db1"]:
                print(f"  - {table}")
        
        if self.results["tables_only_in_db2"]:
            print("\n[TABLES UNIQUEMENT DANS DB2]")
            for table in self.results["tables_only_in_db2"]:
                print(f"  - {table}")
        
        # Tables identiques
        if self.results["identical_tables"]:
            print("\n[TABLES IDENTIQUES]")
            for table in self.results["identical_tables"]:
                print(f"  ✓ {table}")
        
        # Tables différentes
        if self.results["different_tables"]:
            print("\n[TABLES DIFFÉRENTES]")
            for table, diffs in self.results["different_tables"].items():
                print(f"\n  ❌ {table}")
                
                if diffs["structure_differences"]:
                    print("    [Différences de structure]")
                    for diff in diffs["structure_differences"]:
                        print(f"      - {diff}")
                
                if diffs["index_differences"]:
                    print("    [Différences d'indices]")
                    for diff in diffs["index_differences"]:
                        print(f"      - {diff}")
                
                if diffs["data_differences"]:
                    print("    [Différences de données]")
                    print(f"      - {diffs['data_differences']}")

def main():
    """Fonction principale pour l'exécution en ligne de commande"""
    parser = argparse.ArgumentParser(description="Outil de comparaison de bases de données SQLite")
    parser.add_argument("db1", help="Chemin vers la première base de données SQLite")
    parser.add_argument("db2", help="Chemin vers la deuxième base de données SQLite")
    parser.add_argument("-f", "--format", choices=["console", "html", "csv"], default="console",
                        help="Format de sortie (défaut: console)")
    parser.add_argument("-o", "--output", help="Fichier de sortie pour les formats HTML et CSV")
    
    args = parser.parse_args()
    
    if args.format != "console" and not args.output:
        parser.error("Un fichier de sortie est requis pour les formats HTML et CSV")
    
    try:
        comparator = SQLiteComparator(args.db1, args.db2, args.format, args.output)
        comparator.compare_tables()
        comparator.output_results()
    except Exception as e:
        print(f"Erreur: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 