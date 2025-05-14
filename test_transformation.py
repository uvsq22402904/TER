from Compare import SQLiteComparator

def main():
    # Chemins vers les bases de données
    example_db = "data/example.db"
    output_db = "data/output.db"
    
    print("=== Début de la comparaison des bases de données ===")
    print(f"Base de données exemple : {example_db}")
    print(f"Base de données de sortie : {output_db}")
    
    try:
        # Créer le comparateur
        comparator = SQLiteComparator(example_db, output_db)
        
        # Effectuer la comparaison
        results = comparator.compare_tables()
        
        # Afficher les résultats
        comparator.output_results()
        
        # Vérifier si la transformation a réussi
        if (len(results["tables_only_in_db1"]) == 0 and 
            len(results["tables_only_in_db2"]) == 0 and 
            len(results["different_tables"]) == 0):
            print("\n✅ La transformation a réussi ! Les bases de données sont identiques.")
        else:
            print("\n❌ La transformation a échoué. Des différences ont été trouvées.")
            
            if results["tables_only_in_db1"]:
                print("\nTables manquantes dans la base de sortie :")
                for table in results["tables_only_in_db1"]:
                    print(f"  - {table}")
            
            if results["tables_only_in_db2"]:
                print("\nTables supplémentaires dans la base de sortie :")
                for table in results["tables_only_in_db2"]:
                    print(f"  - {table}")
            
            if results["different_tables"]:
                print("\nTables avec des différences :")
                for table, diffs in results["different_tables"].items():
                    print(f"\n  Table : {table}")
                    if diffs["structure_differences"]:
                        print("    Différences de structure :")
                        for diff in diffs["structure_differences"]:
                            print(f"      - {diff}")
                    if diffs["index_differences"]:
                        print("    Différences d'indices :")
                        for diff in diffs["index_differences"]:
                            print(f"      - {diff}")
                    if diffs["data_differences"]:
                        print("    Différences de données :")
                        print(f"      - {diffs['data_differences']}")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la comparaison : {e}")
        raise

if __name__ == "__main__":
    main() 