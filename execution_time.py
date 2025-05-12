import sqlite3
import time

# Connexion à la base de données
conn = sqlite3.connect('data/example.db')
cursor = conn.cursor()

# Enregistrer le temps de début
start_time = time.time()

# Exécuter la requête SQL
cursor.execute("""
    SELECT *
    FROM employe emp
    INNER JOIN entreprise ent ON emp.entreprise_id = ent.id
    INNER JOIN employe_service_association esa ON emp.id = esa.employe_id
    INNER JOIN service ser ON esa.service_id = ser.id
    WHERE ser.nom = 'Finance'
      AND ent.nom = 'Innovatech';
""")

# Récupérer les résultats (optionnel)
resultats = cursor.fetchall()

# Calculer et afficher le temps d'exécution
end_time = time.time()
execution_time = end_time - start_time
print(f"Temps d'exécution de la requête : {execution_time} secondes")

# Fermer la connexion
conn.close()
