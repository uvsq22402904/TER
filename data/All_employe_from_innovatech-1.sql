// This SQL query retrieves all employees who work in the Finance department of the company "Innovatech".
 
SELECT *
FROM employe emp
INNER JOIN entreprise ent ON emp.entreprise_id = ent.id
INNER JOIN employe_service_association esa ON emp.id = esa.employe_id
INNER JOIN service ser ON esa.service_id = ser.id
WHERE ser.nom = 'Finance'
  AND ent.nom = 'Innovatech';