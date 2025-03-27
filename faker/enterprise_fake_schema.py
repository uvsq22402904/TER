from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import date

Base = declarative_base()

# Table d'association entre Employe et Service
employe_service_association = Table(
    'employe_service_association',
    Base.metadata,
    Column('employe_id', Integer, ForeignKey('employe.id'), primary_key=True),
    Column('service_id', Integer, ForeignKey('service.id'), primary_key=True)
)

class Entreprise(Base):
    __tablename__ = 'entreprise'
    id = Column(Integer, primary_key=True)
    nom = Column(String, nullable=False)
    ville = Column(String, nullable=False)
    employes = relationship('Employe', back_populates='entreprise')

class Service(Base):
    __tablename__ = 'service'
    id = Column(Integer, primary_key=True)
    nom = Column(String, nullable=False)
    employes = relationship('Employe', secondary=employe_service_association, back_populates='services')

class Employe(Base):
    __tablename__ = 'employe'
    id = Column(Integer, primary_key=True)
    nom = Column(String, nullable=False)
    date_embauche = Column(Date, nullable=False)
    entreprise_id = Column(Integer, ForeignKey('entreprise.id'))
    entreprise = relationship('Entreprise', back_populates='employes')
    services = relationship('Service', secondary=employe_service_association, back_populates='employes')

# Création de la base de données SQLite
engine = create_engine('sqlite:///data/example.db')
Base.metadata.create_all(engine)

# Création de la session
Session = sessionmaker(bind=engine)
session = Session()

def clear_database(session, metadata):
    for table in reversed(metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()

clear_database(session, Base.metadata)

# Ajout des entreprises
entreprises = [
    Entreprise(nom='TechCorp', ville='Paris'),
    Entreprise(nom='DataSoft', ville='Lyon'),
    Entreprise(nom='Innovatech', ville='Marseille'),
    Entreprise(nom='CloudNet', ville='Toulouse')
]
session.add_all(entreprises)

# Ajout des services
services = [
    Service(nom='Développement'),
    Service(nom='Ressources Humaines'),
    Service(nom='Marketing'),
    Service(nom='Support IT'),
    Service(nom='Finance')
]
session.add_all(services)
session.commit()

# Ajout des employés avec plusieurs services chacun
employes = [
    Employe(nom='Alice', date_embauche=date(2020, 5, 10), entreprise=entreprises[0], services=[services[0]]),
    Employe(nom='Bob', date_embauche=date(2018, 7, 23), entreprise=entreprises[0], services=[services[1], services[4]]),
    Employe(nom='Charlie', date_embauche=date(2022, 1, 15), entreprise=entreprises[1], services=[services[2]]),
    Employe(nom='David', date_embauche=date(2019, 3, 8), entreprise=entreprises[1], services=[services[0]]),
    Employe(nom='Emma', date_embauche=date(2021, 6, 5), entreprise=entreprises[2], services=[services[3]]),
    Employe(nom='Frank', date_embauche=date(2017, 2, 14), entreprise=entreprises[2], services=[services[4]]),
    Employe(nom='Grace', date_embauche=date(2023, 9, 12), entreprise=entreprises[3], services=[services[0], services[3]]),
    Employe(nom='Hank', date_embauche=date(2016, 11, 30), entreprise=entreprises[3], services=[services[2]]),
    Employe(nom='Irene', date_embauche=date(2015, 4, 21), entreprise=entreprises[0], services=[services[4]]),
    Employe(nom='Jack', date_embauche=date(2024, 1, 7), entreprise=entreprises[1], services=[services[3]]),
    Employe(nom='Kevin', date_embauche=date(2018, 10, 15), entreprise=entreprises[2], services=[services[1]]),
    Employe(nom='Laura', date_embauche=date(2020, 12, 2), entreprise=entreprises[3], services=[services[4]])
]
session.add_all(employes)
session.commit()

print("Base de données initialisée avec table d'association !")
