from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

# Table d'association Many-to-Many entre Person et Car
person_car = Table(
    'person_car', Base.metadata,
    Column('person_id', Integer, ForeignKey('person.id'), primary_key=True),
    Column('car_id', Integer, ForeignKey('car.id'), primary_key=True)
)

class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    cars = relationship('Car', secondary=person_car, back_populates='owners')

class Car(Base):
    __tablename__ = 'car'
    id = Column(Integer, primary_key=True)
    model = Column(String)
    owners = relationship('Person', secondary=person_car, back_populates='cars')

# Créer la base de données SQLite
engine = create_engine('sqlite:///example.db')
Base.metadata.create_all(engine)

# Ajouter des données
Session = sessionmaker(bind=engine)
session = Session()

def clear_database(session, metadata):
    """Supprime toutes les données de toutes les tables de la base de données."""
    for table in reversed(metadata.sorted_tables):  # Suppression dans l'ordre inverse pour gérer les FK
        session.execute(table.delete())
    session.commit()

clear_database(session, Base.metadata)

# Création des personnes
alice = Person(name='Alice')
bob = Person(name='Bob')
yanis = Person(name='Yanis')  # Ajout de Yanis

session.add_all([alice, bob, yanis])
session.commit()

# Création des voitures
tesla = Car(model='Tesla Model S')
toyota = Car(model='Toyota Corolla')

session.add_all([tesla, toyota])
session.commit()

# Associer les voitures aux propriétaires
alice.cars.append(tesla)  # Alice possède la Tesla
yanis.cars.append(tesla)  # Yanis possède aussi la même Tesla
bob.cars.append(toyota)   # Bob possède la Toyota

session.commit()

# Vérification des propriétaires de la Tesla
tesla_owners = session.query(Person).join(person_car).filter(Car.id == tesla.id).all()
print(f"Les propriétaires de la {tesla.model} sont : {[owner.name for owner in tesla_owners]}")
