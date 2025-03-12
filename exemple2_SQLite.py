from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# Classe représentant une personne
class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, primary_key=True)
    name = Column(String)

# Classe représentant une maison
class House(Base):
    __tablename__ = 'house'
    id = Column(Integer, primary_key=True)
    address = Column(String)
    owner_id = Column(Integer, ForeignKey('person.id'))

# Classe représentant un chien
class Dog(Base):
    __tablename__ = 'dog'
    id = Column(Integer, primary_key=True)
    breed = Column(String)
    owner_id = Column(Integer, ForeignKey('person.id'))
    house_id = Column(Integer, ForeignKey('house.id'))

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

# Ajouter des personnes
person1 = Person(name='Alice')
person2 = Person(name='Bob')
session.add(person1)
session.add(person2)

# Ajouter des maisons
house1 = House(address='123 Main St', owner_id=1)
house2 = House(address='456 Oak Rd', owner_id=2)
session.add(house1)
session.add(house2)

dog1 = Dog(breed='Golden Retriever', owner_id=1, house_id=1)  # Chien vivant dans la maison 1
dog2 = Dog(breed='Bulldog', owner_id=2, house_id=2)  # Chien vivant dans la maison 2
session.add(dog1)
session.add(dog2)

session.commit()
