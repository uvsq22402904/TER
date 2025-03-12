from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class Car(Base):
    __tablename__ = 'car'
    id = Column(Integer, primary_key=True)
    model = Column(String)
    owner_id = Column(Integer, ForeignKey('person.id'))

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


person1 = Person(name='Alice')
person2 = Person(name='Bob')
session.add(person1)
session.add(person2)

car1 = Car(model='Tesla Model S', owner_id=1)
car2 = Car(model='Toyota Corolla', owner_id=2)
session.add(car1)
session.add(car2)

session.commit()