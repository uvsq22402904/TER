from models.neo import neo
from enviroment import DATABASE_URI_TEST


# test extration from sql to neo4j
neo(DATABASE_URI_TEST)
    