from models.neo import neo
from models.m_sqlite import m_sqlite
from enviroment import DATABASE_URI_TEST,DATABASE_URI_OUPUT_TEST


# test extration from sql to neo4j
# neo(DATABASE_URI_TEST)
m_sqlite(DATABASE_URI_OUPUT_TEST)
    