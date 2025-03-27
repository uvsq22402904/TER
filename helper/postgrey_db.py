from sqlalchemy import Engine
import pandas as pd



# postgreSql
def postgreSchema(table: str, db_engine: Engine):
    return pd.read_sql_query(
                f"SELECT column_name, data_type, character_maximum_length, is_nullable "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table}';", 
                db_engine
            )
