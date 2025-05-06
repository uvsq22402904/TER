from sqlalchemy import (
    Integer, BigInteger, SmallInteger, Float, Numeric, Boolean, String,
    Date, DateTime, LargeBinary
)

from datetime import datetime

def convertir_type(sql_type: str):
    """Convertit un type SQL en type SQLAlchemy en utilisant match-case (Python 3.10+)."""
    sql_type = sql_type.upper().strip()

    match sql_type:
        case s if "BIGINT" in s:
            return BigInteger
        case s if "SMALLINT" in s:
            return SmallInteger
        case s if "INT" in s:
            return Integer
        case s if "FLOAT" in s or "REAL" in s or "DOUBLE" in s or "LONG" in s:
            return Float
        case s if "DECIMAL" in s or "NUMERIC" in s:
            return Numeric
        case s if "BOOL" in s:
            return Boolean
        case s if "CHAR" in s or "TEXT" in s or "VARCHAR" in s or "STRING" in s:
            return String
        case "DATE":
            return Date
        case s if "DATETIME" in s or "TIMESTAMP" in s:
            return DateTime
        case s if "BLOB" in s or "BINARY" in s:
            return LargeBinary
        case _:
            print(f"[WARN] Type SQL non reconnu : {sql_type} → String utilisé par défaut.")
            return String

def cast_value(value: str, type_name: str):
    if value is None:
        return None

    try:
        if type_name == "INTEGER":
            return int(value)
        elif type_name == "FLOAT":
            return float(value)
        elif type_name == "BOOLEAN":
            return value in ["true", "1", "True"]
        elif type_name == "DATE":
            return datetime.strptime(value, "%Y-%m-%d").date()
        elif type_name == "DATETIME":
            return datetime.fromisoformat(value)
        else:  # default to string
            return str(value)
    except Exception:
        return value  # fallback