
import pandas as pd
from sqlalchemy import create_engine, text

# Create a SQLAlchemy engine to connect to MySQL
engine = create_engine('mysql+pymysql://dev:dev@127.0.0.1:3306?charset=utf8mb4',isolation_level="AUTOCOMMIT")

conn = engine.connect()

db_name = 'python_project'

conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
print(f"Database '{db_name}' created successfully.")
