import pandas as pd
from sqlalchemy import create_engine, text

# Create a SQLAlchemy engine to connect to MySQL
engine = create_engine('mysql+pymysql://dev:dev@127.0.0.1:3306/python_project?charset=utf8mb4',isolation_level="AUTOCOMMIT")

conn = engine.connect()

# Writing an SQL query
query = "SELECT * FROM stock_rates limit 10"

# Reading the query into a DataFrame
df = pd.read_sql_query(query, conn)

# Display the DataFrame
print(df)