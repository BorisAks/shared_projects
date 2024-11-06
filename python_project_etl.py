import os
import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, TIMESTAMP, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Set up database connection
db_url = 'mysql+pymysql://dev:dev@127.0.0.1:3306/python_project?charset=utf8mb4'
engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
metadata = MetaData()

# Define the logging table in SQLAlchemy
stocks_data_log = Table(
    'stocks_data_log', metadata,
    Column('ID', Integer, primary_key=True, autoincrement=True),
    Column('ETL_PROCESS', String(255), default='stocks_data'),
    Column('Log_time', TIMESTAMP, default=func.now()),
    Column('Log_level', String(10), default='INFO'),
    Column('ERROR_DESC', Text, nullable=True)
)

# Create the table if it doesn't exist
metadata.create_all(engine)

# Session setup
Session = sessionmaker(bind=engine)
session = Session()

# Custom logging handler to log into the MySQL database
class DBLogHandler(logging.Handler):
    def emit(self, record):
        log_message = self.format(record)
        log_entry = {
            'ETL_PROCESS': stock_symbol,
            'Log_level': record.levelname,
            'ERROR_DESC': log_message if record.levelname == 'ERROR' else None
        }
        try:
            # Insert the log entry into the database
            session.execute(stocks_data_log.insert().values(log_entry))
            session.commit()
        except SQLAlchemyError as e:
            print(f"Failed to log to the database: {str(e)}")
            session.rollback()

# Set up logging to file and database
logging.basicConfig(filename="process_log.log", level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

# Add the custom database handler
db_handler = DBLogHandler()
db_handler.setLevel(logging.INFO)
logger.addHandler(db_handler)

# Define the directory and database connection for stock data
stock_files_directory = "C://Users//boris//OneDrive//Documents//DataEngenier//python//python_project//stock_dataset//stocks"
security_name_file = "C://Users//boris//OneDrive//Documents//DataEngenier//python//python_project//stock_dataset//symbols_valid_meta.csv"
engine = create_engine('mysql+pymysql://dev:dev@127.0.0.1:3306/python_project?charset=utf8mb4', isolation_level="AUTOCOMMIT")

# Load security names from the security name file
security_df = pd.read_csv(security_name_file)

# Get the list of stock files
stock_files = sorted([f for f in os.listdir(stock_files_directory) if f.endswith('.csv')])

# Process each file
for stock_file in stock_files:
    try:
        # Extract stock symbol from the file name (assuming it's the part before '.csv')
        stock_symbol = os.path.splitext(stock_file)[0]

        # Load the stock data CSV file
        stock_data = pd.read_csv(os.path.join(stock_files_directory, stock_file))

        # Add a new column 'Symbol' to the stock data
        stock_data['Symbol'] = stock_symbol

        # Merge with security names using the 'Symbol' column
        merged_data = pd.merge(stock_data, security_df[['Symbol', 'Security Name']], on='Symbol', how='left')
        merged_data = merged_data.rename(columns={'Security Name': 'SecurityName'})

        # Push the data into the MySQL database in chunks of 500 rows
        merged_data.to_sql('stock_rates', con=engine, if_exists='append', index=False, chunksize=500)

        # Log success to both the file and the database
        logger.info(f"Successfully processed and inserted data for stock symbol: {stock_symbol}")

    except Exception as e:
        # Log error to both the file and the database
        logger.error(f"Error processing stock file {stock_file}: {str(e)}")
