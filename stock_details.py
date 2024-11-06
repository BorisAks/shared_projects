import json
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

class InvalidInputError(Exception):
    """Custom exception for invalid inputs."""
    pass

class StockDataFetcher:
    def __init__(self, db_url: str):
        """Initialize the StockDataFetcher with the database URL."""
        self.engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
        self.metadata = MetaData()
        self.stocks = Table('stock_rates', self.metadata, autoload_with=self.engine)

    @staticmethod
    def validate_inputs(start_date: str, end_date: str, check_date_range: bool = True):
        """Validates the input parameters and ensures the maximum range of 30 days if check_date_range is True."""
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise InvalidInputError("Invalid date format. Expected format is YYYY-MM-DD.")
        
        # Only check the date range if check_date_range is True
        if check_date_range and (end_date_obj - start_date_obj).days > 30:
            raise InvalidInputError("The date range cannot exceed 30 days.")
        
        # Ensure the end_date is not before the start_date
        if end_date_obj < start_date_obj:
            raise InvalidInputError("end_date cannot be before start_date.")

    def fetch_stock_data(self, stock_symbol: str, start_date: str, end_date: str, file_path: str):
        """Fetch stock data from MySQL using SQLAlchemy Core."""
        
        # Validate the inputs
        self.validate_inputs(
            start_date, end_date)
        
        # Prepare the SQL query
        query = select(self.stocks).where(
            self.stocks.c.Symbol == stock_symbol,
            self.stocks.c.Date.between(start_date, end_date)
        )
        
        try:
            # Execute the query
            with self.engine.connect() as conn:
                result = conn.execute(query)
            
                stock_data = [dict(zip(result.keys(), row)) for row in result]
                
                # Convert the stock data to a JSON formatted string, each entry on a new line
                stock_data_json = json.dumps(stock_data, default=str, indent=4)
             
                with open(file_path, 'w') as f:
                    f.write(stock_data_json)
            print(f"JSON data saved to {file_path}")
            return stock_data
            
        except SQLAlchemyError as e:
            raise InvalidInputError(f"Database error: {str(e)}")

# usage
if __name__ == "__main__":
    stock_symbol = "AAPL"
    start_date = "2019-08-01"
    end_date = "2019-08-30"
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = fr'C:\Users\boris\OneDrive\Documents\DataEngenier\python\stock_data_{stock_symbol}_{current_date}.json'
    db_url = 'mysql+pymysql://dev:dev@127.0.0.1:3306/python_project?charset=utf8mb4'

    fetcher = StockDataFetcher(db_url)
    
    stock_data = fetcher.fetch_stock_data(stock_symbol, start_date, end_date, file_path)
 
