import json
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, Date, select, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
import traceback
from stock_details import StockDataFetcher

# Define the base class for models
Base = declarative_base()

# Define the StockRate model class
class StockRate(Base):
    __tablename__ = 'stock_rates'
    Symbol = Column(String, primary_key=True)
    SecurityName = Column(String)
    Date = Column(Date, primary_key=True)
    Close = Column(Float)
    High = Column(Float)
    Low = Column(Float)
 

# Define the Stocks_Stats class
class Stocks_Stats:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)     
        
    def fetch_stock(self, stock_symbol_lst, startdate, enddate, file_path):
        session = self.Session()
        StockDataFetcher.validate_inputs(startdate, enddate, False)
        try:
            # Subquery 1: Calculate Yield based on startdate and enddate
            start_subquery = (
                session.query(
                    StockRate.Symbol.label('Symbol'),
                    StockRate.Close.label('start_close')
                )
                .filter(and_(StockRate.Date == startdate, StockRate.Symbol.in_(stock_symbol_lst)))
                .subquery()
            )

            end_subquery = (
                session.query(
                    StockRate.Symbol.label('Symbol'),
                    StockRate.Close.label('end_close')
                )
                .filter(and_(StockRate.Date == enddate, StockRate.Symbol.in_(stock_symbol_lst)))
                .subquery()
            )

            # Calculate Yield: ((end_close - start_close) / start_close) * 100
            yield_cte = (
                session.query(
                    start_subquery.c.Symbol,
                    start_subquery.c.start_close,
                    end_subquery.c.end_close,
                    ((end_subquery.c.end_close - start_subquery.c.start_close) /
                     start_subquery.c.start_close * 100).label('Yield')
                )
                .join(end_subquery, start_subquery.c.Symbol == end_subquery.c.Symbol)
                .subquery()
            )

            # Subquery 2: Fetch Max_rate, Min_rate, AVG_Rate for the given symbols and date range
            stats_cte = (
                session.query(
                    StockRate.Symbol,
                    StockRate.SecurityName,
                    func.max(StockRate.High).label('Max_rate'),
                    func.min(StockRate.Low).label('Min_rate'),
                    func.avg(StockRate.Close).label('AVG_Rate')
                )
                .filter(and_(StockRate.Date.between(startdate, enddate), StockRate.Symbol.in_(stock_symbol_lst)))
                .group_by(StockRate.Symbol, StockRate.SecurityName)
                .subquery()
            )

            # Final query: Join Yield and stats data, order by Yield descending
            final_query = (
                session.query(
                    yield_cte.c.Symbol,
                    stats_cte.c.SecurityName,
                    yield_cte.c.start_close,
                    yield_cte.c.end_close,
                    stats_cte.c.Max_rate,
                    stats_cte.c.Min_rate,
                    stats_cte.c.AVG_Rate,
                    yield_cte.c.Yield
                )
                .join(stats_cte, yield_cte.c.Symbol == stats_cte.c.Symbol)
                .order_by(yield_cte.c.Yield.desc())
            )

            # Execute query and fetch results
            results = final_query.all()
            result_list =[
                          {
                           "Symbol": row.Symbol,
                           "Security_Name": row.SecurityName,
                           "Close_start_price": row.start_close,
                           "Close_end_price": row.end_close,
                           "Max_rate": row.Max_rate,
                           "Min_rate": row.Min_rate,
                           "AVG_Rate": row.AVG_Rate,
                           "Yield": row.Yield
                          }
                            for row in results
                         ]
            stock_stat_json = json.dumps(result_list, default=str, indent=4)
            # Optionally save to a file
            if file_path and results:
                with open(file_path, 'w') as f:
                    f.write(stock_stat_json)
            print(f"JSON data saved to {file_path}")
            # Return results
            return results

        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()  # Get the full error traceback for debugging

        finally:
            session.close()

#usage
if __name__ == "__main__":

    db_url = 'mysql+pymysql://dev:dev@127.0.0.1:3306/python_project?charset=utf8mb4'
    stocks_stats = Stocks_Stats(db_url)

    # Example fetch with startdate and enddate, filtering by stock_symbol_lst
    stock_symbol_lst = ['BRY', 'NUAN', 'SCKT']
    startdate = '1999-11-01'
    enddate = '1999-11-30'
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = fr'C:\Users\boris\OneDrive\Documents\DataEngenier\python\stock_stat_{current_date}.json'
    data = stocks_stats.fetch_stock(stock_symbol_lst, startdate, enddate, file_path)

    if data:  # Check if stock_data is not None
        for row in data:
            print(row)
    else:
        print("No data fetched.")
