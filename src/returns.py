import argparse
import logging
from src.config import DBContext

logger = logging.getLogger(__name__)


def create_table(con: DBContext):
    # Create stock_returns tablee
    columns = ["date DATE PRIMARY KEY"]
    for symbol in DBContext.STOCK_COLUMNS:
        columns.append(f"{symbol} FLOAT")    
    try:
        str_columns = ', '.join(columns)
        con.execute(f"CREATE OR REPLACE TABLE stock_returns ({str_columns})")
        logger.info(f"Successfully created stock_returns table")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise        
    
def calculate_returns(con: DBContext, batch_size=20):
    """Calculate returns for all stocks using batch processing similar to transform.py"""
    stock_columns = con.STOCK_COLUMNS
    
    try:
        
        total_stocks = len(stock_columns)
         
        # Process stocks in batches
        for i in range(0, total_stocks, batch_size):
            batch_end = min(i + batch_size, total_stocks)
            batch_ids = con.STOCK_COLUMNS[i:batch_end]
            logger.info(f"Processing batch starting with column {batch_ids[0]}")
            
            # Create return calculation expressions for this batch
            return_exprs = []
            for stock in batch_ids:
                return_exprs.append(f"""
                    (pw.{stock} - LAG(pw.{stock}, 1) OVER (ORDER BY pw.date)) / 
                    NULLIF(LAG(pw.{stock}, 1) OVER (ORDER BY pw.date), 0) * 100 AS {stock}
                """)
            
            if i == 0:
                con.execute("""
                    INSERT OR IGNORE INTO stock_returns (date)
                    SELECT date FROM price_wide
                """)

            # Begin transaction for this batch
            con.execute("BEGIN TRANSACTION")
                        
            # Update stock_returns with calculated returns for this batch
            update_sql = f"""
                UPDATE stock_returns sr
                SET {', '.join(f'{stock} = batch_returns.{stock}' for stock in batch_ids)}
                FROM (
                    SELECT 
                        pw.date,
                        {', '.join(return_exprs)}
                    FROM price_wide pw
                    ORDER BY pw.date
                ) AS batch_returns
                WHERE sr.date = batch_returns.date
            """
            logger.info(update_sql)
            
            con.execute(update_sql)
            
            # Commit transaction
            con.execute("COMMIT")
            
            # Log memory usage after each batch
            logger.info(f"Completed batch {batch_ids[0]}-{batch_ids[-1]}")
    except Exception as e:
        logger.error(f"Error calculating returns: {e}")
        raise 

if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/returns.log"),
            logging.StreamHandler()
        ]
    )
    
    parser = argparse.ArgumentParser(description='Analyze stock returns')
    parser.add_argument('--output', default="results", help='Output directory for results')
    args = parser.parse_args()
    
    with DBContext(profile=True) as con:
        create_table(con)
        calculate_returns(con)
        con.verify('stock_returns')