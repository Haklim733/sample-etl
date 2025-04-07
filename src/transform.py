import argparse
import logging
from src.config import DBContext

logger = logging.getLogger(__name__)

def create_wide_tables(con: DBContext, table: str) -> None:
    # Create prices and trade_volume table for local testing
    columns = ["date DATE PRIMARY KEY"]
    data_type = "INTEGER"
    if table == "price":
        data_type = "FLOAT"
    
    for stock in DBContext.STOCK_COLUMNS:
        columns.append(f"{stock} {data_type}")    
    try:
        str_columns = ', '.join(columns)
        con.execute(f"CREATE OR REPLACE TABLE {table} ({str_columns})")
        logger.info(f"Successfully created {table} table")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise        

def main(con: DBContext, *, field: str, source_table="stocks.raw", batch_size: int = 20, local: bool = True):
    """ 
    """
    total_stocks = len(con.SYMBOLS)
    table_name = f"{field}_wide"
        
    if local:
        create_wide_tables(con, table=f"{table_name}")
    
    for i in range(0, total_stocks, batch_size):
        batch_end = min(i + batch_size, total_stocks)
        batch_ids = con.SYMBOLS[i:batch_end]
        logger.info(f"Processing batch starting with ID {batch_ids[0]}")
        
        # Create pivot query for this batch
        pivot_columns = []
        for stock_id in batch_ids:
            pivot_columns.append(f"MAX(CASE WHEN id = {stock_id} THEN {field} END) AS stk_{stock_id}")
        
        pivot_sql = ", ".join(pivot_columns)

        if i == 0:
            con.execute(f"""
                    INSERT OR IGNORE INTO {table_name} (date)
                    SELECT date FROM {source_table} 
                    WHERE id = 1 
                    ORDER BY date
            """)
        
        update_sql = f"""
        UPDATE {field}_wide
        SET {', '.join(f'stk_{id} = batch_pivot.stk_{id}' for id in batch_ids)}
        FROM (
            SELECT 
                date,
                {pivot_sql}
            FROM {source_table}
            WHERE id IN ({','.join(str(id) for id in batch_ids)})
            GROUP BY date
            ORDER BY date
        ) AS batch_pivot
        WHERE {field}_wide.date = batch_pivot.date
        """
        logger.info(update_sql)
        con.execute("BEGIN TRANSACTION")
        con.execute(update_sql)
        con.execute("COMMIT")
        
        logger.info(f"Processed stocks {batch_ids[0]}-{batch_end}")
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process stock data')
    parser.add_argument('--table', choices=['price', 'trade_volume'], required=True, 
                        help='Type of data to process: price or volume')
    args = parser.parse_args()
 
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/transform.log"),
            logging.StreamHandler()
        ]
    )
    
    logger.info(f"Starting processing for {args.table} data")
    
    with DBContext(profile=True) as conn:
        main(conn, field=args.table) 
        conn.verify(table=f"{args.table}_wide")
    logger.info(f"Completed processing for {args.table} data")
    