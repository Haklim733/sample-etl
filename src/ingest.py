import logging
import os
from pathlib import Path
from src.config import DBContext 

logger = logging.getLogger(__name__)
 
def process_file_by_file(con: DBContext, input_dir: str):
    """
    Process stock data files and insert into separate tables for each stock.
    
    Args:
        con: DuckDB connection object
        memory_limit: DuckDB memory limit in GB
    """
 
    input_files = list(Path(input_dir).glob("*.csv"))
    logger.info([(x,y) for x,y in enumerate(input_files)])

    if not input_files:
        logger.error("No input files found!")
        raise Exception("No input files found!") 

    con.execute(f"""
        CREATE OR REPLACE TABLE stocks.raw
            (id INTEGER,
            date DATE,
            price FLOAT,
            trade_volume INTEGER
            )
    """)

    for file_path in input_files:
        print(f"Processing file: {file_path}")

        file_path_str = file_path.as_posix()
        con.execute(f"""
            COPY stocks.raw FROM '{file_path_str}' WITH (HEADER, DELIMITER ',', FORMAT CSV)
            """) # assumes each files has the same schema

    con.close()

if __name__ == "__main__":
    FILE_DIR = os.environ['CSV_FILE_DIR']

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/ingest.log"),
            logging.StreamHandler()
        ]
    )
    
    conn = DBContext(profile=True)
    process_file_by_file(conn, input_dir=FILE_DIR)

    