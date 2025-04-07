import logging
from src.config import DBContext
import src.transform as main

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/transform_test.log"),
        logging.StreamHandler()
    ]
)
    
def test_table_price():
    with  DBContext(profile=True, memory_limit=1) as conn:
        main.main(conn, field='price') 
        conn.verify(table=f"price_wide")

def test_table_price():
    with  DBContext(profile=True, memory_limit=1) as conn:
        main.main(conn, field='trade_volume') 
        conn.verify(table=f"trade_volume_wide")