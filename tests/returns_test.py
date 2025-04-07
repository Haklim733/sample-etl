import logging
from src.config import DBContext
import src.returns as main

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/returns_test.log"),
        logging.StreamHandler()
    ]
)
    
def test_returns():
    with  DBContext(profile=True, memory_limit=1) as conn:
        main.create_table(conn)
        main.calculate_returns(conn)
        conn.verify('stock_returns')