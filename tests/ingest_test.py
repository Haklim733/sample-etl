import logging
import src.ingest as main 
from src.config import DBContext

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/ingest_test.log"),
        logging.StreamHandler()
    ]
)
    
def test_main():
    with  DBContext(profile=True, memory_limit=0.5) as conn:
        main.process_file_by_file(conn, input_dir='./data/') 