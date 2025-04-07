import logging
import os
from pathlib import Path
from typing import Any, Set 

import duckdb
import psutil

logger = logging.getLogger(__name__)

class DBContext:
    
    SYMBOLS = list(range(1, 201)) # known
    STOCK_COLUMNS = [f"stk_{id}" for id in SYMBOLS]

    def __init__(self, db_path: str = './data/stocks.duckdb', cpu_count: int = 4, memory_limit: int = 24, profile: bool = False, **kwargs):
        self.con = duckdb.connect(db_path)
        self.memory_limit = memory_limit
        self.cpu_count = cpu_count
        self.profile = profile
        self.process = psutil.Process(os.getpid())
         

    def __enter__(self):
        return self

    def configure(self, libraries: list[str] = None):
        if libraries:
            for lib in libraries:
                self.con.execute(f""" INSTALL {lib}; LOAD {lib};""")
        self.con.execute(f"PRAGMA memory_limit='{self.memory_limit}GB'")
        self.con.execute(f"PRAGMA threads={self.cpu_count}")

        if self.profile:
            stmt = f"""
            PRAGMA profiling_mode='detailed';
            PRAGMA custom_profiling_settings = '{"CPU_TIME": "true", "EXTRA_INFO": "true", "OPERATOR_CARDINALITY": "true", "OPERATOR_TIMING": "true"}'; 
            PRAGMA enable_object_cache=false;
            """ 
            self.con.execute(stmt)

    def format_profile_output(self, explain_output: list[Any]):
        plan_lines = []
        for row in explain_output:
            if isinstance(row, (tuple, list)) and len(row) > 0:
                for item in row:
                    if item is not None:
                        if isinstance(item, str) and '\n' in item:
                            plan_lines.extend(item.split('\n'))
                        else:
                            plan_lines.append(str(item))
        return plan_lines
        
    def execute(self, query):
        process = psutil.Process()
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        if self.profile and query.strip().upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'COPY')):
            explain_query = f"EXPLAIN ANALYZE {query}"
            results = self.con.execute(explain_query).fetchall()
            plan_lines = self.format_profile_output(results) 
            plan_text = "\n".join(line for line in plan_lines if line.strip())
            logger.info(f"Query execution plan for: {query[:100]}...\n{plan_text}")
            logger.info(f"analyzed_plan\n{plan_text}")
        else:
            results = self.con.execute(query).fetchall()
             
        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        mem_change = mem_after - mem_before
        logger.info(f"Memory change: {mem_change:.2f} MB ({mem_before:.2f} MB → {mem_after:.2f} MB)")
        return results
    

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'con') and self.con:
            try:
                self.con.close()
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
        return False
    
    def close(self):
        """Explicitly close the connection if not using with statement"""
        if hasattr(self, 'con') and self.con:
            self.con.close()
            logger.info("Database connection closed successfully") 

    def verify(self, table: str):
        stock_columns = self.STOCK_COLUMNS
        verification_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            {', '.join(f'SUM(CASE WHEN {stock} IS NULL THEN 1 ELSE 0 END) as {stock}_null_count' for stock in stock_columns)}
        FROM {table} 
        WHERE date > (SELECT MIN(date) FROM price_wide)
        """
        result = self.con.execute(verification_query).fetchone()
        total_rows = result[0]
        logger.info(f"Verification: Table has {total_rows} rows")
        for i, stock in enumerate(stock_columns):
            null_count = result[i+1]
            if null_count == total_rows:
                logger.error(f"Column {stock} has all NULL values in the table")
            elif null_count > 0:
                logger.warning(f"Column {stock} has {null_count} NULL values out of {total_rows} in the table")
        
        logger.info("All returns calculated successfully")

class ModuleFileHandlerFilter(logging.Filter):
    """
    A filter that adds file handlers to loggers when they're first used.
    Each module gets its own log file based on its __name__.
    """
    def __init__(self, log_dir: Path, formatter: logging.Formatter):
        super().__init__()
        self.log_dir = log_dir
        self.formatter = formatter
        self.configured_loggers: Set[str] = set()
        
    def filter(self, record):
        logger_name = record.name
        
        if logger_name == "root" or logger_name in self.configured_loggers:
            return True
            
        logger = logging.getLogger(logger_name)
        
        has_file_handler = any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        
        if not has_file_handler:
            # Create a log file path based on the module name
            log_filename = f"{logger_name.replace('.', '_')}.log"
            log_file_path = self.log_dir / log_filename
            
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(self.formatter)
            logger.addHandler(file_handler)
            
            self.configured_loggers.add(logger_name)
        
        return True
 