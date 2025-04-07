# language
python

# tool
Duckdb

# summary
(1) read all csv files into single table that persists on disk (ingest.py); 
(2) create and update wide tables filtering for stock id's in batches (transform.py); 
(3) calculate returns in batches as well.

# Requirements

uv must be installed

run `uv sync`

environment variable CSV_FILE_DIR (path to files) must be set (assumes local filesystem)

ex: `export CSV_FILE_DIR=<FILE_DIR>'

# Execution

(1) `uv run python -m src.ingest`

(2) `uv run python -m src.transform --table price` 
    `uv run python -m src.transform --table trade_volume` 

(3) `uv run python -m src.returns.py`

# logging
Create a logs directory at project root: `mkdir logs`
files will be populated in that directory during execution
