from datetime import datetime
import os
import random
import pandas as pd
import math

# Log-uniform distribution (more realistic for trade volumes)
def log_uniform(min_val, max_val):
    """Generate a random number with log-uniform distribution."""
    log_min = math.log(min_val)
    log_max = math.log(max_val)
    return int(math.exp(random.uniform(log_min, log_max)))

def generate_stock_data_files(
    num_files=3,
    file_size_gb=2,
    num_stocks=200,
    start_date="2000-01-01",
    end_date="2025-12-31",
    output_dir="./data"
):
    """
    Generate stock data files with the specified parameters.
    
    Args:
        num_files: Number of files to generate
        file_size_gb: Approximate size of each file in GB
        num_stocks: Number of unique stock IDs
        start_date: Start date for the data
        end_date: End date for the data
        output_dir: Directory to save the generated files
    """
    print(f"Generating {num_files} files of ~{file_size_gb}GB each with {num_stocks} stocks")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Calculate date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = pd.date_range(start=start, end=end, freq='B')  # Business days
    
    # Calculate row size in bytes
    # id (int): ~4 bytes
    # date (date): ~4 bytes
    # price (float): ~8 bytes
    # trade_volume (int): ~4 bytes
    # CSV overhead (commas, newlines): ~5 bytes
    bytes_per_row = 25
    
    # Calculate rows per file
    rows_per_file = int((file_size_gb * 1024 * 1024 * 1024) / bytes_per_row)
    
    # Calculate dates per file (ensuring we have enough dates across all files)
    total_dates_needed = math.ceil(rows_per_file * num_files / num_stocks)
    
    if total_dates_needed > len(date_range):
        print(f"Warning: Not enough dates to generate {num_files} files of {file_size_gb}GB each.")
        print(f"Available dates: {len(date_range)}, Needed: {total_dates_needed}")
        print(f"Either reduce num_files, reduce file_size_gb, or increase date range.")
        dates_per_file = len(date_range) // num_files
    else:
        dates_per_file = math.ceil(total_dates_needed / num_files)
    
    # Generate initial stock prices (between $10 and $1000)
    stock_prices = {}
    for stock_id in range(1, num_stocks + 1):
        stock_prices[stock_id] = random.uniform(10, 1000)
    
    date_chunks = [date_range[i:i + dates_per_file] for i in range(0, len(date_range), dates_per_file)]
    
    # Generate files
    for file_idx in range(num_files):
        if file_idx >= len(date_chunks):
            print(f"Warning: Not enough date chunks for file {file_idx+1}. Skipping.")
            continue
            
        file_dates = date_chunks[file_idx]
        if len(file_dates) == 0:
            print(f"Warning: No dates available for file {file_idx+1}. Skipping.")
            continue
            
        filename = os.path.join(output_dir, f"stock_data_{file_idx+1}.csv")
        print(f"Generating {filename} with {len(file_dates)} dates × {num_stocks} stocks = {len(file_dates) * num_stocks:,} rows")
        
        with open(filename, 'w') as f:
            # Write header
            f.write("id,date,price,trade_volume\n")
            
            # Generate data for each date and stock
            for date in file_dates:
                for stock_id in range(1, num_stocks + 1):
                    # Update price with realistic movement (daily volatility of ~1-2%)
                    if date != file_dates[0] or file_idx == 0:  # Skip price update for first date of first file
                        price_change = stock_prices[stock_id] * random.uniform(-0.02, 0.02)
                        stock_prices[stock_id] += price_change
                        # Ensure price doesn't go too low
                        stock_prices[stock_id] = max(1.0, stock_prices[stock_id])
                    
                    # Generate realistic trade volume (1,000 to 500,000,000 shares)
                    trade_volume = log_uniform(1000, 500_000_000)
                    
                    # Write row
                    f.write(f"{stock_id},{date.strftime('%Y-%m-%d')},{stock_prices[stock_id]:.2f},{trade_volume}\n")
        
        # Get actual file size
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"Generated {filename}: {file_size_mb:.2f} MB")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate stock data files")
    parser.add_argument("--num-files", type=int, default=2, help="Number of files to generate")
    parser.add_argument("--file-size-gb", type=float, default=2, help="Approximate size of each file in GB")
    parser.add_argument("--num-stocks", type=int, default=200, help="Number of unique stock IDs")
    parser.add_argument("--start-date", type=str, default="1970-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2025-12-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="./data", help="Output directory")
    
    args = parser.parse_args()
    
    generate_stock_data_files(
        num_files=args.num_files,
        file_size_gb=args.file_size_gb,
        num_stocks=args.num_stocks,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir
    )