import pandas as pd
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import numpy as np

# Configure logging
logging.basicConfig(
    filename='csv_to_parquet.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def convert_csv_to_parquet(csv_file_path):
    """
    Converts a single CSV file to Parquet format and saves it in the same directory.
    """

    try:
        # Define the Parquet file path (same directory as CSV)
        parquet_file_path = csv_file_path.with_suffix('.parquet')

        # Read CSV file
        df = pd.read_csv(csv_file_path,
                         usecols=["clock_offset", "reg", "val"],
                         encoding='iso-8859-1',
                         header=0,
                         # match the naming in desidulate
                         names=["clock", "clock_offset", "reg", "val", "description"],
                         skipinitialspace=True,
                         dtype={'clock': np.uint64, 'clock_offset': np.uint64}
                         )

        # Convert to Parquet with Zstd compression
        df.to_parquet(parquet_file_path, engine='pyarrow', compression='zstd')

        logging.info(f"Successfully converted '{csv_file_path}' to '{parquet_file_path}'")
        print(f"Converted '{csv_file_path}' to '{parquet_file_path}'")

    except Exception as e:
        logging.error(f"Failed to convert '{csv_file_path}': {e}")
        print(f"Failed to convert '{csv_file_path}': {e}")


def find_csv_files(root_folder):
    """
    Recursively finds all CSV files in the given folder and its subfolders.
    """
    return list(Path(root_folder).rglob('*.csv'))


def main(input_folder):

    # Find all CSV files recursively
    csv_files = find_csv_files(input_folder)
    if not csv_files:
        print("No CSV files found.")
        return

    # Use multi-threading to convert CSV files to Parquet
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(convert_csv_to_parquet, csv_file): str(csv_file) for csv_file in csv_files}

        for future in as_completed(futures):
            csv_file = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing file {csv_file}: {e}")
                print(f"Error processing file {csv_file}: {e}")


if __name__ == "__main__":
    # Check if correct number of arguments are provided
    if len(sys.argv) != 2:
        print("Usage: python3 csv_to_parquet.py <input_folder>")
        sys.exit(1)

    input_folder = sys.argv[1]
    main(input_folder)