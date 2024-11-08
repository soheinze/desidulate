import argparse
import multiprocessing
import pathlib
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

MAX_WORKERS = int(multiprocessing.cpu_count() / 2)

def process_csv(csvfile):
        try:
            # Run 'reg2ssf' with the CSV filename and '--dump_type jsid' argument
            print(f"Processing {csvfile} with reg2ssf...")
            result = subprocess.run(['reg2ssf', csvfile, '--dump_type', 'jsid'],
                                    check=True, capture_output=True, text=True
                                    )

            # Retrieve 'Path/to/file/log.zst' from logging output of reg2ssf; Note: This info is in stderr not stdout!
            output_lines = result.stderr.strip().splitlines()
            for line in output_lines:
                if "log.zst" in line:
                    filename = line.split('writing ')[-1].strip()

            # Pass the 'Path/to/file/log.zst' filename to 'ssf2midi' and run
            print(f"Processing {filename} with ssf2midi...")
            subprocess.run(['ssf2midi', filename], capture_output=True, text=True)
            return f"Successfully processed {csvfile}"

        except:
            print(f"Something went wrong with {csvfile}.")


def automate_process(root_path):
    # Directory to search for Sid Files as specified
    current = pathlib.Path(root_path)

    # Find all CSV files in the root_path, excluding the 'DOCUMENTS' folder.
    csv_files = sorted(
        [file for file in current.rglob('*.csv') if 'DOCUMENTS' not in file.parts]
    )
    print(f"{len(csv_files)} CSV files were found.")

    # Use a ProcessPoolExecutor to process files in parallel
    with ProcessPoolExecutor() as executor:
        # Submit tasks to process each CSV file
        futures = {executor.submit(process_csv, csv_file): csv_file for csv_file in csv_files}

        # Collect and print results as they complete
        for future in as_completed(futures):
            result = future.result()
            print(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hvscdir', default='.', type=str,
                        help='Path to HVSC or subfolder. Default is current working directory.')
    args = parser.parse_args()
    automate_process(args.hvscdir)


if __name__ == '__main__':
    main()