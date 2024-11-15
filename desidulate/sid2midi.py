import argparse
import multiprocessing
import pathlib
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import os

BASE_TIMEOUT = 240

def log_unprocessed_file(file_path, reason):
    """
    Log files that could not be processed to a log file.
    """
    with open('timeout_log.txt', 'a') as log_file:
        log_file.write(f"{file_path}: {reason}\n")

def log_processing_time(file_path, status, reg2ssf_time=None, ssf2midi_time=None, total_time=None):
    """Log processing time details to a log file."""
    with open('processing_times_log.txt', 'a') as log_file:
        log_file.write(f"{file_path}, {status}, reg2ssf_time: {reg2ssf_time:.2f} s, "
                       f"ssf2midi_time: {ssf2midi_time:.2f} s, total_time: {total_time:.2f} s\n")

def get_dynamic_timeout(file_path):
    """Calculate dynamic timeout based on file size."""
    file_size = os.path.getsize(file_path)
    # Adjust timeout based on file size (e.g., 1 second per MB)
    return BASE_TIMEOUT + (file_size // (1024 * 1024))

def process_parquet(parquet_file):
    start_time = time.time()
    parquet_file_str = str(parquet_file)

    reg2ssf_time = None
    ssf2midi_time = None

    try:
        # Calculate a dynamic timeout
        timeout = get_dynamic_timeout(parquet_file_str)

        # Run 'reg2ssf' with the Parquet filename and '--dump_type jsid' argument
        print(f"Processing {parquet_file} with reg2ssf...")
        reg2ssf_start = time.time()
        result = subprocess.run(['reg2ssf', parquet_file_str, '--dump_type', 'jsid'],
                                check=True, capture_output=True, text=True,
                                timeout=timeout
                                )
        reg2ssf_time = time.time() - reg2ssf_start
        print(f"Sucessfully processed {parquet_file} with reg2ssf after {reg2ssf_time:.2f} seconds.")

        # Retrieve 'Path/to/file/log.zst' from logging output of reg2ssf; Note: This info is in stderr not stdout!
        output_lines = result.stderr.strip().splitlines()
        for line in output_lines:
            if "log.zst" in line:
                filename = line.split('writing ')[-1].strip()

        # Pass the 'Path/to/file/log.zst' filename to 'ssf2midi' and run
        print(f"Processing {filename} with ssf2midi...")
        ssf2midi_start = time.time()
        subprocess.run(['ssf2midi', filename], capture_output=True, text=True, timeout=timeout)
        ssf2midi_time = time.time() - ssf2midi_start
        print(f"Successfully processed {filename} with ssf2midi in {ssf2midi_time:.2f} seconds.")

        # Log success with processing times
        total_time = time.time() - start_time
        log_processing_time(parquet_file_str, "Success", reg2ssf_time, ssf2midi_time, total_time)
        return f"Successfully processed {parquet_file}"

    except subprocess.TimeoutExpired:
        print(f"Timeout processing {parquet_file}")
        log_unprocessed_file(parquet_file, "Timeout expired")
        total_time = time.time() - start_time
        log_processing_time(parquet_file_str, "Timeout", total_time=total_time)


    except subprocess.CalledProcessError as e:
        print(f"Error processing {parquet_file}: {e}")
        log_unprocessed_file(parquet_file, f"Process error: {e}")
        total_time = time.time() - start_time
        log_processing_time(parquet_file_str, "Error", total_time=total_time)

    except Exception as e:
        print(f"Unexpected error with {parquet_file}: {e}")
        log_unprocessed_file(parquet_file, f"Unexpected error: {e}")
        total_time = time.time() - start_time
        log_processing_time(parquet_file_str, "Error", total_time=total_time)

    finally:
        elapsed_time = time.time() - start_time
        print(f"Processing time for {parquet_file_str}: {elapsed_time:.2f} seconds")

def create_parquet_files(recordingtool, threads, hvsc_path, root_path):

    # Define the command and arguments
    command = [
        recordingtool ,  # The shell script
        '--audio', 'SID_REG',  # Audio option
        '--maxThreads', threads,  # Max threads
        '--hvsc', hvsc_path,  # HVSC path
        root_path  # Additional path argument, if only certain folders are supposed to be converted
    ]

    # Run the shell command using subprocess
    print("Running recordingtool.sh to generate Parquet files...")
    try:
        subprocess.run(" ".join(command), shell=True, check=True)
        print("Parquet files generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error while running recordingtool.sh: {e}")
        exit(1)


def process_parquet_files(root_path):
    # Directory to search for Sid Files as specified
    current = pathlib.Path(root_path)

    # Find all Parquet files in the root_path, excluding the 'DOCUMENTS' folder.
    parquet_files = sorted(
        [file for file in current.rglob('*.parquet') if 'DOCUMENTS' not in file.parts]
    )
    print(f"{len(parquet_files)} Parquet files were found.")

    # Use a ProcessPoolExecutor to process files in parallel
    max_workers = os.cpu_count() or 8
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks to process each CSV file
        futures = {executor.submit(process_parquet, parquet_file): parquet_file for parquet_file in parquet_files}

        # Collect and print results as they complete
        for future in as_completed(futures):
            result = future.result()
            print(result)

def main():
    # TODO: Adjust the defaults to my final folder structure.
    # Note: Check in recordingtool.sh the path to the jar when running into problems.
    parser = argparse.ArgumentParser()
    parser.add_argument('--hvscdir', default='.', type=str,
                        help='Path to HVSC C64Music folder. Default is working directory.')
    parser.add_argument('--recordingtool', default = './jsidplay2-4.10-mac/recordingtool.sh', type = str,
                        help='Path to recordingtool.sh, default expects jsidplay2 in working directory')
    parser.add_argument('--threads', default=8,
                        help='Number of max threads for recordingtool. Default is 8')
    parser.add_argument('--folder', default='.', type=str,
                        help='Folder to convert. Default is working directory.')

    args = parser.parse_args()

    create_parquet_files(args.recordingtool, args.threads, args.hvscdir, args.folder)

    process_parquet_files(args.folder)


if __name__ == '__main__':
    main()