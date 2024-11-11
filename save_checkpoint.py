import os
import shutil
import sys

def save_checkpoint(checkpoint_name):
    # Define paths for the directory and files to be copied
    results_dir = 'results'
    checkpoint_dir = os.path.join(results_dir, checkpoint_name)
    
    # Paths of the files to copy
    files_to_copy = [
        'iostat_results.csv',
        'mpstat_results.csv',
        'logs/client_stats.log',
        'logs/memtable_size.log'
    ]

    # Path of the LOG file in /home/{USER}/ycsb-rocksdb-data/
    user = os.getenv("USER")
    if user is None:
        print("USER environment variable is not set.")
        sys.exit(1)
    log_file = f'/home/{user}/ycsb-rocksdb-data/LOG'

    # Create the checkpoint directory under results/
    try:
        os.makedirs(checkpoint_dir, exist_ok=True)
        print(f"Checkpoint directory created: {checkpoint_dir}")
    except Exception as e:
        print(f"Error creating checkpoint directory: {e}")
        sys.exit(1)

    # Copy each file
    for file in files_to_copy:
        try:
            shutil.copy(file, checkpoint_dir)
            print(f"Copied {file} to {checkpoint_dir}")
        except FileNotFoundError:
            print(f"File {file} not found. Skipping.")
        except Exception as e:
            print(f"Error copying {file}: {e}")

    # Copy the LOG file
    try:
        shutil.copy(log_file, checkpoint_dir)
        print(f"Copied LOG file to {checkpoint_dir}")
    except FileNotFoundError:
        print(f"LOG file not found at {log_file}. Skipping.")
    except Exception as e:
        print(f"Error copying LOG file: {e}")

if __name__ == "__main__":
    # python save_checkpoint.py <checkpoint_name>
    if len(sys.argv) < 2:
        print("Usage: python save_checkpoint.py <checkpoint_name>")
        sys.exit(1)

    checkpoint_name = sys.argv[1]
    save_checkpoint(checkpoint_name)