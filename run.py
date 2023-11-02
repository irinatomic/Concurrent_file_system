from datetime import datetime
import multiprocessing
import threading
import hashlib
import zlib
import yaml
import sys
import os

# Global dictionaries to store file and part information
file_register = {}
parts_register = {}

# Constants for the file and part register
READY = "ready"
FILENAME = "filename"
MD5_HASH = "md5_hash"
PARTS_COUNT = "parts_count"

# Constants from the yaml file
SYSTEM = "system"
STORAGE = "storage"
PART_SIZE = "part_size"
IO_PROCESSES = "io_processes"
PARTS_DIRECTORY = "parts_directory"
SAVED_DIRECTORY = "saved_directory"

with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

part_size = config[SYSTEM][PART_SIZE]  # Part size is 1 MB

# I/O PROCESSES
def process_file_part(arguments):
    part_id, part_data = arguments
    md5_hash = hashlib.md5(part_data).hexdigest()
    compressed_data = zlib.compress(part_data)

    # Write the compressed data to a file
    with open(os.path.join(config[STORAGE][PARTS_DIRECTORY], f"{part_id}.dat"), 'wb') as writer:
        writer.write(compressed_data)

    return part_id, md5_hash

# I/O PROCESSES 
def get_file_part(part_id):
    # Read the compressed data from a file
    with open(os.path.join(config[STORAGE][PARTS_DIRECTORY], f"{part_id}.dat"), 'rb') as reader:
        compressed_data = reader.read()
        part_data = zlib.decompress(compressed_data)
        md5_hash_check = hashlib.md5(part_data).hexdigest()
        
        if md5_hash_check == parts_register[part_id][MD5_HASH]:
            return part_data
    return None

# I/O PROCESSES 
def delete_file_part(part_filename: str) -> bool:
    file_path = os.path.join(config[STORAGE][PARTS_DIRECTORY], f"{part_filename}.dat")
    if os.path.exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)
        return True
    else:
        return False

# PUT
def put(filename, io_pool):
    file_id = len(file_register)
    file_register[file_id] = {FILENAME: filename, READY: False}

    with open(filename, 'rb') as file:
        num_parts = (os.path.getsize(filename) + part_size - 1) // part_size
        part_ids = list(range(num_parts))
        batch_size = 4

        for i in range(0, num_parts, batch_size):
            parts_data_tuples = []                                  # List of tuples (part_id, part_data)
            parts_ids_batch = part_ids[i:i + batch_size]
            for part_id in parts_ids_batch:
                part_data = file.read(part_size)
                if not part_data:
                    break
                parts_data_tuples.append((f"{file_id}_{part_id}", part_data))

            # Process the parts in parallel
            results = io_pool.map(process_file_part, parts_data_tuples)

            # Write the results to the parts register
            for full_part_id, md5_hash in results:
                parts_register[full_part_id] = {MD5_HASH: md5_hash, READY: True}

    if all(parts_register[f"{file_id}_{i}"][READY] for i in range(num_parts)):
        file_register[file_id][READY] = True
        file_register[file_id][PARTS_COUNT] = num_parts


# GET
def get(file_id_arg):
    file_id = int(file_id_arg)
    if file_id in file_register and file_register[file_id][READY]:
        parts_count = file_register[file_id][PARTS_COUNT]
        new_filename = f"new_file_{file_id}_{datetime.timestamp(datetime.now())}.txt"
        new_filepath = os.path.join(config[STORAGE][SAVED_DIRECTORY], new_filename)
        part_index = 0

        with open(new_filepath, 'wb') as writer:
            for i in range(parts_count):
                part_id = f"{file_id}_{i}"                                              # part_id = filename of the saved part
                if part_id in parts_register and parts_register[part_id][READY]:
                    part_data = get_file_part(part_id)
                    if not part_data:
                        sys.stdout.write(f"Error: Part {part_id} is corrupted\n")
                        break
                    writer.write(part_data)
                    part_index += 1
                else:
                    break

# DELETE
def delete(file_id_arg):
    file_id = int(file_id_arg)

    # mark the file and file parts as not ready
    if file_id in file_register and file_register[file_id][READY]:
        file_register[file_id][READY] = False
        parts_count = file_register[file_id][PARTS_COUNT]
        part_missing = False
        for i in range(parts_count):
            part_id = f"{file_id}_{i}"
            if part_id in parts_register:
                parts_register[part_id][READY] = False
            else:
                part_missing = True
                break
        
        if not part_missing:
            # let the U/I process delete the file part - success -> delete file part from parts_register
            success = True
            for i in range(parts_count):
                part_id = f"{file_id}_{i}"
                if delete_file_part(part_id):
                    del parts_register[part_id]
                else:
                    success = False
                    break
            if success:
                del file_register[file_id]
            else:
                sys.stdout.write("Error: Some parts could not be deleted\n")
        else:
            sys.stdout.write("Error: Some parts are missing\n")

# LIST
def list_files():
    for file_id in file_register:
        if file_register[file_id][READY]:
            sys.stdout.write(f"{file_id}: {file_register[file_id][FILENAME]}\n")

# Function to delete extra files:
#  - Files in the parts directory
#  - Files in the saved directory
def delete_extra_files():
    def delete_files_in_directory(directory):
        if os.path.exists(directory) and os.path.isdir(directory):
            files = os.listdir(directory)
            for file in files:
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                else:
                    sys.stdout.write(f"Skipping non-file: {file_path}")
        else:
            sys.stdout.write(f"Directory does not exist: {directory}")

    parts_directory = config[STORAGE][PARTS_DIRECTORY]
    saved_directory = config[STORAGE][SAVED_DIRECTORY]

    delete_files_in_directory(parts_directory)
    delete_files_in_directory(saved_directory)

def main(io_pool):
    threads = []

    while True:
        sys.stdout.write("Enter a command: ")
        command = input()
        parts = command.split()
        action = parts[0]

        if action == "put" and len(parts) == 2:
            t = threading.Thread(target=put, args=(parts[1], io_pool))
            t.start()
            threads.append(t)
        elif action == "get" and len(parts) == 2:
            t = threading.Thread(target=get, args=(parts[1],))
            t.start()
            threads.append(t)
        elif action == "delete" and len(parts) == 2:
            t = threading.Thread(target=delete, args=(parts[1],))
            t.start()
            threads.append(t)
        elif action == "list" and len(parts) == 1:
            t = threading.Thread(target=list_files)
            t.start()
            threads.append(t)
        elif action == "exit":
            sys.stdout.write("Exiting...\n")
            break
        else:
            sys.stdout.write("Invalid command\n")

    io_pool.terminate()             # Doesn't wait for tasks to finish - kills all processes
    for thread in threads:
        thread.join()

    delete_extra_files()

if __name__ == "__main__":
    io_pool = multiprocessing.Pool(processes=config[SYSTEM][IO_PROCESSES])
    main(io_pool)
