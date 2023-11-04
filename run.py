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
file_counter = 0

# Create an additional dictionary to store locks for every file in file_register.
# Locks for individual parts are not needed since only one thread accesses the parts while processing the file.
file_register_locks = {}

# Counter for the file_parts in RAM
memory_counter = 0
memory_condition = threading.Condition()
memory_counter_lock = threading.Lock()

# Constants for the file and part register
READY = "ready"
FILENAME = "filename"
MD5_HASH = "md5_hash"
PARTS_COUNT = "parts_count"

# Constants from the yaml file
RAM = "ram"
SYSTEM = "system"
STORAGE = "storage"
PART_SIZE = "part_size"
IO_PROCESSES = "io_processes"
PARTS_DIRECTORY = "parts_directory"
SAVED_DIRECTORY = "saved_directory"

with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

part_size = config[SYSTEM][PART_SIZE]                   # Part size is 1 MB
batch_size = 4                                          # Batch size for processing parts in parallel

# I/O PROCESSES
def put_file_part(arguments):
    full_part_id, part_data = arguments
    md5_hash = hashlib.md5(part_data).hexdigest()
    compressed_data = zlib.compress(part_data)

    # Write the compressed data to a file
    with open(os.path.join(config[STORAGE][PARTS_DIRECTORY], f"{full_part_id}.dat"), 'wb') as writer:
        writer.write(compressed_data)

    return full_part_id, md5_hash

# I/O PROCESSES 
def get_file_part(arguments):
    global parts_register
    full_part_id, previous_md5_hash = arguments
    file_path = os.path.join(config[STORAGE][PARTS_DIRECTORY], f"{full_part_id}.dat")

    if os.path.exists(file_path):
        with open(file_path, 'rb') as reader:
            compressed_data = reader.read()
            part_data = zlib.decompress(compressed_data)
            md5_hash_check = hashlib.md5(part_data).hexdigest()

            if md5_hash_check == previous_md5_hash:
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
def put(filename, io_pool, file_id):
    global file_register
    global parts_register
    global memory_counter

    ispis = f"{file_id} PUT\n"
    sys.stdout.write(ispis)
    

    file_register_locks[file_id].acquire()
    file_register[file_id] = {FILENAME: filename, READY: False}

    with open(filename, 'rb') as file:
        num_parts = (os.path.getsize(filename) + part_size - 1) // part_size
        part_ids = list(range(num_parts))
        extra_memory = batch_size * config[SYSTEM][PART_SIZE]

        for i in range(0, num_parts, batch_size):
            
            memory_condition.acquire()
            while True:
                
                sys.stdout.write(str(file_id)) 
                memory_counter_lock.acquire()

                if memory_counter + extra_memory <= config[SYSTEM][RAM]:

                    memory_counter += extra_memory

                    ispis = f"{file_id} {memory_counter} THEN\n"
                    sys.stdout.write(ispis)

                    memory_counter_lock.release()
                    memory_condition.release()

                    break
                else:
                    ispis = f"{file_id} {memory_counter} ELSE\n"
                    sys.stdout.write(ispis)

                    memory_counter_lock.release()
                    memory_condition.wait()

            parts_data_tuples = []                                  # List of tuples (part_id, part_data)
            parts_ids_batch = part_ids[i:i + batch_size]
            for part_id in parts_ids_batch:
                part_data = file.read(part_size)
                if not part_data:
                    break
                parts_data_tuples.append((f"{file_id}_{part_id}", part_data))

            # Process the parts in parallel
            results = io_pool.map(put_file_part, parts_data_tuples)

            # Notify everyone we are done
            memory_counter_lock.acquire()
            memory_counter -= extra_memory
            memory_counter_lock.release()
            memory_condition.acquire()
            memory_condition.notify_all()
            memory_condition.release()

            # Write the results to the parts register
            for full_part_id, md5_hash in results:
                parts_register[full_part_id] = {MD5_HASH: md5_hash, READY: True}

    if all(parts_register[f"{file_id}_{i}"][READY] for i in range(num_parts)):
        file_register[file_id][READY] = True
        file_register[file_id][PARTS_COUNT] = num_parts
    file_register_locks[file_id].release()

# GET
def get(file_id_arg, io_pool):
    global file_register
    global parts_register
    file_id = int(file_id_arg)

    file_register_locks[file_id].acquire()

    if file_id in file_register and file_register[file_id][READY]:
        num_parts = file_register[file_id][PARTS_COUNT]
        part_ids = list(range(num_parts))

        # Create a file to write in
        new_filename = f"new_file_{file_id}_{datetime.timestamp(datetime.now())}.txt"
        new_filepath = os.path.join(config[STORAGE][SAVED_DIRECTORY], new_filename)

        with open(new_filepath, 'wb') as writer:
            for i in range(0, num_parts, batch_size):
                parts_data_tuples = []                              # List of tuples (part_id, md5_hash)
                part_ids_batch = part_ids[i:i + batch_size]
                for part_id in part_ids_batch:
                    full_part_id = f"{file_id}_{part_id}"
                    md5_hash = parts_register[full_part_id][MD5_HASH]
                    parts_data_tuples.append((full_part_id, md5_hash))

                # Process the parts in parallel
                results = io_pool.map(get_file_part, parts_data_tuples)

                # If the part is corrupted, the part_data is None
                for part_data in results:
                    if not part_data:
                        sys.stdout.write(f"Error: Some parts are missing or corrupted\n")
                        file_register_locks[file_id].release()
                        return
                    writer.write(part_data)

    file_register_locks[file_id].release()

# DELETE
def delete(file_id_arg, io_pool):
    global file_register
    global parts_register
    file_id = int(file_id_arg)

    file_register_locks[file_id].acquire()

    if file_id in file_register and file_register[file_id][READY]:
        # mark the file and file parts as not ready
        file_register[file_id][READY] = False
        parts_count = file_register[file_id][PARTS_COUNT]

        # Split parts into chunks
        full_part_ids = [f"{file_id}_{i}" for i in range(parts_count)]
        chunk_size = 4

        for i in range(0, parts_count, chunk_size):
            chunk = full_part_ids[i:i + chunk_size]                         # List of 4 full_part_ids = part_filename
            results = io_pool.map(delete_file_part, chunk)

            # Check if all parts in the chunk were successfully deleted
            if all(results):
                # Remove deleted parts from parts_register
                for full_part_id in chunk:
                    if full_part_id in parts_register:
                        del parts_register[full_part_id]
            else:
                sys.stdout.write("Error: Some parts could not be deleted\n")
                
        file_register_locks[file_id].release()
    else:
        file_register_locks[file_id].release()

# LIST
def list_files():
    for file_id in file_register:
        if file_register[file_id][READY]:
            sys.stdout.write(f"{file_id}: {file_register[file_id][FILENAME]}\n")

# Delete created files
def delete_extra_files():
    sys.stdout.write(str(memory_counter))
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
    global file_counter
    threads = []

    while True:
        sys.stdout.write("Enter a command: ")
        command = input()
        parts = command.split()
        action = parts[0]

        if action == "put" and len(parts) == 2:
            file_counter += 1
            file_register_locks[file_counter] = threading.Lock()
            t = threading.Thread(target=put, args=(parts[1], io_pool, file_counter))
            t.start()
            threads.append(t)
        elif action == "get" and len(parts) == 2:
            t = threading.Thread(target=get, args=(parts[1], io_pool))
            t.start()
            threads.append(t)
        elif action == "delete" and len(parts) == 2:
            t = threading.Thread(target=delete, args=(parts[1], io_pool))
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

    delete_extra_files()
    io_pool.terminate()             # Doesn't wait for tasks to finish - kills all processes
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    io_pool = multiprocessing.Pool(processes=config[SYSTEM][IO_PROCESSES])
    main(io_pool)
