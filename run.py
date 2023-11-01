import threading
import hashlib
import zlib
import yaml
import sys
import os

file_register = {}
parts_register = {}

with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

def process_file_part(part_id, part_data):
    md5_hash = hashlib.md5(part_data).hexdigest()
    compressed_data = zlib.compress(part_data)
    # write the compressed data to a file
    with open(os.path.join(config['storage']['parts_directory'], f"{part_id}.dat"), 'wb') as writer:
        writer.write(compressed_data)
    return part_id, md5_hash

def put(filename):
    file_id = len(file_register)
    file_register[file_id] = {"filename": filename, "ready": False}
    part_size = config['system']['part_size']

    with open(filename, 'rb') as file:
        part_index = 0
        while True:
            part_data = file.read(part_size)
            if not part_data:
                break                               # EOF
            
            part_id = f"{file_id}_{part_index}"
            parts_register[part_id] = {"ready": False}

            part_id, md5_hash = process_file_part(part_id, part_data)
            parts_register[part_id]["md5_hash"] = md5_hash
            parts_register[part_id]["ready"] = True
            part_index += 1

    # Check if the whole file is ready
    if all(parts_register[f"{file_id}_{i}"]["ready"] for i in range(part_index)):
        file_register[file_id]["ready"] = True


def get(filename):
    sys.stdout.write("getting file: " + filename + "\n")

def delete(filename):
    sys.stdout.write("deleting file: " + filename + "\n")

def list_files():
    sys.stdout.write("listing files\n")

# Delete all files in the parts directory and saved directory
def delete_extra_files():
    parts_directory = config['storage']['parts_directory']
    if os.path.exists(parts_directory) and os.path.isdir(parts_directory):
        files = os.listdir(parts_directory)
        for file in files:
            file_path = os.path.join(parts_directory, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                sys.stdout.write(f"Skipping non-file: {file_path}")
    else:
        sys.stdout.write(f"Directory does not exist: {parts_directory}")

    saved_directory = config['storage']['saveddirectory']
    if os.path.exists(saved_directory) and os.path.isdir(saved_directory):
        files = os.listdir(saved_directory)
        for file in files:
            file_path = os.path.join(saved_directory, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                sys.stdout.write(f"Skipping non-file: {file_path}")
    else:
        sys.stdout.write(f"Directory does not exist: {saved_directory}")

def main():
    threads = []

    while True:
        sys.stdout.write("Enter a command: ")
        command = input()
        parts = command.split()
        action = parts[0]

        if action == "put" and len(parts) == 2:
            t = threading.Thread(target=put, args=(parts[1],))
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

    for thread in threads:
        thread.join()

    delete_extra_files()

if __name__ == "__main__":
    main()
