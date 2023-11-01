import threading
import sys

def put(filename):
    sys.stdout.write("putting file: " + filename + "\n")

def get(filename):
    sys.stdout.write("getting file: " + filename + "\n")

def delete(filename):
    sys.stdout.write("deleting file: " + filename + "\n")

def list_files():
    sys.stdout.write("listing files\n")

def main():
    threads = []

    while True:
        command = input("Enter a command: ")
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

if __name__ == "__main__":
    main()
