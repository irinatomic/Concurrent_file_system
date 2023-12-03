# Concurrent File System
This project was done as homework for a UNiversity subject "PArallel programming".

The project implements a parallel file storage system. It includes components such as a File Registry, Part Registry, and a Command Acceptance Thread. The system is capable of storing files by dividing them into specific-sized parts, compressing, and storing each part separately.

## Overview
This project entails the implementation of a parallel file storage system. The system comprises several components:

### File Registry
Maintains a list of file names stored in the system, unique identifiers, and their status (ready for use or not). Optionally, additional file data may be stored in this registry.

### Part Registry
Contains a list of all parts of files added to the system. It includes unique part identifiers, unique file identifiers to which they belong, the part's sequence number in the file, and MD5 hash content. Additional data may be stored optionally.

### Command Acceptance Thread
The main thread in the primary process solely accepts commands from the command line. A separate thread is spawned for each received command (except for the shutdown command).

### Commands
- `put`
- `get`
- `delete`
- `list`
- `exit`

## Implementation Details
The system is implemented as a Python script named `run.py`. It involves:

- Accepting and processing commands through the Command Acceptance Thread.
- Handling `put`, `get`, `delete`, `list`, and `exit` commands as specified in the project requirements.
- Ensuring parallel processing of commands, particularly managing memory consumption during `put` and `get` operations.

## Configuration
The system's configuration is managed through a YAML file, allowing customization of:
- Path to the directory storing file parts
- Number of Input/Output (I/O) processes
- Maximum memory allocation for the system

Please refer to the project's Python script `run.py` and the associated codebase for detailed implementation.
