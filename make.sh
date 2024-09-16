#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the build directory
BUILD_DIR="build"

# Create the build directory if it doesn't exist
mkdir -p "$BUILD_DIR"

# Navigate to the build directory
cd "$BUILD_DIR"

# Configure the project
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Build the project
cmake --build .

# Navigate back to the original directory
cd ..

# Copy the header files to /usr/include/
sudo cp -r ./include/leveldb /usr/include/

# Copy the built library to /usr/lib/
sudo cp build/libleveldb.a /usr/lib/

echo "Build and installation completed successfully."
