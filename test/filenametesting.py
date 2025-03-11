import os
import time
import shutil
import subprocess

# Define the test directory
TEST_DIR = "openmsistream_test"

# Define a list of filenames with special characters
FILENAMES = [
    "normal_file.txt",  # Normal filename
    "space file.txt",  # Space in filename
    "semicolon;file.txt",  # Semicolon in filename
    "quote'file.txt",  # Single quote
    'double"quote.txt',  # Double quote
    "asterisk*file.txt",  # Asterisk
    "question?mark.txt",  # Question mark
    "pipe|file.txt",  # Pipe character
    "ampersand&file.txt",  # Ampersand
    "percent%file.txt",  # Percent symbol
    "newline\nfile.txt",  # Newline (should fail to create)
    "tab\tfile.txt",  # Tab character (should fail to create)
    "slash/file.txt",  # Forward slash (invalid on Linux)
    "backslash\\file.txt",  # Backslash (problematic on Windows)
]


# Function to create test files
def create_test_files():
    """Creates test files with various names in the test directory."""
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)  # Clean up previous test runs
    os.makedirs(TEST_DIR, exist_ok=True)

    for filename in FILENAMES:
        file_path = os.path.join(TEST_DIR, filename)
        try:
            with open(file_path, "w") as f:
                f.write("Test content")
            print(f"Created: {file_path}")
        except Exception as e:
            print(f"Failed to create '{file_path}': {e}")


# Function to start OpenMSIStream and monitor the test directory
def start_openmsistream():
    """Starts OpenMSIStream and logs reactions to filenames."""
    try:
        # Assuming OpenMSIStream is installed and accessible via CLI
        command = ["openmsistream", "--dir", TEST_DIR]
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        print("\nOpenMSIStream started. Monitoring directory...\n")
        time.sleep(5)  # Allow some time for processing

        process.terminate()  # Stop OpenMSIStream after a short time
        stdout, stderr = process.communicate()

        print("\n--- OpenMSIStream Output ---")
        print(stdout)
        print("\n--- Errors (if any) ---")
        print(stderr)

    except FileNotFoundError:
        print(
            "Error: OpenMSIStream not found. Ensure it is installed and available in your PATH."
        )


# Run the test
if __name__ == "__main__":
    create_test_files()
    start_openmsistream()
