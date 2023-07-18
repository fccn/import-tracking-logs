# Custom s3 Manager
This script provides functionality to manage files in an S3 bucket and perform various operations such as listing files and folders, downloading files, verifying checksums, extracting compressed files, and sending files to an endpoint.

### Prerequisites

Before running the script, ensure that you have the following prerequisites:

Python 3.x installed

### Installation

1. Clone the repository or download the script files to your local machine.

2. Install the necessary packages by using this command:
```
pip install -r requirements.txt
```


### Additional Information

- The Uncompress class provides a method to extract files from a compressed archive.
- The calculate_checksum function calculates the MD5 checksum of a file.
- Ensure you have the necessary access keys and endpoint URLs configured in your environment variables or modify the code to provide them directly.