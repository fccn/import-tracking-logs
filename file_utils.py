import hashlib
import gzip

class Uncompress:
    @staticmethod
    def extract_files(file):
        try:
            with gzip.open(file, 'rb') as f_in:
                uncompressed_file = file.rstrip('.gz')
                with open(uncompressed_file, 'wb') as f_out:
                    f_out.write(f_in.read())
        except OSError as e:
            print(f"Error extracting files: {str(e)}")
                
def calculate_checksum(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as file:
        for byte_block in iter(lambda: file.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

