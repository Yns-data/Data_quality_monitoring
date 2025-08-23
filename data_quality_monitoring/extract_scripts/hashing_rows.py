# type: ignore
import hashlib

def hash_row(row):
    row_str = str(tuple(row.values))
    hash_object = hashlib.sha256(row_str.encode())
    hash_int = int(hash_object.hexdigest(), 16)
    return str(hash_int % 10**8).zfill(8)