
import hashlib
def hash_string(s):
    return hashlib.sha256((s).encode('utf-8')).hexdigest()