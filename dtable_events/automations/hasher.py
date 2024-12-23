import base64
import logging
import os
try:
    from Crypto.Cipher import AES
except ImportError:
    AES = None

from dtable_events.app.config import SECRET_KEY

class AESPasswordDecodeError(Exception):
    pass

# the block size for the cipher object; must be 16, 24, or 32 for AES
BLOCK_SIZE = 32

# the character used for padding--with a block cipher such as AES, the value
# you encrypt must be a multiple of BLOCK_SIZE in length.  This character is
# used to ensure that your value is always a multiple of BLOCK_SIZE
PADDING = '{'

# one-liner to sufficiently pad the text to be encrypted
pad = lambda s: s + (16 - len(s) % 16) * PADDING

# one-liners to encrypt/encode and decrypt/decode a string
# encrypt with AES, encode with base64
EncodeAES = lambda c, s: base64.b64encode(c.encrypt(pad(s).encode('utf-8'))).decode('utf-8')
DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).decode('utf-8').rstrip(PADDING)

def _force_str(s, encoding='utf-8', strings_only=False, errors='strict'):
    if strings_only and isinstance(s, (bytes, bytearray)):
        raise TypeError("Cannot handle bytes when strings_only is set to True.")
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    elif isinstance(s, bytearray):
        return s.decode(encoding, errors).encode(encoding)
    elif isinstance(s, (list, tuple)):
        return [_force_str(item, encoding, strings_only, errors) for item in s]
    elif isinstance(s, dict):
        return {_force_str(key, encoding, strings_only, errors): _force_str(value, encoding, strings_only, errors) for key, value in s.items()}
    else:
        return str(s)

class AESPasswordHasher:
    algorithm = 'aes'

    def __init__(self, secret=None):
        if not secret:
            secret = SECRET_KEY[:BLOCK_SIZE]
        self.cipher = AES.new(secret.encode('utf-8'), AES.MODE_ECB)

    def encode(self, password):
        password = _force_str(password)
        return "%s$%s" % (self.algorithm, EncodeAES(self.cipher, password))

    def decode(self, encoded):
        algorithm, data = encoded.split('$', 1)
        if algorithm != self.algorithm:
            raise AESPasswordDecodeError
        data = data.encode('utf-8')

        return DecodeAES(self.cipher, data)
