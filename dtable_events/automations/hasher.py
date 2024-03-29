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

class AESPasswordHasher:
    algorithm = 'aes'

    def __init__(self, secret=None):
        if not secret:
            secret = SECRET_KEY[:BLOCK_SIZE]
        self.cipher = AES.new(secret.encode('utf-8'), AES.MODE_ECB)

    def decode(self, encoded):
        algorithm, data = encoded.split('$', 1)
        if algorithm != self.algorithm:
            raise AESPasswordDecodeError
        data = data.encode('utf-8')

        return DecodeAES(self.cipher, data)
