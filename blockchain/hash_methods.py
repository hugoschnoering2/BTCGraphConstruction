
import hashlib
from Crypto.Hash import RIPEMD160


def hash160(data: bytes) -> bytes:
    return RIPEMD160.RIPEMD160Hash(hashlib.sha256(data).digest()).digest()


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def hash256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()
