
import base58
import bech32

from blockchain.hash_methods import sha256


def is_pk(data: bytes) -> bool:
    return (data[0:1] == b"\x04") and len(data) == 65


def is_pkh(data: bytes) -> bool:
    return len(data) == 20


def is_compressed_pk(data) -> bool:
    return (data[0:1] == b"\x02" or data[0:1] == b"\x03") and len(data) == 33


def is_sh(data: bytes) -> bool:
    return len(data) == 20


def is_wsh(data: bytes) -> bool:
    return len(data) == 32


def is_trsh(data: bytes) -> bool:
    return len(data) == 32


def to_b58(data: bytes):
    return base58.b58encode(data)


def to_address(data: bytes, tp: int):

    if tp == 0 or tp == 1:
        data = bytearray(b"\x00") + bytearray(data)
        checksum = sha256(sha256(data))
        checksum = bytearray(checksum)[:4]
        data += checksum
        data = bytes(data)
        return to_b58(data=data).decode("utf-8")

    elif tp == 2 or tp == 6:
        data = bytearray(b"\x05") + bytearray(data)
        checksum = sha256(sha256(data))
        checksum = bytearray(checksum)[:4]
        data += checksum
        data = bytes(data)
        return to_b58(data=data).decode("utf-8")

    elif tp == 3 or tp == 4:
        data = bytearray(data)
        data = [int(e) for e in data]
        return bech32.encode(hrp="bc", witver=0, witprog=data)

    else:
        raise ValueError
