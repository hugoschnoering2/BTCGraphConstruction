
import re
import binascii

from blockchain.account import is_pk, is_pkh, is_compressed_pk, is_sh, is_wsh, is_trsh
from blockchain.script.decode import parse_script, decode_witness
from blockchain.hash_methods import hash160


def is_p2pk(tokens: list):
    if len(tokens) != 2:
        return False
    if (tokens[0][0] != "data") or (tokens[1] != ("op", b"\xac")):
        return False
    return is_pk(tokens[0][1]) or is_compressed_pk(tokens[0][1])


def is_p2pkh(tokens: list):
    if len(tokens) != 5:
        return False
    if ((tokens[0] != ("op", b"v")) or (tokens[1] != ("op", b"\xa9")) or (tokens[2][0] != "data")
            or (tokens[3] != ("op", b"\x88")) or (tokens[4] != ("op", b"\xac"))):
        return False
    return is_pkh(tokens[2][1])


def is_p2sh(tokens: list):
    if len(tokens) != 3:
        return False
    if (tokens[0] != ("op", b"\xa9")) or (tokens[1][0] != "data") or (tokens[2] != ("op", b"\x87")):
        return False
    return is_sh(tokens[1][1])


def is_p2ms(tokens: list):
    if len(tokens) <= 3:
        return False
    if (tokens[-1] != ("op", b"\xae")) or (tokens[0][0] != "op") or (tokens[-2][0] != "op"):
        return False
    for i in range(1, len(tokens)-2):
        if is_pk(tokens[i][1]) or is_compressed_pk(tokens[i][1]):
            return True
    return False


def is_p2wpkh(tokens: list):
    if len(tokens) != 2:
        return False
    if (tokens[0] != ("op", b"\x00")) and (tokens[1][0] != "data"):
        return False
    return is_pkh(tokens[1][1])


def is_p2wsh(tokens: list):
    if len(tokens) != 2:
        return False
    if (tokens[0] != ("op", b"\x00")) and (tokens[1][0] != "data"):
        return False
    return is_wsh(tokens[1][1])


def is_p2taproot(tokens: list):
    if len(tokens) != 2:
        return False
    if (tokens[0] != ("op", b"\x51")) and (tokens[1][0] != "data"):
        return False
    return is_trsh(tokens[1][1])


dersig = "30[0-9a-f]{2}[0-9a-f]+02[0-9a-f]+(01|02|03|81|82|83)"


def is_signature(data: bytes) -> bool:
    data_str = binascii.hexlify(data).decode("utf-8")
    return not(re.fullmatch(dersig, data_str) is None)


def match_script(script: bytes = None, tokens: list = None):
    tokens = parse_script(script) if tokens is None else tokens
    if is_p2pk(tokens):
        return "p2pk"
    elif is_p2pkh(tokens):
        return "p2pkh"
    elif is_p2sh(tokens):
        return "p2sh"
    elif is_p2ms(tokens):
        return "p2ms"
    elif is_p2wpkh(tokens):
        return "p2wpkh"
    elif is_p2wsh(tokens):
        return "p2wsh"
    elif is_p2taproot(tokens):
        return "p2taproot"
    else:
        return "unknown"


def match_owner_script(script: bytes = None, tokens: list = None):
    tokens = parse_script(script) if tokens is None else tokens
    if is_p2pk(tokens):
        pk = tokens[0][1]
        pkh = hash160(pk)
        return 0, pkh
    elif is_p2pkh(tokens):
        return 1, tokens[2][1]
    elif is_p2sh(tokens):
        return 2, tokens[1][1]
    elif is_p2wpkh(tokens):
        return 3, tokens[1][1]
    elif is_p2wsh(tokens):
        return 4, tokens[1][1]
    elif is_p2taproot(tokens):
        return 5, tokens[1][1]
    else:
        sh = hash160(script)
        return 6, sh


def detect_script_in_unlocking(script: bytes = None, tokens: list = None):
    tokens = parse_script(script) if tokens is None else tokens
    if (len(tokens) == 0) or (tokens[-1][0] != "data"):
        return None
    locking_script = tokens[-1][1]
    if is_pk(locking_script) or is_compressed_pk(locking_script):
        return None
    if is_signature(locking_script):
        return None
    try:
        parse_script(locking_script)
        return locking_script
    except:
        return None


def detect_script_in_witness(witness: bytes):
    witness_decoded = decode_witness(witness)
    if len(witness_decoded) == 0:
        return None
    locking_script = witness_decoded[-1]
    if is_pk(locking_script) or is_compressed_pk(locking_script):
        return None
    if is_signature(locking_script):
        return None
    try:
        parse_script(locking_script)
        return locking_script
    except:
        return None
