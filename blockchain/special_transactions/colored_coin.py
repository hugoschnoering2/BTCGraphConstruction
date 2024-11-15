
from blockchain.models.raw_transaction import RawTransaction
from blockchain.script.decode import parse_script
from blockchain.script.match import match_owner_script
from blockchain.account import to_address
from blockchain.read_binary_files import internal_byte_order_to_hex


def is_open_asset_protocol(transaction: RawTransaction) -> bool:
    for tx_out in transaction.txos_out:
        script = tx_out.script
        try:
            tokens = parse_script(script=script)
        except:
            continue
        if len(tokens) == 0:
            continue
        if int(hex(tokens[0][1][0]), 16) == 0x6a:
            for tp, data in tokens[1:]:
                if tp == "data":
                    if data[:2] == b"OA":
                        return True
                    break
            return False
    return False


def is_epobc_protocol(transaction: RawTransaction) -> bool:
    """Example: 354324 855"""
    if len(transaction.txos_in) == 0:
        return False
    first_tx_in = transaction.txos_in[0]
    sequence_number = first_tx_in.sequence
    sequence_number = int(internal_byte_order_to_hex(sequence_number), 16)
    tag = sequence_number & 0x3f
    if tag == 0x25 or tag == 0x33:
        return True
    return False


def is_omnilayer_class_a_b(transaction: RawTransaction) -> bool:
    """1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4P"""
    for tx_out in transaction.txos_out:
        try:
            tp, owner = match_owner_script(script=tx_out.script)
            address = to_address(data=owner, tp=tp)
        except:
            continue
        if address == "1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4P":
            return True
    return False


def is_omnilayer_class_c(transaction: RawTransaction) -> bool:
    for tx_out in transaction.txos_out:
        script = tx_out.script
        try:
            tokens = parse_script(script=script)
        except:
            continue
        if len(tokens) < 2:
            continue
        if int(hex(tokens[0][1][0]), 16) == 0x6a and tokens[1][0] == "data" and tokens[1][1][:4] == b"omni":
            return True
    return False
