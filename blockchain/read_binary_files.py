
import os
import binascii

from blockchain.hash_methods import hash256
from blockchain.models.raw_transaction import RawInputTXO, RawOutputTXO, RawTransaction
from blockchain.models.raw_block import RawBlock
from typing import List


def read_variable_length_integer(f):
    byte_ = f.read(1)
    hex_ = binascii.hexlify(byte_)
    int_ = int(hex_, 16)
    if int_ < 0xfd:
        return int_
    elif int_ == 0xfd:
        return int(binascii.hexlify(f.read(2)[::-1]), 16)
    elif int_ == 0xfe:
        return int(binascii.hexlify(f.read(4)[::-1]), 16)
    elif int_ == 0xff:
        return int(binascii.hexlify(f.read(8)[::-1]), 16)
    else:
        raise ValueError


def internal_byte_order_to_hex(s: bytes) -> hex:
    return binascii.hexlify(s[::-1]).decode("utf-8")


def process_transaction(f, coinbase: bool = False, drop_zero: bool = True) -> RawTransaction:
    byte_start = f.tell()  # easily retrieve the start of the transaction data
    txos_in, txos_out = [], []
    transaction_data = f.read(4)  # used to compute the hash of the transaction
    byte_start_hash = f.tell()
    num_txos_in = read_variable_length_integer(f)  # number of input UTXO
    witness_flag = num_txos_in == 0
    if witness_flag:
        f.read(1)
        byte_start_hash = f.tell()
        num_txos_in = read_variable_length_integer(f)
    if coinbase:  # COINBASE
        f.read(32)  # previous transaction is None
        f.read(4)  # previous vout is None
        size_script = read_variable_length_integer(f)
        script = f.read(size_script)  # unlocking script
        sequence = f.read(4)  # sequence
        txos_in.append(RawInputTXO(tx_hash=b"", vout=-1, script=script if size_script > 0 else b"", witness=b"",
                                   sequence=sequence))
    else:
        for position_in in range(num_txos_in):
            tx_hash = f.read(32)  # id of the transaction that has created the tx
            tx_vout = int(internal_byte_order_to_hex(f.read(4)), 16)  # position in the output list
            size_script = read_variable_length_integer(f)
            script = f.read(size_script)
            sequence = f.read(4)  # sequence
            txos_in.append(RawInputTXO(tx_hash=tx_hash, vout=tx_vout, script=script if size_script > 0 else b"",
                                       witness=b"", sequence=sequence))
    num_txos_out = read_variable_length_integer(f)  # number of output UTXO
    for position_out in range(num_txos_out):
        value = f.read(8)  # number of satoshis
        script_size = read_variable_length_integer(f)
        script = f.read(script_size)  # locking script
        if not drop_zero or int(internal_byte_order_to_hex(value), 16) > 0:
            txos_out.append(RawOutputTXO(vout=position_out, value=value, script=script))
    byte_end_hash = f.tell()
    f.seek(byte_start_hash)
    transaction_data += f.read(byte_end_hash - byte_start_hash)
    if witness_flag:
        for position_in in range(num_txos_in):
            byte_start_witness = f.tell()
            num_stack_items = read_variable_length_integer(f)
            for _ in range(num_stack_items):
                size = read_variable_length_integer(f)
                witness_data = f.read(size)
            byte_end_witness = f.tell()
            f.seek(byte_start_witness)
            witness_stack = f.read(byte_end_witness - byte_start_witness)
            txos_in[position_in].update_witness(new_witness=witness_stack)  # add all witness data to the input
    transaction_data += f.read(4)  # add the lock time to the data sequence to be hashed
    transaction_hash = hash256(transaction_data)  # compute the hash of the transaction
    return RawTransaction(tx_hash=transaction_hash, txos_in=txos_in, txos_out=txos_out)


def process_block(f, drop_zero: bool = True) -> RawBlock:
    byte_start = f.tell()
    f.read(4)  # magic bytes
    f.read(4)  # size
    header = f.read(80)  # get the header of the block
    previous_block_hash = header[4:36]
    block_hash = hash256(header)
    num_transactions = read_variable_length_integer(f)
    transactions = []
    for position in range(num_transactions):
        transaction = process_transaction(f, coinbase=position == 0, drop_zero=drop_zero)
        transactions.append(transaction)
    byte_end = f.tell()
    return RawBlock(hash=block_hash, previous_hash=previous_block_hash, byte_start=byte_start, byte_end=byte_end,
                    transactions=transactions)


def process_file(file: str, folder: str, max_block: int = None, drop_zero: bool = True) -> List[RawBlock]:
    blocks = []
    with open(os.path.join(folder, file), "rb") as f:
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(0, 0)
        while file_size - f.tell() > 0:
            block = process_block(f, drop_zero=drop_zero)
            blocks.append(block)
            if max_block is not None and len(blocks) >= max_block:
                break
    return blocks
