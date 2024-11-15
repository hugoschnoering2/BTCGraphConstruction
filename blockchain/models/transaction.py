
import binascii

import numpy as np
import pandas as pd

from typing import List, Dict, Optional
from tabulate import tabulate

from blockchain.read_binary_files import internal_byte_order_to_hex
from blockchain.account import to_address


class TXO(object):

    def __init__(self, txo_id: bytes = None, value: bytes = None, node_id: int = None, tp: int = None,
                 node_hash: bytes = None, reveal: int = None, reuse: int = None, alias: int = None):

        self.txo_id = bytes(txo_id)
        self.value = value
        self.node_id = node_id
        self.tp = tp

        self.node_hash = bytes(node_hash) if node_hash else None
        self.reveal = reveal
        self.reuse = reuse

        self.alias = alias

    @property
    def txo_position(self) -> int:
        # return the position of the txo in the creation transaction output
        return int(binascii.hexlify(self.txo_id[-4:]), 16)

    @property
    def tx_hash(self) -> bytes:
        return internal_byte_order_to_hex(self.txo_id[:-4])

    @property
    def value_int(self):
        return int(internal_byte_order_to_hex(bytes(self.value)), 16)

    @property
    def address(self) -> str:
        return to_address(data=self.node_hash, tp=self.tp) if self.node_hash else None


class Transaction(object):

    def __init__(self, block_num: int = None, position: int = None, input_txos: Optional[List[TXO]] = None,
                 output_txos: Optional[List[TXO]] = None):

        self.block_num = block_num  # block where the transaction is
        self.position = position  # position in the block
        self.input_txos = input_txos if input_txos is not None else []
        self.output_txos = output_txos if output_txos is not None else []

    @classmethod
    def from_rows(cls, block_num: int = None, position: int = None, input_txos: Optional[list] = None,
                  output_txos: Optional[list] = None):
        return cls(block_num=block_num,
                   position=position,
                   input_txos=[TXO(**row) for row in input_txos] if input_txos is not None else [],
                   output_txos=[TXO(**row) for row in output_txos] if output_txos is not None else [])

    def compute_features(self):
        return TransactionFeatures(transaction=self)

    def __repr__(self):
        if len(self.output_txos) > 0:
            tx_hash = self.output_txos[0].tx_hash
        else:
            tx_hash = None
        repr_block = f"Transaction --- block {self.block_num} --- position {self.position} -- hash {tx_hash}"
        repr_inputs = pd.DataFrame([{"tx_id": txo.tx_hash[:6] + "..." + txo.tx_hash[-6:],
                                     "vout": txo.txo_position,
                                     "value": txo.value_int / 10 ** 8,
                                     "node_id": txo.node_id,
                                     "alias": txo.alias,
                                     "address": txo.address,
                                     "reveal": txo.reveal,
                                     "reuse": txo.reuse if txo.reuse and txo.reuse < 1000000 else None}
                                    for txo in self.input_txos])
        repr_inputs = tabulate(repr_inputs, headers="keys", tablefmt="psql", showindex="never")
        repr_outputs = pd.DataFrame([{"vout": txo.txo_position,
                                      "value": txo.value_int / 10 ** 8,
                                      "node_id": txo.node_id,
                                      "alias": txo.alias,
                                      "address": txo.address,
                                      "reveal": txo.reveal,
                                      "reuse": txo.reuse if txo.reuse and txo.reuse < 1000000 else None}
                                     for txo in self.output_txos])
        repr_outputs = tabulate(repr_outputs, headers="keys", tablefmt="psql", showindex="never")
        return ("\n" + str(repr_block) + "\n\n Input TXOs: \n"
                + str(repr_inputs) + "\n Output TXOs: \n " + str(repr_outputs))


class BatchBlockTransactions(object):  # represents a list of transaction within the same block

    def __init__(self, block_num: int, transactions: Dict[int, Transaction]):

        self.block_num = block_num
        self.transactions = transactions

    @classmethod
    def from_rows(cls, block_num: int, input_txos: list, output_txos: list):

        transactions = dict()

        for txo in input_txos:
            if txo["position"] not in transactions:
                transactions[txo["position"]] = {"inputs": [], "outputs": []}
            transactions[txo["position"]]["inputs"].append({k: v for k, v in txo.items() if k != "position"})

        for txo in output_txos:
            if txo["position"] not in transactions:
                transactions[txo["position"]] = {"inputs": [], "outputs": []}
            transactions[txo["position"]]["outputs"].append({k: v for k, v in txo.items() if k != "position"})

        transactions = {position: Transaction.from_rows(block_num=block_num,
                                                        position=position,
                                                        input_txos=txos["inputs"],
                                                        output_txos=txos["outputs"])
                        for position, txos in transactions.items()}

        return cls(block_num=block_num, transactions=transactions)


class TransactionFeatures(object):

    def __init__(self, transaction: Transaction):

        self.transaction = transaction

        self.input_ids = {txo.node_id for txo in transaction.input_txos}
        self.output_ids = {txo.node_id for txo in transaction.output_txos}
        self.input_values = {txo.value_int for txo in transaction.input_txos}  # unique input values

        output_values, occ_output_values = np.unique([txo.value_int for txo in transaction.output_txos],
                                                     return_counts=True)
        desc_sorted_output_values = [(v, c) for v, c in zip(output_values, occ_output_values)]
        desc_sorted_output_values = sorted(desc_sorted_output_values, key=lambda x: x[1], reverse=True)

        self.output_values = [v for (v, c) in desc_sorted_output_values]
        self.occ_output_values = [c for (v, c) in desc_sorted_output_values]

    @property
    def num_inputs(self):
        return len(self.transaction.input_txos)

    @property
    def num_outputs(self):
        return len(self.transaction.output_txos)

    @property
    def num_input_ids(self):
        return len(self.input_ids)

    @property
    def num_output_ids(self):
        return len(self.output_ids)

    @property
    def num_input_values(self):
        return len(self.input_values)

    @property
    def num_output_values(self):
        return len(self.output_values)

    @property
    def most_represented_output_value(self):
        if len(self.output_values) > 0:
            return self.output_values[0]
        else:
            return None

    @property
    def occ_most_represented_output_value(self):
        if len(self.occ_output_values) > 0:
            return self.occ_output_values[0]
        else:
            return None

    @property
    def is_position_zero_in_output(self):
        return 0 in [txo.txo_position for txo in self.transaction.output_txos]
