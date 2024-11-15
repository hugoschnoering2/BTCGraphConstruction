
from typing import List, Optional

from blockchain.script.match import match_owner_script, detect_script_in_unlocking, detect_script_in_witness


class RawInputTXO(object):

    def __init__(self, tx_hash: bytes, vout: int, script: bytes, witness: bytes, sequence: Optional[bytes] = None):

        self.tx_hash = tx_hash  # hash of the transaction in which the TXO was created
        self.vout = vout  # position in the output of the created transaction
        self.script = script  # unlocking script
        self.witness = witness
        self.sequence = sequence

    def update_witness(self, new_witness: bytes):
        self.witness = new_witness

    @property
    def txo_id(self):
        return self.tx_hash + self.vout.to_bytes(4, byteorder="big")

    @property
    def hidden_locking_script(self):
        locking_script = detect_script_in_unlocking(script=self.script)
        if locking_script is not None:  # script detected in the unlocking script
            return locking_script
        return detect_script_in_witness(self.witness)


class RawOutputTXO(object):

    def __init__(self, vout: int, value: bytes, script: bytes, tx_hash: bytes = None):

        self.tx_hash = tx_hash  # hash of the transaction in which the TXO was created
        self.vout = vout  # position in the output of the created transaction
        self.value = value
        self.script = script  # locking script

    def update_tx_hash(self, tx_hash: bytes):
        self.tx_hash = tx_hash

    @property
    def txo_id(self):
        return self.tx_hash + self.vout.to_bytes(4, byteorder="big")

    @property
    def owner(self):
        return match_owner_script(script=self.script)


class RawTransaction(object):

    def __init__(self, tx_hash: bytes, txos_in: List[RawInputTXO], txos_out: List[RawOutputTXO]):

        self.tx_hash = tx_hash
        self.txos_in = txos_in
        self.txos_out = txos_out

        for txo in self.txos_out:
            txo.update_tx_hash(tx_hash=self.tx_hash)
