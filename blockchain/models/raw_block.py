
from typing import List
from blockchain.models.raw_transaction import RawTransaction


class RawBlock(object):

    def __init__(self, hash: bytes, previous_hash: bytes,
                 byte_start: int, byte_end: int,
                 transactions: List[RawTransaction]):

        self.hash = hash
        self.previous_hash = previous_hash
        self.byte_start = byte_start
        self.byte_end = byte_end
        self.transactions = transactions  # transactions are ordered

        self.num_file = None
        self.block_num = None

    @property
    def num_transactions(self):
        return len(self.transactions)
