
from dataclasses import dataclass
from psycopg2 import Binary

from database.dbmodels.row import Row
from blockchain.models.raw_block import RawBlock


@dataclass
class Block(Row):

    hash: Binary
    num_file: int
    byte_start: int
    num: int
    num_transactions: int

    @classmethod
    def from_raw_block(cls, raw_block: RawBlock):
        return cls(hash=Binary(raw_block.hash),
                   num_file=raw_block.num_file,
                   byte_start=raw_block.byte_start,
                   num=raw_block.block_num,
                   num_transactions=raw_block.num_transactions)

    @classmethod
    def table_name(cls):
        return "blocks"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (byte_start INTEGER NOT NULL, num INTEGER NOT NULL, "
                f"num_file SMALLINT NOT NULL, num_transactions SMALLINT NOT NULL, hash BYTEA NOT NULL)")

    @classmethod
    def create_index_num(cls):
        return f"CREATE INDEX IF NOT EXISTS blocks_num ON {cls.table_name()} (num)"

    @classmethod
    def drop_index_num(cls):
        return f"DROP INDEX IF EXISTS blocks_num"

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(num) AS max FROM {cls.table_name()}"
