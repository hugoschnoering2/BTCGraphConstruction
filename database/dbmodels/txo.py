

from dataclasses import dataclass
from psycopg2 import Binary

from database.dbmodels.row import Row


@dataclass
class Spent_TXO(Row):

    block_num: int
    position: int
    txo_id: Binary  # tx consumed id = hash + vout

    @classmethod
    def table_name(cls):
        return "spent_txos"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (block_num INTEGER NOT NULL, "
                f"position SMALLINT NOT NULL, txo_id BYTEA NOT NULL)")

    @classmethod
    def create_index_block_num(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_block_num ON {cls.table_name()} (block_num)"

    @classmethod
    def drop_index_block_num(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_block_num"

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(block_num) AS max FROM {cls.table_name()}"


@dataclass
class Created_TXO(Row):

    block_num: int
    position: int
    txo_id: Binary  # tx created id = hash + vout
    tp: int
    value: Binary
    node_id: int

    @classmethod
    def table_name(cls):
        return "created_txos"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (block_num INTEGER NOT NULL, position SMALLINT NOT NULL, "
                f"tp SMALLINT NOT NULL, value BYTEA NOT NULL, txo_id BYTEA NOT NULL, node_id INTEGER NOT NULL)")

    @classmethod
    def create_index_block_num(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_block_num ON created_txos (block_num)"

    @classmethod
    def drop_index_block_num(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_block_num"

    @classmethod
    def create_index_id(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_txo_id ON created_txos (txo_id)"

    @classmethod
    def drop_index_id(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_txo_id"

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(block_num) AS max FROM {cls.table_name()}"
