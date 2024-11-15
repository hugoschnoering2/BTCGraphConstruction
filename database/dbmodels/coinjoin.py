
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class CoinJoin(Row):

    block: int  # block
    position: int  # position in the block
    joinmarket: int
    wasabi1: int
    wasabi11: int
    wasabi2: int
    whirlpool: int
    whirlpool_tx0: int

    @classmethod
    def table_name(cls):
        return "coinjoin"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (block INTEGER, position INTEGER, joinmarket INTEGER, wasabi1 INTEGER, "
                f"wasabi11 INTEGER, wasabi2 INTEGER, whirlpool INTEGER, whirlpool_tx0 INTEGER, "
                f"  PRIMARY KEY (block, position))")

    @classmethod
    def drop_table(cls):
        return f"DROP TABLE IF EXISTS {cls.table_name()}"

    @classmethod
    def create_index_block(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_block ON {cls.table_name()} (block)"

    @classmethod
    def drop_index_block(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_block"
