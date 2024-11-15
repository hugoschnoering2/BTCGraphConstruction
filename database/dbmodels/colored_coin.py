
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class ColoredCoin(Row):

    block: int  # block
    position: int  # position in the block
    oa: int  # is open asset
    epobc: int  # is epobc
    ol_ab: int  # is omnilayer a or b
    ol_c: int  # is omnilayer c

    @classmethod
    def table_name(cls):
        return "colored_coin"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (block INTEGER, position INTEGER, oa INTEGER, epobc INTEGER, "
                f"ol_ab INTEGER, ol_c INTEGER, PRIMARY KEY (block, position))")

    @classmethod
    def create_index_block(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_block ON {cls.table_name()} (block)"

    @classmethod
    def drop_index_block(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_block"

    @classmethod
    def drop_table(cls):
        return f"DROP TABLE IF EXISTS {cls.table_name()}"
