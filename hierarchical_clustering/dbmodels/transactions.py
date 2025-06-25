
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class Transaction(Row):

    block_num: int  # block number
    position: int  # position

    @classmethod
    def table_name(cls, start: int, end: int):
        return f"transactions_{start}_{end}"

    @classmethod
    def create_table(cls, start: int, end: int):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name(start, end)} "
                f"(block_num INTEGER NOT NULL, position INTEGER NOT NULL)")

    @classmethod
    def empty_table(cls, start, end):
        return f"DELETE FROM {cls.table_name(start, end)}"
