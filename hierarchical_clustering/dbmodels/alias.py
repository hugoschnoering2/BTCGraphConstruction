
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class Alias(Row):

    node_id: int
    alias: int

    @classmethod
    def table_name(cls, start: int, end: int):
        return f"alias_{start}_{end}"

    @classmethod
    def create_index_node_id(cls, start, end):
        return (f"CREATE INDEX IF NOT EXISTS {cls.table_name(start, end)}_node_id "
                f"ON {cls.table_name(start, end)} (node_id)")

    @classmethod
    def drop_index_node_id(cls, start, end):
        return f"DROP INDEX IF EXISTS {cls.table_name(start, end)}_node_id"

    @classmethod
    def create_index_alias(cls, start, end):
        return (f"CREATE INDEX IF NOT EXISTS {cls.table_name(start, end)}_alias "
                f"ON {cls.table_name(start, end)} (alias)")

    @classmethod
    def drop_index_alias(cls, start, end):
        return f"DROP INDEX IF EXISTS {cls.table_name(start, end)}_alias"

    @classmethod
    def create_table(cls, start, end):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name(start, end)} "
                f"(node_id INTEGER NOT NULL UNIQUE, alias INTEGER NOT NULL)")

    @classmethod
    def drop_table(cls, start, end):
        return f"DROP TABLE IF EXISTS {cls.table_name(start, end)}"

