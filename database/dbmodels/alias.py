
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class Alias(Row):

    node_id: int
    alias: int

    @classmethod
    def table_name(cls):
        return "alias"

    @classmethod
    def create_index_node_id(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_node_id ON {cls.table_name()} (node_id)"

    @classmethod
    def drop_index_node_id(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_node_id"

    @classmethod
    def create_index_alias(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_alias ON {cls.table_name()} (alias)"

    @classmethod
    def drop_index_alias(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_alias"

    @classmethod
    def create_table(cls):
        return f"CREATE TABLE IF NOT EXISTS {cls.table_name()} (node_id INTEGER NOT NULL UNIQUE, alias INTEGER NOT NULL)"

    @classmethod
    def drop_table(cls):
        return f"DROP TABLE IF EXISTS {cls.table_name()}"

