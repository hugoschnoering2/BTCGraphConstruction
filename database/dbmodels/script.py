
from dataclasses import dataclass
from psycopg2 import Binary

from database.dbmodels.row import Row


@dataclass
class Script(Row):

    reveal: int
    hash160: Binary
    hash256: Binary
    script: Binary

    @classmethod
    def table_name(cls):
        return "scripts"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (reveal INTEGER NOT NULL, hash160 BYTEA PRIMARY KEY, "
                f"hash256 BYTEA, script BYTEA NOT NULL);")

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(reveal) AS max FROM {cls.table_name()}"
