
from dataclasses import dataclass

from database.dbmodels.row import Row


@dataclass
class TransactionEdge(Row):

    a: int  # origin
    b: int  # destination
    reveal: int  # reveal block
    last_seen: int  # last seen block
    total: int  # total number of transaction
    min_sent: int  # minimum sent
    max_sent: int  # maximum sent
    total_sent: int  # total sent

    @classmethod
    def table_name(cls):
        return "transaction_edges"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE {cls.table_name()} (a INTEGER NOT NULL, b INTEGER NOT NULL, "
                f"reveal INTEGER NOT NULL, last_seen INTEGER NOT NULL, total INTEGER NOT NULL, "
                f"min_sent BIGINT NOT NULL, max_sent BIGINT NOT NULL, total_sent BIGINT NOT NULL)")

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(reveal) FROM {cls.table_name()}"

    @classmethod
    def create_index_reveal(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_reveal ON {cls.table_name()} (reveal)"

    @classmethod
    def drop_index_reveal(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_reveal"

    @classmethod
    def create_index_a(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_a ON {cls.table_name()} (a)"

    @classmethod
    def drop_index_a(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_a"

    @classmethod
    def create_index_b(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_b ON {cls.table_name()} (b)"

    @classmethod
    def drop_index_b(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_b"

    @classmethod
    def create_constraint_a_b(cls):
        return f"ALTER TABLE {cls.table_name()} ADD CONSTRAINT {cls.table_name()}_uniqueness_a_b UNIQUE (a,b)"

    @classmethod
    def drop_constraint_a_b(cls):
        return f"ALTER TABLE {cls.table_name()} DROP CONSTRAINT IF EXISTS {cls.table_name()}_uniqueness_a_b"


@dataclass
class UndirectedTransactionEdge(Row):

    a: int  # origin
    b: int  # destination
    reveal: int  # reveal block

    @classmethod
    def table_name(cls):
        return "undirected_transaction_edges"

    @classmethod
    def get_max_block(cls):
        return f"SELECT max(reveal) FROM {cls.table_name()}"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name()} (a INTEGER NOT NULL, b INTEGER NOT NULL, "
                f"reveal INTEGER NOT NULL)")

    @classmethod
    def create_index_reveal(cls):
        return f"CREATE INDEX IF NOT EXISTS {cls.table_name()}_reveal ON {cls.table_name()} (reveal)"

    @classmethod
    def drop_index_reveal(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_reveal"

    @classmethod
    def create_constraint_a_b(cls):
        return f"ALTER TABLE {cls.table_name()} ADD CONSTRAINT {cls.table_name()}_uniqueness_a_b UNIQUE (a,b)"

    @classmethod
    def drop_constraint_a_b(cls):
        return f"ALTER TABLE {cls.table_name()} DROP CONSTRAINT IF EXISTS {cls.table_name()}_uniqueness_a_b"


@dataclass
class ClusterTransactionEdge(Row):

    a: int  # origin node id
    b: int  # destination node id
    alias: int  # cluster alias

    @classmethod
    def table_name(cls):
        return "cluster_transaction_edges"

    @classmethod
    def drop_index_a(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_a"

    @classmethod
    def drop_index_alias(cls):
        return f"DROP INDEX IF EXISTS {cls.table_name()}_alias"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name()} (a INTEGER NOT NULL, b INTEGER NOT NULL, "
                f"alias INTEGER NOT NULL, PRIMARY KEY (a,b)) ")

    @classmethod
    def drop_table(cls):
        return f"DROP TABLE IF EXISTS {cls.table_name()}"
