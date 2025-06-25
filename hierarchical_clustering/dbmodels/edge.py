
from dataclasses import dataclass
from database.dbmodels.edge import TransactionEdge


@dataclass
class UpTransactionEdge(TransactionEdge):

    @classmethod
    def table_name(cls, start, end):
        return f"up_transaction_edges_{start}_{end}"

    @classmethod
    def drop_table(cls, start, end):
        return f"DROP TABLE IF EXISTS {cls.table_name(start, end)}"

    @classmethod
    def create_table(cls, start, end):
        return (f"CREATE TABLE {cls.table_name(start, end)} (a INTEGER NOT NULL, b INTEGER NOT NULL, "
                f"reveal INTEGER NOT NULL, last_seen INTEGER NOT NULL, total INTEGER NOT NULL, "
                f"min_sent BIGINT NOT NULL, max_sent BIGINT NOT NULL, total_sent BIGINT NOT NULL)")

    @classmethod
    def create_constraint_a_b(cls, start, end):
        return (f"ALTER TABLE {cls.table_name(start, end)} "
                f"ADD CONSTRAINT {cls.table_name(start, end)}_uniqueness_a_b UNIQUE (a,b)")

    @classmethod
    def drop_constraint_a_b(cls, start, end):
        return (f"ALTER TABLE {cls.table_name(start, end)} "
                f"DROP CONSTRAINT IF EXISTS {cls.table_name(start, end)}_uniqueness_a_b")

    @classmethod
    def create_index_reveal(cls, start, end):
        return (f"CREATE INDEX IF NOT EXISTS {cls.table_name(start, end)}_reveal "
                f"ON {cls.table_name(start, end)} (reveal)")

    @classmethod
    def drop_table(cls, start, end):
        return f"DROP TABLE IF EXISTS {cls.table_name(start, end)}"


@dataclass
class DownTransactionEdge(UpTransactionEdge):

    @classmethod
    def table_name(cls, start, end):
        return f"down_transaction_edges_{start}_{end}"
