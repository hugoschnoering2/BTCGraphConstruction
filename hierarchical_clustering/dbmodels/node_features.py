
from dataclasses import dataclass, field

from database.dbmodels.row import Row


@dataclass
class UpNodeFeatures(Row):

    node_id: int  # alias

    degree_out: int = field(default=None)
    total_transactions_out: int = field(default=None)
    first_transaction_out: int = field(default=None)
    last_transaction_out: int = field(default=None)
    min_sent: int = field(default=None)
    max_sent: int = field(default=None)
    total_sent: int = field(default=None)

    degree_in: int = field(default=None)
    total_transactions_in: int = field(default=None)
    first_transaction_in: int = field(default=None)
    last_transaction_in: int = field(default=None)
    min_received: int = field(default=None)
    max_received: int = field(default=None)
    total_received: int = field(default=None)

    @classmethod
    def table_name(cls, start, end):
        return f"up_node_features_{start}_{end}"

    @classmethod
    def create_table(cls, start, end):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name(start, end)} "
                f"(node_id INTEGER NOT NULL UNIQUE, "
                f"degree_in INTEGER, degree_out INTEGER, "
                f"total_transactions_in INTEGER, total_transactions_out INTEGER, "
                f"first_transaction_in INTEGER, last_transaction_in INTEGER, "
                f"first_transaction_out INTEGER, last_transaction_out INTEGER,"
                f"min_sent BIGINT, max_sent BIGINT, total_sent BIGINT, "
                f"min_received BIGINT, max_received BIGINT, total_received BIGINT, "
                f"PRIMARY KEY (node_id))")

    @classmethod
    def drop_table(cls, start, end):
        return f"DROP TABLE up_node_features_{start}_{end}"


@dataclass
class DownNodeFeatures(UpNodeFeatures):

    @classmethod
    def table_name(cls, start, end):
        return f"down_node_features_{start}_{end}"
