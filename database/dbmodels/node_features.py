
from dataclasses import dataclass, field

from database.dbmodels.row import Row


@dataclass
class NodeFeatures(Row):

    alias: int

    degree: int = field(default=None)  # populated with degree.py

    degree_out: int = field(default=None)  # populated with edge_out.py
    total_transactions_out: int = field(default=None)
    first_transaction_out: int = field(default=None)
    last_transaction_out: int = field(default=None)
    min_sent: int = field(default=None)
    max_sent: int = field(default=None)
    total_sent: int = field(default=None)

    degree_in: int = field(default=None)  # populated with edge_in.py
    total_transactions_in: int = field(default=None)
    first_transaction_in: int = field(default=None)
    last_transaction_in: int = field(default=None)
    min_received: int = field(default=None)
    max_received: int = field(default=None)
    total_received: int = field(default=None)

    cluster_size: int = field(default=None)  # populated with cluster_size.py
    cluster_num_edges: int = field(default=None)  # populated with cluster_num_edges.py
    cluster_num_cc: int = field(default=None)  # populated with cluster_num_cc.py
    cluster_num_nodes_in_cc: int = field(default=None)   # populated with cluster_num_cc.py

    label: str = field(default=None)  # populated with category_label.py

    @classmethod
    def table_name(cls):
        return "node_features"

    @classmethod
    def create_table(cls):
        return (f"CREATE TABLE IF NOT EXISTS {cls.table_name()} "
                f"(alias INTEGER NOT NULL UNIQUE, "
                f"degree INTEGER, degree_in INTEGER, degree_out INTEGER, "
                f"total_transactions_in INTEGER, total_transactions_out INTEGER, "
                f"first_transaction_in INTEGER, last_transaction_in INTEGER, "
                f"first_transaction_out INTEGER, last_transaction_out INTEGER,"
                f"min_sent BIGINT, max_sent BIGINT, total_sent BIGINT, "
                f"min_received BIGINT, max_received BIGINT, total_received BIGINT, "
                f"cluster_size INTEGER, cluster_num_edges INTEGER, cluster_num_cc INTEGER, "
                f"cluster_num_nodes_in_cc INTEGER, "
                f"label VARCHAR(12), "
                f"PRIMARY KEY (alias))")
