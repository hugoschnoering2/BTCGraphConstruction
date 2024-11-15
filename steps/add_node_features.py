

from features.cluster_size import add_cluster_size
from features.degree import add_degree_feature
from features.edge_in import add_in_transaction_features
from features.edge_out import add_out_transaction_features

from features.cluster_transaction_edges import add_cluster_transaction_edges
from features.cluster_num_edges import add_cluster_num_edges
from features.cluster_num_cc import add_cluster_connected_components_features


def add_node_features(db: dict, end: int, do: bool, do_cluster_features: bool, block_step: int):

    if not do:
        return

    add_cluster_size(db=db, start=0, end=end, block_step=block_step)
    add_degree_feature(db=db, start=0, end=end, block_step=block_step)
    add_in_transaction_features(db=db, start=0, end=end, block_step=block_step)
    add_out_transaction_features(db=db, start=0, end=end, block_step=block_step)

    if do_cluster_features:
        add_cluster_transaction_edges(db=db, start=0, end=end)
        add_cluster_num_edges(db=db)
        add_cluster_connected_components_features(db=db)
