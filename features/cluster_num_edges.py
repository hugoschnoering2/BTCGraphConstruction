
from tqdm import tqdm
from database.dataService import DataService
from database.dbmodels.node_features import NodeFeatures
from database.dbmodels.edge import ClusterTransactionEdge


def add_cluster_num_edges(db: dict):
    ds = DataService(**db)
    query = f"SELECT alias, COUNT(*) FROM {ClusterTransactionEdge.table_name()} GROUP BY alias"
    rows = ds.execute_query(query=query, fetch="all")
    on_conflict_do = " ON CONFLICT (alias) DO UPDATE SET cluster_num_edges = EXCLUDED.cluster_num_edges"
    node_features = []
    for i, row in tqdm(enumerate(rows), total=len(rows)):
        node_features.append(NodeFeatures(alias=row["alias"], cluster_num_edges=row["count"]))
        if (i + 1) % 1000000 == 0:
            ds.insert(table=NodeFeatures.table_name(), objs=node_features, on_conflict_do=on_conflict_do)
            node_features = []
    ds.insert(table=NodeFeatures.table_name(), objs=node_features, on_conflict_do=on_conflict_do)
