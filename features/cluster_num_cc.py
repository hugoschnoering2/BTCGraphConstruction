
# num cc = total num nodes - num nodes clusterisés + num clusters

import pandas as pd
from tqdm import tqdm

from database.dataService import DataService, Condition
from database.dbmodels.node_features import NodeFeatures
from database.dbmodels.edge import ClusterTransactionEdge

from clustering.unionFind import UnionFind


max_a = 1293517425


def add_cluster_connected_components_features(db: dict, max_num_edges: int = 2000000):

    ds = DataService(**db)

    # do for cluster with only one edge
    query = (f"UPDATE {NodeFeatures.table_name()} SET cluster_num_cc = 1, cluster_num_nodes_in_cc = 2 "
             f"WHERE cluster_num_edges = 1")
    ds.execute_query(query=query)

    # do for the other clusters
    query = f"SELECT alias, COUNT(*) FROM {ClusterTransactionEdge.table_name()} GROUP BY alias ORDER BY count"
    rows = ds.execute_query(query=query, fetch="all")
    rows = [{"alias": row["alias"], "count": row["count"]} for row in rows if row["count"] > 1]
    arr = pd.DataFrame(rows)[::-1]

    arr = arr.loc[arr["count"].isin([2, 3])]
    total_edges = int(arr["count"].sum())
    arr = arr.values

    loop_alias = []
    loop_total_edges = 0

    with tqdm(total=total_edges) as pbar:

        for alias, num_edges in arr:

            loop_alias.append(alias)
            loop_total_edges += num_edges

            if loop_total_edges > max_num_edges:

                uf = UnionFind()

                if loop_total_edges < 2 * max_num_edges:

                    condition = [Condition("alias", "IN", loop_alias)]
                    rows = ds.fetch(table=ClusterTransactionEdge.table_name(), conditions=condition,
                                    limit=loop_total_edges)
                    pbar.update(len(rows))

                    for row in rows:
                        alias_, a, b = row["alias"], row["a"], row["b"]
                        uf.add(new_ids=[(alias_, a), (alias_, b)])
                        uf.union((alias_, a), (alias_, b))

                    del rows

                else:

                    num_inner_loops = max_a // max_num_edges + 1

                    for i in range(num_inner_loops):

                        loop_min_a = int(max_a * i / num_inner_loops)
                        loop_max_a = int(max_a * (i + 1) / num_inner_loops)

                        conditions = [
                            Condition("alias", "IN", loop_alias),
                            Condition("a", ">=", loop_min_a),
                            Condition("a", "<=", loop_max_a)
                        ]

                        rows = ds.fetch(table=ClusterTransactionEdge.table_name(), conditions=conditions)
                        pbar.update(len(rows))

                        for row in rows:
                            alias_, a, b = row["alias"], row["a"], row["b"]
                            uf.add(new_ids=[(alias_, a), (alias_, b)])
                            uf.union((alias_, a), (alias_, b))

                        del rows

                num_nodes = dict()
                num_cc = dict()
                for alias_, _ in uf.cluster_root:
                    num_nodes[alias_] = num_nodes.get(alias_, 0) + 1
                for alias_, _ in uf.clusters_ids:
                    num_cc[alias_] = num_cc.get(alias_, 0) + 1

                new_features = []
                for alias_ in num_nodes:
                    new_features.append(NodeFeatures(alias=alias_, cluster_num_cc=num_cc[alias_],
                                                     cluster_num_nodes_in_cc=num_nodes[alias_]))
                on_conflict_do = (" ON CONFLICT (alias) DO UPDATE SET "
                                  "cluster_num_cc = EXCLUDED.cluster_num_cc, "
                                  "cluster_num_nodes_in_cc = EXCLUDED.cluster_num_nodes_in_cc")
                ds.insert(table=NodeFeatures.table_name(), objs=new_features, on_conflict_do=on_conflict_do)

                del uf
                del num_nodes
                del num_cc

                loop_alias = []
                loop_total_edges = 0

        if len(loop_alias) > 0:

            uf = UnionFind()

            if loop_total_edges < 2 * max_num_edges:

                condition = [Condition("alias", "IN", loop_alias)]
                rows = ds.fetch(table=ClusterTransactionEdge.table_name(), conditions=condition,
                                limit=loop_total_edges)
                pbar.update(len(rows))

                for row in rows:
                    alias_, a, b = row["alias"], row["a"], row["b"]
                    uf.add(new_ids=[(alias_, a), (alias_, b)])
                    uf.union((alias_, a), (alias_, b))

                del rows

            else:

                num_inner_loops = max_a // max_num_edges + 1

                for i in range(num_inner_loops):

                    loop_min_a = int(max_a * i / num_inner_loops)
                    loop_max_a = int(max_a * (i + 1) / num_inner_loops)

                    conditions = [
                        Condition("alias", "IN", loop_alias),
                        Condition("a", ">=", loop_min_a),
                        Condition("a", "<=", loop_max_a)
                    ]

                    rows = ds.fetch(table=ClusterTransactionEdge.table_name(), conditions=conditions)
                    pbar.update(len(rows))

                    for row in rows:
                        alias_, a, b = row["alias"], row["a"], row["b"]
                        uf.add(new_ids=[(alias_, a), (alias_, b)])
                        uf.union((alias_, a), (alias_, b))

                    del rows

            num_nodes = dict()
            num_cc = dict()
            for alias_, _ in uf.cluster_root:
                num_nodes[alias_] = num_nodes.get(alias_, 0) + 1
            for alias_, _ in uf.clusters_ids:
                num_cc[alias_] = num_cc.get(alias_, 0) + 1

            new_features = []
            for alias_ in num_nodes:
                new_features.append(NodeFeatures(alias=alias_, cluster_num_cc=num_cc[alias_],
                                                 cluster_num_nodes_in_cc=num_nodes[alias_]))
            on_conflict_do = (" ON CONFLICT (alias) DO UPDATE SET "
                              "cluster_num_cc = EXCLUDED.cluster_num_cc, "
                              "cluster_num_nodes_in_cc = EXCLUDED.cluster_num_nodes_in_cc")
            ds.insert(table=NodeFeatures.table_name(), objs=new_features, on_conflict_do=on_conflict_do)

            del uf
            del num_nodes
            del num_cc
