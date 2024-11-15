
from tqdm import tqdm

from mpire import WorkerPool

from database.utils import DataService
from database.dbmodels.node import Node
from database.dbmodels.alias import Alias
from database.dbmodels.node_features import NodeFeatures


def add_cluster_size(db: dict, start: int, end: int, block_step: int):

    num_steps = (end - start) // block_step + 1

    dict_cluster_sizes = dict()

    ds = DataService(**db)
    ds.execute_query(query=NodeFeatures.create_table())
    ds.execute_query(query=Node.create_index_reveal())
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Alias.create_index_node_id())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_aliases_block(worker_state, block_num) -> list:
        connector = worker_state["pool"].getconn()
        query = (f"SELECT n.node_id, a.alias "
                 f"FROM (SELECT * FROM {Node.table_name()} WHERE reveal = {block_num}) AS n "
                 f"LEFT JOIN {Alias.table_name()} AS a ON n.node_id = a.node_id")
        rows = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        worker_state["pool"].putconn(connector)
        aliases = [row["node_id"] if row["alias"] is None else row["alias"] for row in rows]
        return aliases

    for i in tqdm(range(num_steps)):
        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]
        if len(block_nums) == 0:
            continue
        with WorkerPool(n_jobs=8, start_method="threading", use_worker_state=True) as pool:
            block_aliases = pool.map(get_aliases_block, block_nums, progress_bar=False,
                                     worker_init=init_db_conn, worker_exit=close_db_conn)
        for aliases in block_aliases:
            for alias in aliases:
                dict_cluster_sizes[alias] = dict_cluster_sizes.get(alias, 0) + 1

    on_conflict_do = " ON CONFLICT (alias) DO UPDATE SET cluster_size = EXCLUDED.cluster_size"

    new_objs = []
    ds = DataService(**db)

    for ind, alias in enumerate(dict_cluster_sizes.keys()):
        new_objs.append(NodeFeatures(alias=alias, cluster_size=dict_cluster_sizes[alias]))
        if ind % 500000 == 0:
            ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
            new_objs = []

    ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
