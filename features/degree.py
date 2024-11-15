
from tqdm import tqdm

from mpire import WorkerPool

from database.utils import DataService
from database.dbmodels.edge import UndirectedTransactionEdge
from database.dbmodels.node_features import NodeFeatures


def add_degree_feature(db: dict, start: int, end: int, block_step: int):

    num_steps = (end - start) // block_step + 1

    dict_degree = dict()

    ds = DataService(**db)
    ds.execute_query(query=NodeFeatures.create_table())
    ds.execute_query(query=UndirectedTransactionEdge.create_index_reveal())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num) -> list:
        connector = worker_state["pool"].getconn()
        query = f"SELECT * from {UndirectedTransactionEdge.table_name()} where reveal = {block_num}"
        edges = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        edges = [(row["a"], row["b"]) for row in edges]
        worker_state["pool"].putconn(connector)
        return edges

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        if len(block_nums) == 0:
            continue

        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_edges = pool.map(get_edges_block, block_nums, progress_bar=False,
                                   worker_init=init_db_conn, worker_exit=close_db_conn)

        for edges in block_edges:

            for sender, receiver in edges:

                dict_degree[sender] = dict_degree.get(sender, 0) + 1
                dict_degree[receiver] = dict_degree.get(receiver, 0) + 1

    on_conflict_do = " ON CONFLICT (alias) DO UPDATE SET degree = EXCLUDED.degree"

    new_objs = []
    ds = DataService(**db)

    for ind, alias in tqdm(enumerate(dict_degree.keys()), total=len(dict_degree)):
        new_objs.append(NodeFeatures(alias=alias, degree=dict_degree[alias]))
        if ind % 500000 == 0:
            ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
            new_objs = []

    ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
    ds.execute_query(query=UndirectedTransactionEdge.drop_index_reveal())
