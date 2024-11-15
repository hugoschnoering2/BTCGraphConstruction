
from tqdm import tqdm
from mpire import WorkerPool


from database.dataService import DataService
from database.utils import prepare_table
from database.dbmodels.edge import UndirectedTransactionEdge, TransactionEdge


def populate_undirected_edges(db: dict, start: int,  end: int, do: bool, block_step: int):

    if not do:
        return None

    ds = DataService(**db)
    start = max(prepare_table(ds=ds, cls=UndirectedTransactionEdge), start)
    print(f"Populating the table {UndirectedTransactionEdge.table_name()}, target block: {end}, start: {start}")
    try:
        ds.execute_query(query=UndirectedTransactionEdge.create_constraint_a_b())
    except Exception as e:
        print(f"Error {type(e)} - {e}")
        print("Impossible to create the constraint on undirected_transaction_edges(a,b), press 'y' to continue.")
        assert input() == "y"

    ds.execute_query(query=TransactionEdge.create_index_reveal())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num):

        connector = worker_state["pool"].getconn()
        query = f"SELECT a, b FROM {TransactionEdge.table_name()} WHERE reveal = {block_num}"
        block_edges = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        worker_state["pool"].putconn(connector)

        new_edges = set()
        for row in block_edges:
            if row["a"] < row["b"]:
                new_edges.add((row["a"], row["b"]))
            elif row["b"] < row["a"]:
                new_edges.add((row["b"], row["a"]))

        return new_edges

    num_steps = (end - start) // block_step + 1

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        # we collect the block transactions from the selected blocks
        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_edges = pool.map(get_edges_block, block_nums, progress_bar=False,
                                   worker_init=init_db_conn, worker_exit=close_db_conn)

        new_edges = dict()

        for block_num, edges in zip(block_nums, block_edges):

            for (a, b) in edges:
                if b < a:
                    raise Exception
                elif (a, b) not in new_edges:
                    new_edges[(a, b)] = block_num

        new_edges = [UndirectedTransactionEdge(a=k[0], b=k[1], reveal=reveal) for k, reveal in new_edges.items()]

        DataService(**db).insert(table=UndirectedTransactionEdge.table_name(), objs=new_edges,
                                 on_conflict_do_nothing=True)

    DataService(**db).execute_query(query=UndirectedTransactionEdge.drop_constraint_a_b())
