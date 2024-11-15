
import os

from tqdm import tqdm

from psycopg2 import Binary

from blockchain.read_binary_files import process_file
from blockchain.hash_methods import hash160, sha256
from database.dataService import DataService, Condition

from database.dbmodels.block import Block
from database.dbmodels.txo import Spent_TXO, Created_TXO
from database.dbmodels.script import Script
from database.dbmodels.node import Node
from database.utils import prepare_table


def extract_from_file(file: str, folder: str, hash2num: dict, start: int, add_script: bool = False):

    blocks = process_file(file, folder, drop_zero=True)  # a list of raw blocks
    blocks = [block for block in blocks if block.hash in hash2num and hash2num[block.hash] >= start]
    for block in blocks:
        block.block_num = hash2num[block.hash]  # get the number of the block from its hash

    new_spent = list()
    new_created = list()
    new_nodes = dict()
    new_scripts = dict()

    for block in blocks:
        for position, transaction in enumerate(block.transactions):  # for each transaction found in the block
            if position > 0:
                for txo_in in transaction.txos_in:  # for each input TXO
                    new_spent.append(Spent_TXO(block_num=block.block_num, position=position,
                                               txo_id=Binary(txo_in.txo_id)))
                    if add_script:  # if we want to add scripts to the db
                        try:
                            locking_script = txo_in.hidden_locking_script
                        except Exception as e:
                            raise e
                        if locking_script is not None:  # script detected in the unlocking script / witness
                            new_scripts[locking_script] = min(block.block_num,
                                                              new_scripts.get(locking_script, 1000000000))
            for txo_out in transaction.txos_out:  # for each output TXO
                try:
                    tp, owner = txo_out.owner
                except Exception as e:
                    print(f"Impossible to decode the script of tx_out (block {block.block_num}, position {position}, "
                          f"v_out {txo_out.vout}): {txo_out.script}, error: {e}")
                    continue
                new_created.append({
                    "block_num": block.block_num,
                    "position": position,
                    "id": Binary(txo_out.txo_id),
                    "tp": tp,
                    "value": Binary(txo_out.value),
                    "owner": owner  # we set the address but we will change it later
                })
                if owner in new_nodes:
                    new_dict = {"reveal": min(new_nodes[owner]["reveal"], block.block_num)}
                    if new_nodes[owner]["reveal"] < block.block_num:
                        new_dict["reuse"] = min(block.block_num, new_nodes[owner]["reuse"])
                    else:
                        new_dict["reuse"] = new_nodes[owner]["reveal"]
                    new_nodes[owner] = new_dict
                else:
                    new_nodes[owner] = {"reveal": block.block_num,
                                        "reuse": 100000000}
                if (tp in [6]) and add_script:
                    new_scripts[txo_out.script] = min(block.block_num, new_scripts.get(txo_out.script, 1000000000))
    return {
        "spent": new_spent,
        "created": new_created,
        "scripts": [Script(reveal=num, hash160=Binary(hash160(script)), hash256=Binary(sha256(script)),
                           script=Binary(script)) for script, num in new_scripts.items()],
        "nodes": [Node(hash=Binary(owner), reveal=val["reveal"],
                       reuse=val["reuse"]) for owner, val in new_nodes.items()]
    }


def populate_txo(db: dict, start: int, end: int, folder: str, add_script: bool = True,
                 create_index: bool = False, do: bool = True, safe_mode: bool = True):

    if not do:
        return None

    print(f"\n {'-' * 30} \n Populating the tables {Created_TXO.table_name()}, {Spent_TXO.table_name()}, "
          f"{Script.table_name()}, {Node.table_name()}, target block: {end} \n")
    ds = DataService(**db)

    start = max(start, prepare_table(ds=ds, cls=Spent_TXO))
    prepare_table(ds=ds, cls=Created_TXO)
    prepare_table(ds=ds, cls=Node)
    if add_script:
        prepare_table(ds=ds, cls=Script)

    if start < end:

        if safe_mode:
            print("Press 'y' to confirm the drop of all indexes.")
            assert input() == "y"
        ds.execute_query(query=Spent_TXO.drop_index_block_num())
        ds.execute_query(query=Created_TXO.drop_index_block_num())
        ds.execute_query(query=Created_TXO.drop_index_id())
        ds.execute_query(query=Node.drop_index_node_id())
        ds.execute_query(query=Node.create_index_reveal())

        try:
            ds.execute_query(query=Node.create_constraint_hash())
        except Exception as e:
            print(f"Error {type(e)} - {e}")
            print("Impossible to create the constraint on node(hash), press 'y' to continue.")
            if safe_mode:
                assert input() == "y"

        ds = DataService(**db)
        conditions = [Condition("num", ">", start), Condition("num", "<=", end)]
        rows = ds.fetch(table=Block.table_name(), columns=["hash", "num", "num_file"], conditions=conditions)
        hash2num = {bytes(row["hash"]): row["num"] for row in rows}
        selected_files = set([int(row['num_file']) for row in rows])
        block_files = os.listdir(folder)
        files = [file for file in block_files if "blk" in file and
                 int(file.split(".")[0].replace("blk", "")) in selected_files]
        files = sorted(files, key=lambda name: int(name.replace("blk", "").replace(".dat", "")))

        ds = DataService(**db)
        connector_pool = DataService(**db).pool(min_connection=5, max_connection=20)

        try:

            for file in tqdm(files):  # do for each selected file (i.e. containing a block to be added)

                data = extract_from_file(file=file, folder=folder, hash2num=hash2num, start=start,
                                         add_script=add_script)

                # insert the spent TXOs
                connector = connector_pool.getconn()
                ds.insert(table=Spent_TXO.table_name(), objs=data["spent"], connector=connector)
                connector_pool.putconn(connector)

                # insert the nodes, in order to get the indexes for the insertion of the created TXOs
                on_conflict_do = " ON CONFLICT (hash) DO UPDATE SET reveal = LEAST(t.reveal,EXCLUDED.reveal)," + \
                                 " reuse = CASE" + \
                                 " WHEN t.reuse IS NULL THEN GREATEST(t.reveal,EXCLUDED.reveal)" + \
                                 " WHEN (t.reveal <= EXCLUDED.reveal) THEN LEAST(EXCLUDED.reveal,t.reuse)" + \
                                 " ELSE t.reveal END"
                returning = " RETURNING hash,node_id"
                # after inserting the nodes, we get back their ids
                connector = connector_pool.getconn()
                node_rows = ds.insert(table=Node.table_name(), objs=data["nodes"], on_conflict_do=on_conflict_do,
                                      returning=returning, connector=connector)
                connector_pool.putconn(connector)
                owner2node_id = {bytes(row["hash"]): row["node_id"] for row in node_rows}

                # insert the created TXOs (but before add the ids)
                created = [Created_TXO(block_num=new_created["block_num"], position=new_created["position"],
                                       txo_id=new_created["id"], tp=new_created["tp"], value=new_created["value"],
                                       node_id=owner2node_id[new_created["owner"]])
                           for new_created in data["created"]]
                connector = connector_pool.getconn()
                ds.insert(table=Created_TXO.table_name(), objs=created, connector=connector)
                connector_pool.putconn(connector)

                # insert the scripts
                if add_script:
                    on_conflict_do = " ON CONFLICT (hash160) DO UPDATE SET reveal = LEAST(t.reveal,EXCLUDED.reveal)"
                    connector = connector_pool.getconn()
                    ds.insert(table=Script.table_name(), objs=data["new_scripts"], on_conflict_do=on_conflict_do,
                              connector=connector)
                    connector_pool.putconn(connector)

        except Exception as e:
            print(f"Failure, we recommend to rollback to the previous state: {start}")
            raise e

    ds = DataService(**db)
    ds.execute_query(query=Node.drop_constraint_hash())
        
    if create_index:
        ds.execute_query(query=Spent_TXO.create_index_block_num())
        ds.execute_query(query=Created_TXO.create_index_block_num())
        ds.execute_query(query=Created_TXO.create_index_id())
        ds.execute_query(query=Node.create_index_node_id())
        ds.execute_query(query=Node.create_index_reveal())
