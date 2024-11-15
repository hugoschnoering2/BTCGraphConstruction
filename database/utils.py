
from typing import Optional

from database.dbmodels.txo import Created_TXO, Spent_TXO
from database.dbmodels.node import Node
from database.dbmodels.coinjoin import CoinJoin
from database.dbmodels.colored_coin import ColoredCoin

from database.dataService import DataService


def prepare_table(ds: DataService, cls):

    if not ds.execute_query(cls.exists(), fetch="one")["exists"]:
        print(f"Table {cls.table_name()} has not been found, press 'y' to confirm its creation")
        assert input() == "y", "Abort"
        ds.execute_query(cls.create_table())
        max_block = -1
    elif ds.execute_query(cls.get_len(), fetch="one")["len"] == 0:
        print(f"Table {cls.table_name()} has been found but is empty")
        max_block = -1
    else:
        max_block = ds.execute_query(cls.get_max_block(), fetch="one")["max"]
        print(f"Table {cls.table_name()} has been found and is populated up to the block {max_block}")

    return max_block


def query_input_txos(block_num: int, join_node: bool = False, join_alias: bool = False,
                     exclude_coinjoin: bool = False, exclude_colored_coin: bool = False,
                     only_one_per_position: bool = False, only_positions: Optional[list] = None):

    if only_one_per_position:
        query = (f"SELECT DISTINCT ON (position) position,txo_id "
                 f"FROM {Spent_TXO.table_name()} "
                 f"WHERE block_num = {block_num}")
    else:
        query = f"SELECT position,txo_id FROM {Spent_TXO.table_name()} WHERE block_num = {block_num}"

    if exclude_coinjoin:
        cj_forbidden_positions = f"SELECT position FROM {CoinJoin.table_name()} WHERE block = {block_num}"
        query += f" AND position NOT IN ({cj_forbidden_positions})"

    if exclude_colored_coin:
        cc_forbidden_positions = f"SELECT position FROM {ColoredCoin.table_name()} WHERE block = {block_num}"
        query += f" AND position NOT IN ({cc_forbidden_positions})"

    if only_positions is not None:
        assert not exclude_coinjoin and not exclude_colored_coin
        query += f" AND position IN ({','.join([str(position) for position in only_positions])})"

    query = (f"SELECT spent.*,created.node_id,created.value,created.block_num,created.tp FROM ({query}) AS spent "
             f"INNER JOIN {Created_TXO.table_name()} AS created ON spent.txo_id = created.txo_id")
    query = f"SELECT * FROM ({query}) AS spent WHERE spent.block_num <= {block_num}"
    query = (f"SELECT spent.*, dense_rank() OVER (PARTITION BY spent.txo_id  ORDER BY spent.block_num DESC) "
             f"AS rank FROM ({query}) AS spent")
    query = (f"SELECT DISTINCT ON (spent.position,spent.txo_id) "
             f"spent.position,spent.txo_id,spent.node_id,spent.tp,spent.value FROM ({query}) "
             f"AS spent WHERE spent.rank = 1")

    if join_node:
        query = (f"SELECT spent.*,nodes.hash AS node_hash,nodes.reveal,nodes.reuse FROM ({query}) AS spent "
                 f"INNER JOIN {Node.table_name()} AS nodes ON spent.node_id = nodes.node_id")

    if join_alias:
        query = (f"SELECT spent.*,CASE WHEN alias.alias IS NULL THEN spent.node_id ELSE alias.alias END AS alias "
                 f"FROM ({query}) AS spent LEFT JOIN alias AS alias on spent.node_id = alias.node_id")

    return query


def query_output_txos(block_num: int, join_node: bool = False, join_alias: bool = False,
                      exclude_coinjoin: bool = False, exclude_colored_coin: bool = False,
                      only_positions: Optional[list] = None):

    query = (f"SELECT DISTINCT ON (position,txo_id) position,txo_id,node_id,value,tp "
             f"FROM {Created_TXO.table_name()} WHERE block_num = {block_num}")

    if exclude_coinjoin:
        cj_forbidden_positions = f"SELECT position FROM {CoinJoin.table_name()} WHERE block = {block_num}"
        query += f" AND position NOT IN ({cj_forbidden_positions})"

    if exclude_colored_coin:
        cc_forbidden_positions = f"SELECT position FROM {ColoredCoin.table_name()} WHERE block = {block_num}"
        query += f" AND position NOT IN ({cc_forbidden_positions})"

    if only_positions is not None:
        assert not exclude_coinjoin and not exclude_colored_coin
        query += f" AND position IN ({','.join([str(position) for position in only_positions])})"

    if join_node:
        query = (f"SELECT created.*,nodes.hash AS node_hash,nodes.reveal,nodes.reuse FROM ({query}) AS created "
                 f"INNER JOIN {Node.table_name()} AS nodes ON created.node_id = nodes.node_id")

    if join_alias:
        query = (f"SELECT created.*, CASE WHEN alias.alias IS NULL THEN created.node_id ELSE alias.alias END AS alias "
                 f"FROM ({query}) AS created LEFT JOIN alias AS alias on created.node_id = alias.node_id")

    return query
