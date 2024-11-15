
from tqdm import tqdm
from typing import List
from copy import deepcopy
from joblib import Parallel, delayed

from database.dataService import DataService, Condition
from blockchain.read_binary_files import process_file

from database.utils import prepare_table

from database.dbmodels.block import Block
from database.dbmodels.colored_coin import ColoredCoin

from blockchain.special_transactions.colored_coin import (is_epobc_protocol, is_open_asset_protocol,
                                                          is_omnilayer_class_c, is_omnilayer_class_a_b)


def extract_from_file(file: str, folder: str, hash2num: dict[bytes, int]) -> List[ColoredCoin]:
    colored_coin_transactions: List[ColoredCoin] = []
    processed_file = process_file(file=file, folder=folder, drop_zero=False)
    for raw_block in processed_file:
        try:
            block = hash2num[raw_block.hash]
        except:
            continue
        for position_transaction, raw_transaction in enumerate(raw_block.transactions):
            if position_transaction == 0:
                continue
            is_oap = is_open_asset_protocol(transaction=raw_transaction)
            is_epobc = is_epobc_protocol(transaction=raw_transaction)
            is_ol_a_b = is_omnilayer_class_a_b(transaction=raw_transaction)
            is_ol_c = is_omnilayer_class_c(transaction=raw_transaction)
            if is_oap or is_epobc or is_ol_a_b or is_ol_c:
                colored_coin_transactions.append(
                    ColoredCoin(block=block, position=position_transaction, oa=int(is_oap),
                                epobc=int(is_epobc), ol_ab=int(is_ol_a_b), ol_c=int(is_ol_c)))
    return colored_coin_transactions


def populate_colored_coins(db: dict, start: int, end: int, folder: str, do: bool):

    if not do:
        return None

    rows = DataService(**db).fetch(table=Block.table_name(),
                                   conditions=[Condition("num", "<=", end),
                                               Condition("num", ">=", start)])

    files = sorted(list(set([str(row["num_file"]) for row in rows])))
    files = ["blk" + "0" * (5 - len(file)) + file + ".dat" for file in files]
    hash2num = {bytes(row["hash"]): int(row["num"]) for row in rows}

    transactions = Parallel(n_jobs=10)(delayed(extract_from_file)(file, folder, deepcopy(hash2num))
                                       for file in tqdm(files))

    transactions = [transaction for transactions_ in transactions for transaction in transactions_]
    transactions = [transaction for transaction in transactions if start <= transaction.block <= end]

    print(f"Populating the table {ColoredCoin.table_name()}, target block: {end}")
    ds = DataService(**db)
    ds.execute_query(query=ColoredCoin.drop_table())
    prepare_table(ds=ds, cls=ColoredCoin)

    print(f"Inserting {len(transactions)} transactions")
    DataService(**db).insert(table=ColoredCoin.table_name(), objs=transactions)
