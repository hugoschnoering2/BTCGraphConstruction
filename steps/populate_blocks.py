
import os

from tqdm import tqdm
from typing import List
from joblib import Parallel, delayed

from blockchain.models.raw_block import RawBlock
from blockchain.read_binary_files import process_block, internal_byte_order_to_hex

from database.utils import prepare_table
from database.dbmodels.block import Block
from database.dataService import DataService


def extract_from_file(file: str, folder: str) -> List[RawBlock]:
    num_file = int(file.split(".")[0].replace("blk", ""))
    blocks = []
    with open(os.path.join(folder, file), "rb") as f:
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(0, 0)
        while file_size - f.tell() > 0:
            block = process_block(f)
            block.num_file = num_file
            blocks.append(block)
    return blocks


def populate_blocks(db: dict, end: int, folder: str, do: bool = True):

    if not do:
        return None

    print(f"\n {'-' * 30} \n Populating the table {Block.table_name()}, target block: {end}")
    start = prepare_table(ds=DataService(**db), cls=Block)  # get the starting block

    if start < end:  # if there is at least one block to be processed

        if start < 0:  # the database is empty
            prev_hash = (b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                         b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
        else:  # else we get the last hash
            query = f"SELECT hash FROM {Block.table_name()} WHERE num = {start}"
            prev_hash = bytes(DataService(**db).execute_query(query=query, fetch="one")["hash"])

        print(f"Last block: {start}, previous hash: {internal_byte_order_to_hex(prev_hash)}")
        print("New blocks are about to be inserted, the index block(num) must be dropped, confirm with 'y'")
        assert input() == "y", "Abort"
        DataService(**db).execute_query(query=Block.drop_index_num())

        files = [file for file in os.listdir(folder) if "blk" in file]
        files = sorted(files, key=lambda x: int(x.split(".")[0].replace("blk", "")))

        # we process in parallel all the blocks
        blocks = Parallel(n_jobs=-1)(delayed(extract_from_file)(file, folder) for file in tqdm(files))
        blocks = [block for blocks_ in blocks for block in blocks_]
        blocks = {block.previous_hash: block for block in blocks}

        blocks_to_add = []
        # we need to compute the block number from the chain of hashes
        for i in range(len(blocks)):
            if start + 1 + i > end:
                break
            if prev_hash in blocks:
                next_block = blocks[prev_hash]
                next_block.block_num = start + 1 + i
                prev_hash = next_block.previous_hash
                blocks_to_add.append(next_block)
            else:
                break

        blocks_to_add = [Block.from_raw_block(block) for block in blocks_to_add if (block.block_num > start)
                         and (block.block_num <= end)]

        # finally we can insert all the new blocks
        print(f"Inserting {len(blocks_to_add)} new blocks")
        DataService(**db).insert(table=Block.table_name(), objs=blocks_to_add)

    DataService(**db).execute_query(Block.create_index_num())
