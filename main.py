
import os
import yaml

from steps.populate_blocks import populate_blocks
from steps.populate_txo import populate_txo
from steps.populate_coinjoin import populate_coinjoins
from steps.populate_colored_coins import populate_colored_coins
from steps.populate_alias import populate_alias
from steps.populate_edges import populate_edges
from steps.populate_indirected_edges import populate_undirected_edges
from steps.add_node_features import add_node_features


if __name__ == "__main__":

    path_config = os.path.join(os.path.dirname(__file__), "conf.yaml")
    config = yaml.load(open(path_config, "r"), Loader=yaml.FullLoader)

    # first populate the blocks
    populate_blocks(db=config["db"], end=config["end"], folder=config["folder"], **config["blocks"])

    # then populate the TXOs, nodes, and scripts
    populate_txo(db=config["db"], start=config["start"], end=config["end"], folder=config["folder"], **config["txos"])

    # then populate the CoinJoin transactions
    populate_coinjoins(db=config["db"], start=config["start"], end=config["end"], **config["coinjoin"])

    # then populate the Colored Coin transactions
    populate_colored_coins(db=config["db"], start=config["start"], end=config["end"], folder=config["folder"],
                           **config["colored_coins"])

    # then populate the alias
    populate_alias(db=config["db"], start=config["start"],  end=config["end"], **config["alias"])

    # then populate the edges
    populate_edges(db=config["db"], start=config["start"], end=config["end"], **config["transaction_edges"])

    # then populate the undirected edges
    populate_undirected_edges(db=config["db"], start=config["start"], end=config["end"],
                              **config["undirected_transaction_edges"])

    # add the node features
    add_node_features(db=config["db"], end=config["end"], **config["features"])
