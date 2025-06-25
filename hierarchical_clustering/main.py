
import os
import yaml

from hierarchical_clustering.steps.sample_transactions import sample_transactions
from hierarchical_clustering.steps.populate_alias import populate_alias
from hierarchical_clustering.steps.populate_edges import populate_edges
from hierarchical_clustering.steps.populate_node_features import populate_node_features
from hierarchical_clustering.steps.dump_table import dump_table

path_hc_config = "hc_config.yaml"
hc_config = yaml.load(open(path_hc_config, "r"), Loader=yaml.FullLoader)


if __name__ == "__main__":

    for span_dict in hc_config["spans"]:

        dump_file = f"dump_files/db_{span_dict['start']}_{span_dict['end']}.dump"
        if os.path.exists(dump_file):  # span already processed
            continue

        # sample transactions and store them in the db
        sample_transactions(db=hc_config["db"], hc_db=hc_config["hc_db"], start=span_dict["start"],
                            end=span_dict["end"], **hc_config["sampling"])

        # create the alias from these transactions
        populate_alias(db=hc_config["db"], hc_db=hc_config["hc_db"], start=span_dict["start"], end=span_dict["end"],
                       **hc_config["clustering"])

        # create tables for the up and down edges
        populate_edges(db=hc_config["db"], hc_db=hc_config["hc_db"],
                       start=span_dict["start"], end=span_dict["end"],
                       **hc_config["transaction_edges"])

        # create table for the up and down node features
        populate_node_features(hc_db=hc_config["hc_db"], start=span_dict["start"], end=span_dict["end"],
                               **hc_config["features"])

        # dump in a file
        dump_table(hc_db=hc_config["hc_db"], start=span_dict["start"], end=span_dict["end"])
