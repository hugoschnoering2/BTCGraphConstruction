
import os
import subprocess


def dump_table(hc_db: dict, start, end):

    output_file = f"dump_files/db_{start}_{end}.dump"

    command = [
        "pg_dump",
        "-U", hc_db["user"],
        "-h", hc_db["endpoint"],
        "-p", str(hc_db["port"]),
        "-d", hc_db["db"],
        "-F", "c",
        "-f", output_file
        ]

    tables = [
        f"alias_{start}_{end}",
        f"down_node_features_{start}_{end}",
        f"down_transaction_edges_{start}_{end}",
        f"up_node_features_{start}_{end}",
        f"up_transaction_edges_{start}_{end}",
    ]

    for table in tables:
        command.extend(["-t", table])

    env = os.environ.copy()
    env["PGPASSWORD"] = hc_db["password"]

    try:
        subprocess.run(command, check=True, env=env)
        print(f"Database dumped successfully to {output_file}")
    except subprocess.CalledProcessError as e:
        print("Error during pg_dump:", e)
