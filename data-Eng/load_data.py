import sqlite3
import pandas as pd
import time

def load_csv_to_sqlite(csv_path, db_name, table_name):
    start = time.time()
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    end = time.time()
    print(f"Loaded {csv_path} into {db_name}.{table_name} in {end - start:.2f} sec")

# Small dataset (CSV in current folder)
load_csv_to_sqlite("people_small.csv", "people_small.db", "people")

# Large dataset
load_csv_to_sqlite("people_large.csv", "people_large.db", "people")

# Jobs table for JOIN
load_csv_to_sqlite("jobs_large.csv", "people_large.db", "jobs")
