# process.py
from typing import List, Dict
import time
import sys
import os.path
import json
import sqlite3
import yaml


# import csv
# import re


class Database:
    def __init__(self, path="./db/manifold.db"):
        self.path = path
        try:
            self.conn = sqlite3.connect(path)
        except sqlite3.Error as e:
            print(f"SQLite error: {' '.join(e.args)}")

    def execute(self, sql, rows=None, fetch=False, all_results=False):
        cur = self.conn.cursor()
        try:
            if rows:
                cur.executemany(sql, rows)
            else:
                cur.execute(sql)
            self.conn.commit()

            if fetch:
                if all_results:
                    return cur.fetchall()
                return cur.fetchone()

            print(
                f"Successfully executed!\n"
                f"current row count {cur.rowcount}\n"
                f"last row id {cur.lastrowid}"
            )
        except sqlite3.Error as e:
            print(f"SQLite error: {' '.join(e.args)}")
            self.conn.rollback()
            self.conn.close()

    def purge_tables(self):
        tables = self.execute(
            sql="SELECT name FROM sqlite_master WHERE type='table';",
            fetch=True,
            all_results=True,
        )

        for t in tables:
            t = t[0]
            print(f"purging table: {t}")
            self.execute(sql=f"DELETE FROM {t};")

    def insert_into_users(self, rows):
        print("inserting into table users")
        self.execute(sql="INSERT INTO users VALUES (?, ?, ?, ?)", rows=rows)

    def insert_into_process(self, rows):
        print("inserting into table process")
        self.execute(sql="INSERT INTO process VALUES (?, ?, ?, ?, ?, ?)", rows=rows)

    def check_sum(self):
        processed = self.execute(sql="SELECT sum(processed) from process;", fetch=True)[
            0
        ]
        rows = self.execute(sql="SELECT count(*) from users;", fetch=True)[0]
        print(f"Rows processed: {processed}, Rows in users: {rows}")

    def get_stats(self):
        top_10_skipped = self.execute(
            sql="SELECT file_name FROM process ORDER BY skipped DESC limit 10;",
            fetch=True,
            all_results=True,
        )

        top_10_processed = self.execute(
            sql="SELECT file_name FROM process ORDER BY processed DESC limit 10;",
            fetch=True,
            all_results=True,
        )

        top_10_zip_codes = self.execute(
            sql="""
                /* zip_code and their counts where */
                SELECT d2.zip_code, count(*)
                FROM (
                    /* zip_code, last_name, and last_name counts */
                    SELECT zip_code, d.* FROM users 
                    JOIN (
                        /* last names and their counts */
                        SELECT last_name, count(*) AS freq
                        FROM users 
                        WHERE last_name != ''
                        GROUP BY last_name
                    ) d 
                    ON users.last_name = d.last_name
                    WHERE zip_code != ''
                ) d2 
                WHERE d2.freq = 1
                GROUP BY 1
                ORDER BY 2 ASC
                limit 10;
            """,
            fetch=True,
            all_results=True,
        )

        print(
            f"top 10 skipped files: {top_10_skipped}\n"
            f"top 10 processed files: {top_10_processed}\n"
            f"top 10 zip codes with the most unique last names: {top_10_zip_codes}"
        )

    def close(self):
        print("closing connection to db")
        self.conn.close()


def parser(data: Dict, keys: List[str], vals: Dict, count: int = 0):
    # return the keys and values
    # return body should be
    # all the keys you found with values
    # if none found
    # return {}
    if isinstance(data, dict):
        for key, val in data.items():
            if key in keys:
                vals[key] = val
                count = count + 1
                # if all elements are found
                if count == len(keys):
                    return vals
            if isinstance(val, dict):
                return parser(val, keys, count=count, vals=vals)
        # print(vals)
        return vals


def convert_to_tuple(data: Dict):
    # header: [first_name, middle_name, last_name, zip_code]
    if data:
        row = [None] * 4
        for k, v in data.items():
            if k == "first_name":
                row[0] = v
            elif k == "middle_name":
                row[1] = v
            elif k == "last_name":
                row[2] = v
            else:
                row[3] = v
        return tuple(row)


# def concat_filename(source: str, file_path: str):
#     prefixes = re.split(r"/|\.", file_path)
#     return f"parsed/{source}/{'_'.join(prefixes[4:-1])}.csv"
#
#
# def create_directory(directory: str):
#     import os
#
#     exists = os.path.exists(directory)
#     if not exists:
#         os.makedirs(directory)
#         print(f"created folder {directory}")


# def write_to_csv(file_name: str, data: List[List], header: List = None):
#     with open(file_name, "w") as f:
#         # using default params
#         writer = csv.writer(f)
#         if header:
#             writer.writerow(header)
#         for d in data:
#             writer.writerow(d)


def parsing_a_file(source: str, path: str, batch: List, keys: List):
    # json loads into a variable
    # call parser
    # parsed into row
    # keep track of number of processed
    # keep track of number of skipped
    # return filename, number of processed, number of skipped
    records, processed, skipped = 0, 0, 0
    f_name = path.split("/")[-1]

    with open(path) as f:
        try:
            data = json.loads(json.load(f))
        except json.JSONDecodeError as e:
            print(e)
            sys.exit(1)

    for d in data:
        records = records + 1
        d = parser(d, keys=keys, vals={})
        if d:
            processed = processed + 1
            batch.append(convert_to_tuple(d))
        else:
            skipped = skipped + 1

    assert processed + skipped == records
    # f_name = concat_filename(source, path)
    # create_directory(f"./parsed/{source}")
    # write_to_csv(f_name, rows, keys)
    return batch, (source, path, f_name, processed, skipped, records)


def traverse_and_fetch_paths(source):
    base = f"./data/{source}"
    start = time.time()
    paths = []
    for path, subdir, files in os.walk(base):
        for name in files:
            if name.split(".")[-1] == "json":
                paths.append(os.path.join(path, name))
    print(
        f"traversed {base} in {time.time() - start}s\nand discovered {len(paths)} files"
    )
    return paths


def process():
    # this is where your code goes
    batch_size = 10000
    files_processed = []
    current_batch = []
    manifold = Database()

    with open("./process/process.yml") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        print(f"reading config...\n{config}")

    # read yaml
    # traverse folders
    start = time.time()
    for source in config["process"]:
        s_start = time.time()
        print(f"fetching file list for {source}...")
        paths = traverse_and_fetch_paths(source)

        for p in paths:
            p_start = time.time()
            current_batch, f_stats = parsing_a_file(
                source=source,
                path=p,
                batch=current_batch,
                keys=["first_name", "middle_name", "last_name", "zip_code"],
            )
            files_processed.append(f_stats)

            # batching
            if len(current_batch) > batch_size:
                print(f"batch size of {batch_size} reached, dumping...")
                manifold.insert_into_users(rows=current_batch)
                current_batch = []

            if len(files_processed) > batch_size:
                print(f"batch size of {batch_size} reached, dumping...")
                manifold.insert_into_process(rows=files_processed)
                files_processed = []

            print(f"{p} processed in {time.time() - p_start}s")
        print(f"source: {source} processed in {time.time() - s_start}s")

    manifold.insert_into_users(rows=current_batch)
    manifold.insert_into_process(rows=files_processed)
    print(f"data processed in {time.time() - start}s")

    manifold.check_sum()
    manifold.get_stats()
    manifold.close()


if __name__ == "__main__":
    process()
