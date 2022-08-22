# process.py
from typing import List, Dict, Tuple
import time
import os.path
import json
import sqlite3
import logging
from datetime import datetime

TODAY = datetime.today().strftime(format="%Y_%m_%dT%H%M%S%fZ")
logging.basicConfig(filename=f"./logs/{TODAY}_process.log", level=logging.INFO)
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


class Database:
    def __init__(self, path="./db/manifold.db"):
        self.path = path
        try:
            self.conn = sqlite3.connect(path)
        except sqlite3.Error as e:
            LOG.error(f"SQLite error: {' '.join(e.args)}")

    def execute(
        self,
        sql: str,
        rows: List[Tuple] = None,
        fetch: bool = False,
        all_results: bool = False,
    ):
        """
        Wrapper around sqlite3 execute
        :param sql: SQL command to run
        :param rows: List of tuples supplied for executemany
        :param fetch: Bool to return results from execution
        :param all_results: Default to False and only return one result,
        if set to true return all
        :return:
        """

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

            LOG.info(
                f"Successfully executed!\n"
                f"current row count {cur.rowcount}\n"
                f"last row id {cur.lastrowid}"
            )
        except sqlite3.Error as e:
            LOG.error(f"SQLite error: {' '.join(e.args)}")
            LOG.error(f"SQL: {sql}\n{rows}")
            self.conn.rollback()
            self.conn.close()

    def insert_into_users(self, rows: List[Tuple]):
        LOG.info("Inserting into table users")
        self.execute(sql="INSERT INTO users VALUES (?, ?, ?, ?)", rows=rows)

    def insert_into_process(self, rows: List[Tuple]):
        LOG.info("Inserting into table process")
        self.execute(sql="INSERT INTO process VALUES (?, ?, ?, ?, ?, ?)", rows=rows)

    def check_sum(self):
        """
        Run SQL to check sum of processed and number of existing rows
        :return: None
        """

        processed = self.execute(sql="SELECT sum(processed) from process;", fetch=True)[
            0
        ]
        rows = self.execute(sql="SELECT count(*) from users;", fetch=True)[0]
        LOG.info(f"Rows processed: {processed}, Rows in users: {rows}")

    def get_stats(self) -> Dict:
        """
        Run SQL to get list of stats
        :return: Dict
        """

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

        top_10_zip_codes_unique = self.execute(
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

        top_10_zip_codes = self.execute(
            sql="SELECT zip_code, count(*) FROM users WHERE zip_code != '' GROUP BY zip_code ORDER BY 2 DESC limit 10;",
            fetch=True,
            all_results=True,
        )

        return {
            "top_10_processed": [e[0] for e in top_10_processed],
            "top_10_skipped": [e[0] for e in top_10_skipped],
            "top_10_zip_codes_w_unique_last_names": [
                e[0] for e in top_10_zip_codes_unique
            ],
            "top_10_zip_codes": [e[0] for e in top_10_zip_codes],
        }

    def close(self):
        """
        Close connection to DB
        :return: None
        """

        LOG.info("Closing connection to db")
        self.conn.close()


def parser(data: Dict, keys: List[str], vals: Dict, count: int = 0) -> Dict:
    """
    Parse the data for the list of keys and its values
    :param data: Dict
    :param keys: List of keys to look for
    :param vals: Values if found
    :param count: Int value of the number of found keys,
    if all are found do not traverse the rest of the dictionary
    :return: The found subset of keys and values
    """

    if isinstance(data, dict):
        for key, val in data.items():
            if key in keys:
                vals[key] = val
                count = count + 1
                if count == len(keys):
                    return vals
            if isinstance(val, dict):
                return parser(val, keys, count=count, vals=vals)
        return vals


def convert_to_tuple(data: Dict) -> Tuple:
    """
    Convert the data to a tuple representing a row in the table Users
    :param data: Parsed dict
    :return: The data in tuple format
    """

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


def parsing_a_file(
    source: str, path: str, batch: List[Tuple], keys: List[str]
) -> (List[Tuple], Tuple):
    """
    Parse a json file

    :param source: Top subdirectory
    :param path: Path to file
    :param batch: Batch of processed data
    :param keys: Keys to look for in the json file
    :return: List of processed data, and meta of the file processed
    """

    records, processed, skipped = 0, 0, 0
    f_name = path.split("/")[-1]

    with open(path) as f:
        try:
            data = json.loads(json.load(f))
        except json.JSONDecodeError as e:
            LOG.info(e)
            return batch, None

    for d in data:
        records = records + 1
        d = parser(d, keys=keys, vals={})
        if d:
            processed = processed + 1
            batch.append(convert_to_tuple(d))
        else:
            skipped = skipped + 1
    return batch, (source, path, f_name, processed, skipped, records)


def traverse_and_fetch_paths(source: str = None) -> List[str]:
    """
    Traverse data directory
    :param source: root if not provided, traverses the first level of the data directory
    :return: list of file paths
    """
    base = "./data"
    paths = []

    if not source:
        return os.listdir(base)
    else:
        base = f"{base}/{source}"

        start = time.time()
        for path, subdir, files in os.walk(base):
            for name in files:
                if name.split(".")[-1] == "json":
                    paths.append(os.path.join(path, name))
        LOG.info(
            f"Traversed {base} in {time.time() - start}s and discovered {len(paths)} files"
        )
        return paths


def process():
    # this is where your code goes

    batch_size = 10000
    files_processed = []
    current_batch = []

    source = traverse_and_fetch_paths()
    manifold = Database()

    start = time.time()
    for s in source:
        if s in [".DS_Store"]:
            continue

        s_start = time.time()
        LOG.info(f"Fetching file list for {s}...")
        paths = traverse_and_fetch_paths(s)

        if len(paths) == 0:
            LOG.info("No json files found")
            return None

        for p in paths:
            p_start = time.time()
            current_batch, f_stats = parsing_a_file(
                source=s,
                path=p,
                batch=current_batch,
                keys=["first_name", "middle_name", "last_name", "zip_code"],
            )
            if f_stats:
                files_processed.append(f_stats)

            # batching

            if len(current_batch) > batch_size:
                LOG.info(
                    f"{len(current_batch)} Surpassed batch size of 10000, dumping..."
                )
                manifold.insert_into_users(rows=current_batch)
                current_batch = []

            if len(files_processed) > batch_size:
                LOG.info(
                    f"{len(files_processed)} Surpassed batch size of 10000, dumping..."
                )
                manifold.insert_into_process(rows=files_processed)
                files_processed = []

            LOG.info(f"{p} processed in {time.time() - p_start}s")
        LOG.info(f"Source: {s} processed in {time.time() - s_start}s")

    manifold.insert_into_users(rows=current_batch)
    manifold.insert_into_process(rows=files_processed)

    LOG.info(f"Data processed in {time.time() - start}s")
    manifold.check_sum()
    results = manifold.get_stats()
    print(results)

    manifold.close()


if __name__ == "__main__":
    process()
