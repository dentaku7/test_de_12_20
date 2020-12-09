import csv
import glob
import gzip
import logging
import os
import random
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TaskDataGenerator')

CSV_OPTIONS = {'delimiter': ',',
               'quotechar': '\\',
               'quoting': csv.QUOTE_MINIMAL}

START_DATETIME = datetime.fromisoformat('2019-01-01')
START_TMS = START_DATETIME.timestamp() * 1000

DB_NAME = 'task_data'
TABLE_NAME = 'user_dimensions'

DATA_DIR = Path(os.environ["TASK_DATA_DIR"])
RESULT_DIR = Path(os.environ["RESULT_DATA_DIR"])

USER_DMNS_CSV = Path(f'{TABLE_NAME}.csv.gz')

CLCKSTREAM_NAME = 'clickstream'

PG_SETTINGS = {
    "host": "db",
    "port": 5432,
    "user": "postgres",
    "password": "postgres"
}

TABLE_SCHEMA = f"""
CREATE TABLE {TABLE_NAME} (
    user_id varchar(255),
    install_tms bigint
);
create index {TABLE_NAME}_index
    on {TABLE_NAME} (user_id);
"""


class CSVWriter:
    def __init__(self, file_name, max_lines):
        self._file_name = file_name
        self.max_lines = max_lines
        self.file_no = 0
        self.lines_written = 0
        self._open_new_file()

    @property
    def file_name(self):
        return f"{self._file_name}-{self.file_no}.csv.gz"

    def _open_new_file(self):
        self.gzip_file = gzip.open(DATA_DIR / Path(self.file_name), 'wt')
        logger.info(f"Saving {Path(self.file_name)}")
        self.csv_writer = csv.writer(self.gzip_file, **CSV_OPTIONS)

    def close_current_file(self):
        self.gzip_file.close()

    def open_next_file(self):
        self.lines_written = 0
        self.file_no += 1
        self.close_current_file()
        self._open_new_file()

    def write_row(self, row):
        if self.lines_written >= self.max_lines:
            self.open_next_file()
        self.csv_writer.writerow(row)
        self.lines_written += 1

    def __del__(self):
        self.close_current_file()


def generate_clickstream(max_lines, events_per_user):
    fake = Faker()
    csv_writer = CSVWriter(CLCKSTREAM_NAME, max_lines=max_lines)
    with gzip.open(DATA_DIR / USER_DMNS_CSV, 'rt') as gzip_file:
        csv_reader = csv.reader(gzip_file, **CSV_OPTIONS)
        for line in csv_reader:
            country = fake.country_code('alpha-3')
            for i in range(events_per_user):
                event_name = random.choice(['purchase', 'click', 'level_complete', 'level_fail'])
                row = [line[0],
                       country,
                       event_name,
                       round(START_TMS + random.random() * 10000000)]
                csv_writer.write_row(row)
    csv_writer.close_current_file()


def generate_dimensions(users):
    logger.info(f"Saving file {USER_DMNS_CSV}")
    with gzip.open(DATA_DIR / USER_DMNS_CSV, 'wt') as gz_writer:
        csv_writer = csv.writer(gz_writer, **CSV_OPTIONS)
        for _ in range(users):
            row = [uuid4(),
                   round(START_TMS + random.random() * 86400 * 1000)]
            csv_writer.writerow(row)
    logger.info("Done")


def get_pg_cursor(dbname):
    pg_connect = psycopg2.connect(dbname=dbname, **PG_SETTINGS)
    pg_connect.set_session(autocommit=True)
    return pg_connect.cursor()


def delete_files(path):
    file_list = glob.glob(os.path.join(path, "*"))
    for f in file_list:
        os.remove(f)


def generate_data(total_users, clickstream_file_max_lines, events_per_user):
    delete_files(DATA_DIR)
    delete_files(RESULT_DIR)
    try:
        get_pg_cursor(None).execute(f"CREATE DATABASE {DB_NAME}")
        get_pg_cursor(DB_NAME).execute(TABLE_SCHEMA)
    except Exception:
        get_pg_cursor(DB_NAME).execute(f"TRUNCATE TABLE {TABLE_NAME}")

    # Generate data
    logger.info(f"Generating {total_users} users in CSV")
    generate_dimensions(users=total_users)

    logger.info(f"Generating clickstream with {clickstream_file_max_lines} lines per file, "
                f"{events_per_user} events per user")
    generate_clickstream(max_lines=clickstream_file_max_lines, events_per_user=events_per_user)

    # Copy data to db table
    logger.info("Loading CSV file into database")
    file_path = DATA_DIR / Path(USER_DMNS_CSV)
    get_pg_cursor(DB_NAME).copy_from(gzip.open(file_path), TABLE_NAME, sep=',')

    logger.info("DONE")


if __name__ == '__main__':
    generate_data(total_users=10,
                  clickstream_file_max_lines=10,
                  events_per_user=10)
