import requests
import time
from typing import Dict, List, Tuple, Any
import sqlite3
import feedparser
import webbrowser
import ssl

if hasattr(ssl, '_create_unverified_conext'):
    ssl._create_default_https_context = ssl._create_default_https_context


def get(cursor: sqlite3.Cursor):
    entries = []
    feed = feedparser.parse("https://stackoverflow.com/jobs/feed")
    feed_entries = feed.entries
    for e in feed_entries:
        entries.append(e.keymap)
    return entries


def create_table(cursor: sqlite3.Cursor):
    """

    :type cursor: object
    """
    create_statement1 = (f"""CREATE TABLE IF NOT EXISTS  stackoverflow_jobs(
    e.guid TEXT PRIMARY KEY,
    e.link TEXT,
    e.data TEXT,
    e.title TEXT NOT NULL,
    e.description TEXT,
    );
        """)
    cursor.execute(create_statement1)


def save_to_db(stackoverflow):
    insert_statement1 = f"""INSERT INTO stackoverflow_jobs(
        e.guid, e.link, e.data, e.title, e.description,)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)"""


def get_github_jobs_data() -> List[Dict]:
    """retrieve github jobs data in form of a list of dictionaries after json processing"""
    all_data = []
    page = 1
    more_data = True
    while more_data:
        url = f"https://jobs.github.com/positions.json?page={page}"
        raw_data = requests.get(url)
        if "GitHubber!" in raw_data.text:  # sometimes if I ask for pages too quickly I get an error; only happens in testing
            continue  # trying continue, but might want break
        if not raw_data.ok:  # if we didn't get a 200 response code, don't try to decode with .json
            continue
        partial_jobs_list = raw_data.json()
        all_data.extend(partial_jobs_list)
        if len(partial_jobs_list) < 50:
            more_data = False
        time.sleep(.1)  # short sleep between requests so I dont wear out my welcome.
        page += 1
    return all_data


def save_data(data, filename='data.txt'):
    with open(filename, 'a', encoding='utf-8') as file:
        for item in data:
            print(item, file=file)


def hard_code_create_table(cursor: sqlite3.Cursor):
    create_statement = f"""CREATE TABLE IF NOT EXISTS hardcode_github_jobs(
    id TEXT PRIMARY KEY,
    type TEXT,
    url TEXT,
    created_at TEXT,
    company TEXT NOT NULL,
    company_url TEXT,
    location TEXT,
    title TEXT NOT NULL,
    description TEXT,
    how_to_apply TEXT,
    company_logo TEXT
    );
        """
    cursor.execute(create_statement)


def hard_code_save_to_db(cursor: sqlite3.Cursor, all_github_jobs: List[Dict[str, Any]]):
    # in the insert statement below we need one '?' for each column, then we will use a second param with each of the values
    # when we execute it. SQLITE3 will do the data sanitization to avoid little bobby tables style problems
    insert_statement = f"""INSERT INTO hardcode_github_jobs(
        id, type, url, created_at, company, company_url, location, title, description, how_to_apply, company_logo)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)"""
    for job_info in all_github_jobs:
        # first turn all the values from the jobs dict into a tuple
        data_to_enter = tuple(job_info.values())
        cursor.execute(insert_statement, data_to_enter)



def open_db(filename: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    db_connection = sqlite3.connect(filename)  # connect to existing DB or create new one
    cursor = db_connection.cursor()  # get ready to read/write data
    return db_connection, cursor


def close_db(connection: sqlite3.Connection):
    connection.commit()  # make sure any changes get saved
    connection.close()


def main() -> object:
    jobs_table_name = 'github_jobs_table'  # might be better as a constant in global space
    jobs_table_name = 'stackoverflow_table'
    db_name = 'jobdemo.sqlite'
    connection, cursor = open_db(db_name)
    data = get_github_jobs_data()
    create_table(cursor)
    save_to_db()
    hard_code_create_table(cursor)
    hard_code_save_to_db(cursor, data)
    # desc = make_column_description_from_json_dict(data[0])  # assumes all records have the same fields so make table from first
    # create_table(cursor, desc, jobs_table_name)
    # save_to_db(data, cursor, jobs_table_name)
    close_db(connection)

if __name__ == '__main__':
    main()