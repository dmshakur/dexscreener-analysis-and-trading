from threading import Lock
from datetime import datetime
import sqlite3
import requests
import time


db_path = './data/historical_price_data_sol.db'


def make_api_call(path: str = db_path):
    try:
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        cur = conn.cursor()
        cur.execute('''
            SELECT DISTINCT pair_address
            FROM collection_addresses;
        ''')
        conn.commit()
        api_call = db_path + ','.join(
            [add[0] for add in cur.fetchall()]
        )
        conn.close()

        date = datetime.now().isoformat()
        response = requests.get(api_call).json()
        print('manage_db.py, api call:\n', api_call, '\napi response:\n', response)

        return response, date

    except Exception as e:
        print(f'error: {e}')
        return 'empty_db', ''


def collect_data_and_insert():
    # This function makes an api request gets the reponse data,
    # and sends it to the add_data_to_db func
    while True:
        response, date = make_api_call()
        add_data_to_db(response, date)
        time.sleep(2)


def add_pair_address(address_data: list[str], path: list[str] = db_path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    cur = conn.cursor()

    for pair_address in address_data:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS
            collection_addresses (
                pair_address TEXT PRIMARY KEY
            );
            INSERT INTO
                collection_addresses
                (pair_address)
            VALUES
                ({pair_address});
        ''')
        conn.commit()
    conn.close()



def remove_pair_address(address: list[str], path: list[str] = db_path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    cur = conn.cursor()

    cur.execute(f'''
        DELETE FROM collection_addresses
        WHERE pair_address = '{address}';
    ''')
    conn.commit()
    conn.close()



def add_data_to_db(data, date: list[str], path: list[str] = db_path):
    if data == 'empty_db':
        print('Database empty, ending iteration')
        return
    print('add_data_to_db, type(data) arg: ', type(data))

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")

    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS
        pair_address_master (
            pair_address TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            dex_id TEXT NOT NULL
        );
    ''')
    conn.commit()

    for pair in data['pairs']:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS
            {pair["pairAddress"]} (
                date DATE,
                price_usd REAL
            );


            INSERT INTO
                master_table
                (pair_address, name, dex_id)
            VALUES (
                {pair["pairAddress"]},
                {pair["baseToken"]["name"]},
                {pair["dexId"]}
            );


            INSERT INTO
                {pair["pair_address"]}
                (date, price_usd)
            VALUES (
                {date},
                {pair["priceUsd"]}
            );
        ''')
        conn.commit()
    conn.close()
