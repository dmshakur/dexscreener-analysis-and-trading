from threading import Lock
from datetime import datetime
import sqlite3
import time
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests



# Define CSS class selectors for collecting data from html
all_tokens_class = 'ds-dex-table-row ds-dex-table-row-new'
token_class = 'ds-table-data-cell ds-dex-table-row-col-token'
token_name_class = "ds-dex-table-row-base-token-name"
token_symbol_class = "ds-dex-table-row-base-token-symbol"
price_class = 'ds-table-data-cell ds-dex-table-row-col-price'
age_class = 'ds-table-data-cell ds-dex-table-row-col-pair-age'
price_change_class = 'ds-table-data-cell ds-dex-table-row-col-price-change'
base_class = 'ds-table-data-cell'

cols = [
    'token_name',
    'token_symbol',
    'token_address',
    'current_price_usd',
    'time',
    'age',
    'buys',
    'sells',
    'volume',
    'makers',
    'price_change_5m',
    'price_change_1h',
    'price_change_6h',
    'price_change_24h',
    'liquidity',
    'fdv'
]

csv_save_location = './data/dxsc_newpair_data/'
db_path = './data/historical_price_data_sol.db'
dexscreener_api = 'https://api.dexscreener.com/latest/dex/pairs/solana/'

db_lock = Lock()


def make_api_call(path: list[str] = db_path, url: list[str] = dexscreener_api):
    try:

        with sqlite3.connect(path) as conn:
            cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                collection_addresses (
                    pair_address TEXT PRIMARY KEY
                );
            ''')
            cur.execute('''
                SELECT DISTINCT pair_address
                FROM collection_addresses;
            ''')
            pair_addresses = cur.fetchall()

        assert pair_addresses, 'pair_addresses table is empty'

        api_call = dexscreener_api + ','.join(
            [add[0] for add in pair_addresses]
        )

        date = datetime.now().isoformat()
        response = requests.get(api_call).json()

        return response, date

    except AssertionError as e:
        print('db_lock, releasing make_api_call')
        print(f'error: {e}')
        return 'empty_db', ''



def collect_data_and_insert():
    # This function makes an api request gets the reponse data,
    # and sends it to the add_data_to_db func
    while True:
        db_lock.acquire(blocking = True)
        print('db_lock, acquired collect_data_and_insert')

        response, date = make_api_call()
        add_data_to_db(response, date)

        db_lock.release()
        print('db_lock, releasing collect_data_and_insert')

        time.sleep(2)



def add_pair_address(address_data: pd.Series, path: list[str] = db_path):
    print('pre db_lock, add_pair_address')
    db_lock.acquire(blocking = True)
    print('db_lock, acquired add_pair_address')
    print('inside db_lock, add_pair_address')

    with sqlite3.connect(path) as conn:
        cur = conn.cursor()

        for pair_address in address_data:
            print(pair_address)
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                collection_addresses (
                    pair_address TEXT PRIMARY KEY
                );
            ''')
            cur.execute('''
                INSERT INTO
                    collection_addresses
                    (pair_address)
                VALUES
                    (?);
            ''', (pair_address,))
            conn.commit()

    db_lock.release()
    print('db_lock, releasing add_pair_address')



def remove_pair_address(address: list[str], path: list[str] = db_path):
    db_lock.acquire(blocking = True)
    print('db_lock acquired, remove_pair_address')

    with sqlite3.connect(path) as conn:
        cur = conn.cursor()

        cur.execute(f'''
            DELETE FROM collection_addresses
            WHERE pair_address = "{address}";
        ''')

    db_lock.release()
    print('db_lock, releasing remove_pair_address')


def add_data_to_db(data, date: list[str], path: list[str] = db_path):
    if data == 'empty_db':
        print('Database empty, ending iteration')
        return

    with sqlite3.connect(path) as conn:
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS
            pair_address_master (
                pair_address TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                dex_id TEXT NOT NULL
            );
        ''')

        for pair in data['pairs']:
            if 'pairAddress' in pair:
                cur.execute(f'''
                    SELECT 1
                    FROM pair_address_master
                    WHERE pair_address = "{pair["pairAddress"]}";
                ''')
                if not cur.fetchone():
                    cur.execute(f'''
                        INSERT INTO
                            pair_address_master
                            (pair_address, name, dex_id)
                        VALUES (
                            "{pair["pairAddress"]}",
                            "{pair["baseToken"]["name"]}",
                            "{pair["dexId"]}"
                        );
                    ''')
                    conn.commit()
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS
                "{pair["pairAddress"]}" (
                    date DATE,
                    price_usd REAL
                );
            ''')
            cur.execute(f'''
                INSERT INTO
                    "{pair["pairAddress"]}"
                    (date, price_usd)
                VALUES (
                    "{date}",
                    {pair["priceUsd"]}
                );
            ''')
            conn.commit()



def process_unformatted_number(num):
    n = re.sub(r'\D', '', num)
    if num[-1].lower() == 'k':
        return float(n) * 1000.0
    elif num[-1].lower() == 'm':
        return float(n) * 1000000.0
    elif int(num[-1]):
        return float(n)
    else:
        return f"invalid value: {n}"



def format_and_save_data(raw_html, collect_new_pairs = True):
    df = pd.DataFrame(columns=cols)
    soup = BeautifulSoup(raw_html, 'lxml')
    all_tokens = soup.find_all('a', class_=all_tokens_class)
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    for token in all_tokens:
        # separating the data for more specific data points
        basic_token_data = token.find(class_=token_class)
        token_price = token.find(class_=price_class).get_text()[1:]
        age = token.find(class_=age_class).get_text()
        price_changes = [element.get_text() for element in token.find_all(class_=price_change_class)]
        remaining_data = [element.get_text() for element in token.find_all(class_=base_class)]

        # convert age to a numeric value representing seconds
        age_split = age.split(' ')
        age = 0

        for x in age_split:
            if x[-1].lower() == 'h':
                age += int(x[:-1]) * (60**2)
            elif x[-1].lower() == 'm':
                age += int(x[:-1]) * 60
            elif x[-1].lower() == 's':
                age += int(x[:-1])


        # Structure data assuming each category has correct count of data
        token_data = pd.DataFrame([{
            'token_name': basic_token_data.find('span', class_ = token_name_class).get_text(),
            'token_symbol': basic_token_data.find('span', class_ = token_symbol_class).get_text(),
            'pair_address': token['href'].split('/')[2],
            'current_price_usd': token_price,
            'time': current_time,
            'age': age,
            'buys': re.sub(r'\D', '', remaining_data[3]),
            'sells': re.sub(r'\D', '', remaining_data[4]),
            'volume': process_unformatted_number(remaining_data[5]),
            'makers': re.sub(r'\D', '', remaining_data[6]),
            'price_change_5m': re.sub(r'%|,', '', price_changes[0]),
            'price_change_1h': re.sub(r'%|,', '', price_changes[1]),
            'price_change_6h': re.sub(r'%|,', '', price_changes[2]),
            'price_change_24h': re.sub(r'%|,', '', price_changes[3]),
            'liquidity': process_unformatted_number(remaining_data[-2]),
            'fdv': process_unformatted_number(remaining_data[-1])
        }])
        df = pd.concat([df, token_data], ignore_index=True)

    # save data collected data to a csv file
    df.to_csv(f'{csv_save_location}dxsc_newpairs_{current_time}.csv', index=False)

    # save coin data for any new coins in the database
    if collect_new_pairs:
        new_addresses = df.loc[df['age'] < 240, 'pair_address']
        print('new_addresses_df:\n', new_addresses, type(new_addresses))
        add_pair_address(new_addresses)

    return df

