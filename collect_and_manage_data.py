'''
collect_and_manage_data.py

currently all data is stored in databases but I am going to
shift that over to csv files for better access, there have
also been a lot of issues with concurrent access that should be solved by
changing the data pipeline around


data organization:
    master csv file: contains metadata for coins
    collection csv file: contains up to 1000 coin pair addresses
    coin specific csv files: contains pricing data over time

functions:
    make_api_call
    collect_price_data
    add_pair_address
    remove_pair_address
    add_data_to_db
    format_and_save_data
'''

from datetime import datetime
import os
import re
import time

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

csv_save_path = './data/dxsc_newpair_data/'
price_data_path = './data/token_price_data/'
master_path = './data/master_token_data.csv'
collection_path = './data/collection_addresses.csv'
dexscreener_api = 'https://api.dexscreener.com/latest/dex/pairs/solana/'



def make_api_call(path: list[str] = collection_path, url: list[str] = dexscreener_api):
    try:
        df = pd.read_csv(path)

        api_call = dexscreener_api + ','.join(df['pair_addresses'])

        response = requests.get(api_call).json()
        date = datetime.now().isoformat()

        return response, date

    except AssertionError as e:
        print('db_lock, releasing make_api_call')
        print(f'error: {e}')
        return 'empty_file', None



def collect_price_data():
    while True:
        start_time = time.time()

        response, date = make_api_call()
        add_data_to_db(response, date)

        end_time = time.time()
        print(f'collect_price_data execution time: {(end_time - start_time) * 1000} milliseconds')

        time.sleep(2)




def add_collection_address(address_data: pd.Series, path: list[str] = collection_path):
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame({'pair_address': []})

    updated_df = pd.concat(
        [df, address_data.to_frame(name = 'pair_address')],
        ignore_index = True
    )

    updated_df.to_csv(path)



def add_data_to_db(data, date: list[str], path: list[str] = master_path, token_path: list[str] = price_data_path):
    if data == 'empty_file':
        print('Database empty, ending iteration')
        return

    # creating or loading dataframe 
    if os.path.exists(path):
        master_df = pd.read_csv(path)
    else:
        master_df = pd.DataFrame(
            columns = [
                'pair_address',
                'name',
                'symbol',
                'buys',
                'sells',
                'dex_id'
            ]
        )

    # creating a copy of the df to work with
    updated_df = master_df.copy(deep = True)

    for pair in data['pairs']:
        if 'pairAddress' in pair:
            if pair['pairAddress'] not in master_df['pair_address'].values:
                pair_data = {
                    'pair_address': pair['pairAddress'],
                    'name': pair['baseToken']['name'],
                    'symbol': pair['baseToken']['symbol'],
                    'dex_id': pair['dexId']
                }
                updated_df = updated_df.concat([updated_df, pair_data], ignore_index = True)

        # creating or loading price dataframe
        if os.path.exists():
            token_df = pd.read_csv()
        else:
            token_df = pd.DataFrame(columns = ['price_data', 'time'])

        # updating pricing data
        updated_token_df = pd.concat([
                token_df,
                pd.DataFrame({'price_data': pair['priceUsd'], 'time': date})
        ])
        updated_token_df.to_csv()

    updated_df.to_csv(path)



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
        add_collection_address(new_addresses)

    return df
