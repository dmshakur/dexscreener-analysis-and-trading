from datetime import datetime
import re
import pandas as pd
from bs4 import BeautifulSoup

from manage_db import add_pair_address

save_location = './data/'
time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# Define CSS class selectors
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

    for token in all_tokens:
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
                age += int(x[:-1]) * 360
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
            'time': time,
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

    df.to_csv(f'{save_location}dxsc_newpairs_{time}.csv', index=False)

    print('process_html.py ', collect_new_pairs, ' testing printing, since it doesn\'t seem to be working')
    if collect_new_pairs:
        new_addresses = df.loc[df['age'] < 300, 'pair_address']
        print('new_addresses_df:\n', new_addresses)
        add_pair_address(new_addresses)

    return df

