from datetime import datetime
from decimal import Decimal, getcontext
import re
import pandas as pd
from bs4 import BeautifulSoup

save_location = './dex_data/'
time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

getcontext().prec = 30

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
    'token_address',
    'current_price',
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

def format_and_save_data(raw_html):
    df = pd.DataFrame(columns=cols)
    soup = BeautifulSoup(raw_html, 'lxml')
    all_tokens = soup.find_all('a', class_=all_tokens_class)

    for token in all_tokens:
        basic_token_data = token.find(class_=token_class)
        token_price = token.find(class_=price_class).get_text()
        age = token.find(class_=age_class).get_text()
        price_changes = [element.get_text() for element in token.find_all(class_=price_change_class)]
        remaining_data = [element.get_text() for element in token.find_all(class_=base_class)]
        print(token_price[1:])

        # Structure data assuming each category has correct count of data
        token_data = pd.DataFrame([{
            'token_name': basic_token_data.find('span', class_ = token_name_class).get_text(),
            'token_symbol': basic_token_data.find('span', class_ = token_symbol_class).get_text(),
            'token_address': token['href'].split('/')[2],
            'current_price_usd': token_price,
            'time': time,
            'age': age,
            'buys': re.sub(r'\D', '', remaining_data[3]),
            'sells': remaining_data[4],
            'volume': remaining_data[5],
            'makers': remaining_data[6],
            'price_change_5m': price_changes[0],
            'price_change_1h': price_changes[1],
            'price_change_6h': price_changes[2],
            'price_change_24h': price_changes[3],
            'liquidity': remaining_data[-2],
            'fdv': remaining_data[-1]
        }])
        df = pd.concat([df, token_data], ignore_index=True)

    df.to_csv(f'{save_location}dxsc_newpairs_{time}.csv', index=False)
    return df

