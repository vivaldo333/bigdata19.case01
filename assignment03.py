"""
Assignment 03
=============

The goal of this assignment is to start working on individual project.
You need to find data source, and scrape it to Parquet file.
It is recommended to scrape data asynchronously, in batches.

Run this code with

из инета берём htpm и кладём их в паркет
берём lxml и ложим из паркета в csv

    > fab run assignment03:scrape_data()
"""
import aiofiles
import asyncio
from aiohttp import ClientSession
import config as cfg
from contextlib import closing
from collections import defaultdict
import datetime
from dateutil.relativedelta import relativedelta
import json
import os
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

CURRENCYDDIR = cfg.BUILDDIR / 'currency'
CURRENCYDDIR.mkdir(parents=True, exist_ok=True)
CURRENCY_DESC_FILE = 'currency_list.json'
CURRENCY_DESC_JSON = CURRENCYDDIR / CURRENCY_DESC_FILE
CURRENCIES_AUTH_REQUEST = cfg.SECRETDIR / 'currencies.json'
AVAILABLE_CURRENCY_CODES = ['CAD','CHF','EUR','GBP','JPY','PLN']
BASE_CURRENCY='USD'

#json deserialization
__currencies_auth_data = cfg.json.loads(CURRENCIES_AUTH_REQUEST.read_text())

def get_token_for_currencies():
    URL = "https://devapi.currencycloud.com/v2/authenticate/api"
    req = requests.post(url=URL, data=__currencies_auth_data)
    TOKEN_JSON = cfg.json.loads(req.text)
    return TOKEN_JSON['auth_token']


def get_currency_desc_list():
    URL = "https://devapi.currencycloud.com/v2/reference/currencies"
    header_params = {'X-Auth-Token':get_token_for_currencies()}
    req = requests.get(url=URL, headers=header_params)
    return req.json()


def save_currency_desc_to_file():
    data = get_currency_desc_list()
    with open(CURRENCY_DESC_JSON, 'w', encoding='utf-8') as f:
        cfg.json.dump(data, f, ensure_ascii=False, indent=4)


def get_currency_codes():
    data = cfg.json.loads(CURRENCY_DESC_JSON.read_text())
    currency_codes = list(map(lambda x: x['code'], data['currencies']))
    return ",".join(str(curr_code) for curr_code in currency_codes if curr_code in AVAILABLE_CURRENCY_CODES)

def get_max_retrieve_date(date):
    today = datetime.date.today()
    return today if today < date else date


async def get_history_rates(from_d, to_d, session):
    checkedToDate = get_max_retrieve_date(to_d)
    fromDateStr = from_d.strftime("%Y-%m-%d")
    toDateStr = checkedToDate.strftime("%Y-%m-%d")
    symbols=get_currency_codes()
    url = "https://api.exchangeratesapi.io/history"
    async with session.get(f'{url}?start_at={fromDateStr}&end_at={toDateStr}&base={BASE_CURRENCY}&symbols={symbols}') as response:
        text = await response.read()
        async with aiofiles.open(CURRENCYDDIR / f'{fromDateStr}_{toDateStr}.json', 'wb') as f:
            await f.write(text)

def daterange_with_year_step(start_date, end_date):
    for n in range(int(relativedelta(end_date, start_date).years)+1):
        yield start_date + relativedelta(years=n)

def load_rate_history():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
        }

    async def run(dataList):
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(
                get_history_rates(fromD, toD, session)) for fromD, toD in zip(dataList, dataList[1:]))
            await asyncio.gather(*tasks)

    today = datetime.date.today()
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(today.year + 1, 1, 1)
    data = list(daterange_with_year_step(start_date, end_date))

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda x, y: None)
    loop.run_until_complete(
        asyncio.ensure_future(
            run(data)
        )
    )

def convert_json_to_parquet(encoding='utf-8', batch_size=180, compression='BROTLI'):
    names = ('date',
             'currency_code',
             'rate')

    fields = [
        pa.field('date', pa.string()),
        pa.field('currency_code', pa.string()),
        pa.field('rate', pa.float32(), nullable=True)
    ]
    schema = pa.schema(fields)

    def read_incremental(code):
            batch = defaultdict(list)
            for f_name in tqdm(os.listdir(CURRENCYDDIR)):
                file = os.path.join(CURRENCYDDIR, f_name)
                if os.path.isfile(file) and f_name.endswith('.json') and not f_name.endswith(CURRENCY_DESC_FILE):
                    with open(file, 'r', encoding=encoding) as f:
                        json_dict = json.load(f)
                        #unidict = {k.decode(encoding): v.decode(encoding) for k, v in json_dict['rates'].items()}

                        for k, v in json_dict['rates'].items():
                            batch['date'].append(k)
                            batch['currency_code'].append(code)
                            batch['rate'].append(v[code])

                        data = [
                            pa.array(batch['date']),
                            pa.array(batch['currency_code']),
                            pa.array(batch['rate'])
                        ]
                        table = pa.Table.from_arrays(data, schema=schema)
                        writer.write_table(table)

    for currency_code in AVAILABLE_CURRENCY_CODES:
        file_name = CURRENCYDDIR / str('data_' + currency_code + '.parquet')
        with closing(pq.ParquetWriter(file_name, schema, use_dictionary=False, compression=compression, flavor={'spark'})) as writer:
            read_incremental(currency_code)
########################################################################
def scrape_data():
    """Scrape custom data."""
    save_currency_desc_to_file()
    load_rate_history()
    convert_json_to_parquet()

def main():
    scrape_data()

if __name__ == '__main__':
    main()
