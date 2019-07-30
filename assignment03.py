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
import config as cfg
import datetime
import asyncio
from aiohttp import ClientSession
from itertools import tee
import requests
from dateutil.relativedelta import relativedelta
import sys

#DATA_FILE = cfg.BUILDDIR / 'data.parquet'
CURRENCYDDIR = cfg.BUILDDIR / 'currency'
CURRENCYDDIR.mkdir(parents=True, exist_ok=True)
CURRENCY_DESC_JSON = CURRENCYDDIR / 'currency_list.json'
CURRENCIES_AUTH_REQUEST = cfg.SECRETDIR / 'currencies.json'
AVAILABLE_CURRENCY_CODES = ['CAD','CHF','EUR','GBP','JPY','PLN']
BASE_CURRENCY='USD'

#json deserialization
__currencies_auth_data = cfg.json.loads(CURRENCIES_AUTH_REQUEST.read_text())

def get_token_for_currencies():
    URL = "https://devapi.currencycloud.com/v2/authenticate/api"
    req = requests.post(url=URL, data=__currencies_auth_data)
    TOKEN_JSON = cfg.json.loads(req.text)
    #{"auth_token":"aa8a333efb2a0a8fa54f86e8f748fd3d"}
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

async def get_history_rates(from_d, to_d):
    url = "https://api.exchangeratesapi.io/history"
    parameters={'start_at':from_d,
                'end_at':to_d,
                'base':BASE_CURRENCY,
                'symbols':get_currency_codes()}
    r = requests.get(url=url, params=parameters)
    resp = await r.json()
    async with open(CURRENCYDDIR / f'{from_d}_{to_d}.json', 'wb', encoding='utf-8') as f:
        await cfg.json.dump(resp, f, ensure_ascii=False, indent=4)

def daterange_with_year_step(start_date, end_date):
    for n in range(int(relativedelta(end_date, start_date).years)):
        yield start_date + relativedelta(years=n)

def load_rate_history():

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
        }

    async def run(dataList):
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(
                get_history_rates(fromD, toD)) for fromD, toD in zip(dataList, dataList[1:]))
            await asyncio.gather(*tasks)

    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date.today() if datetime.date.today() > datetime.date(2020, 1, 1) else datetime.date(2020, 1, 1)

    data = list(daterange_with_year_step(start_date, end_date))

    #for a, b in zip(data, data[1:]):
    #    print(a,b)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda x, y: None)
    loop.run_until_complete(asyncio.ensure_future(
        run(data)
    ))

########################################################################
def scrape_data():
    """Scrape custom data."""
    # todo

def main():
    load_rate_history()
    #print(get_history_rates('2018-01-01', '2018-02-01'))


if __name__ == '__main__':
    main()
