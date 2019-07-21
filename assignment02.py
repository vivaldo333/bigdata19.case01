"""
Assignment 02
=============

The goal of this assignment is to implement synchronous scraping using standard python modules,
and compare the scraping speed to asynchronous mode.

Run this code with

    > fab run assignment02.py

#Description of task implementation:
    #read symbols
    #for symbol in symbols
    #url get query via urllib
    #write on disk
    #tqdm - add progress bar
"""

from yahoo import read_symbols, SYNC_YAHOO_HTMLS
from urllib.request import urlretrieve
from urllib.request import urlopen
from tqdm import tqdm

def scrape_descriptions_sync():
    """Scrape companies descriptions synchronously."""

    def scrape_file_urlretrieve(symbol):
        url = f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}'
        destination = SYNC_YAHOO_HTMLS / f'{symbol}.html'
        filename, message = urlretrieve(url, destination)
        #print("filename:", filename)
        #print("message:", message)

    def scrape_file_urlopen(symbol):
        with urlopen(f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}') as response:
            with open(SYNC_YAHOO_HTMLS / f'{symbol}.html', 'wb') as file:
                file.write(response.read())

    def load_files(symbols):
        for symbol in tqdm(symbols):
            #scrape_file_urlopen(symbol)
            scrape_file_urlretrieve(symbol)


    SYNC_YAHOO_HTMLS.mkdir(parents=True, exist_ok=True)
    symbols = read_symbols()
    load_files(symbols)

def main():
    scrape_descriptions_sync()


if __name__ == '__main__':
    main()
