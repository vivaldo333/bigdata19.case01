import aiofiles
from aiohttp import ClientSession
import asyncio
from collections import defaultdict
import csv
import io
import lxml.html
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import tarfile
from tqdm import tqdm

import config as cfg

YAHOO_ARCH = cfg.BUILDDIR / 'yahoo.tbz2'
YAHOO_DATA = cfg.BUILDDIR / 'yahoo.csv'
YAHOO_HTMLS = cfg.BUILDDIR / 'yahoo_html'
YAHOO_PARQUET = cfg.BUILDDIR / 'yahoo_my.parquet'
SYNC_YAHOO_HTMLS = cfg.BUILDDIR / 'yahoo_html_sync'
YAHOO_DATA = cfg.BUILDDIR / "yahoo.csv"


NASDAQ_FILES = (
    cfg.DATADIR / 'nasdaq' / 'amex.csv',
    cfg.DATADIR / 'nasdaq' / 'nasdaq.csv',
    cfg.DATADIR / 'nasdaq' / 'nyse.csv',
    )


def read_symbols():
    """Read symbols from NASDAQ dataset"""

    symbols = set()

    for filename in NASDAQ_FILES:
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbols.add(row['Symbol'].upper().strip())

    return list(sorted(symbols))


def scrape_descriptions_async():
    """Scrape companies descriptions asynchronously."""

    symbols = read_symbols()
    progress = tqdm(total=len(symbols), file=sys.stdout, disable=False)
    YAHOO_HTMLS.mkdir(parents=True, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
        }

    async def fetch(symbol, session):
        async with session.get(f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}') as response:
            text = await response.read()
            async with aiofiles.open(YAHOO_HTMLS / f'{symbol}.html', 'wb') as f:
                await f.write(text)
            progress.update(1)

    async def run(symbols):
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(fetch(symbol, session)) for symbol in symbols)
            await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
    loop.run_until_complete(asyncio.ensure_future(run(symbols)))
    progress.close()


def compress_descriptions(encoding='utf-8', batch_size=1000, compression='BROTLI'):
    """Convert tarfile to parquet"""

    names = ('symbol', 'html')

    def read_incremental():
        """Incremental generator of batches"""
        with tarfile.open(YAHOO_ARCH) as archive:
            batch = defaultdict(list)
            for member in tqdm(archive):
                if member.isfile() and member.name.endswith('.html'):
                    batch['symbol'].append(Path(member.name).stem)
                    batch['html'].append(archive.extractfile(member).read().decode(encoding))
                    if len(batch['symbol']) >= batch_size:
                        yield pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)
                        batch = defaultdict(list)
            if batch:
                yield pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)  # last partial batch

    writer = None
    for batch in read_incremental():
        if writer is None:
            writer = pq.ParquetWriter(YAHOO_PARQUET, batch.schema, use_dictionary=False, compression=compression, flavor={'spark'})
        writer.write_table(batch)
    writer.close()


def decompress_descriptions(encoding='utf-8'):
    """Convert parquet to tarfile"""

    pf = pq.ParquetFile(YAHOO_PARQUET)

    progress = tqdm(file=sys.stdout, disable=False)

    with tarfile.open(YAHOO_ARCH, 'w:bz2') as archive:
        for i in range(pf.metadata.num_row_groups):
            table = pf.read_row_group(i)
            columns = table.to_pydict()
            for symbol, html in zip(columns['symbol'], columns['html']):
                bytes = html.encode(encoding)
                s = io.BytesIO(bytes)
                tarinfo = tarfile.TarInfo(name=f'yahoo/{symbol}.html')
                tarinfo.size = len(bytes)
                archive.addfile(tarinfo=tarinfo, fileobj=s)
                progress.update(1)

    progress.close()

def parse_descriptions(src=YAHOO_PARQUET, dst=YAHOO_DATA):
    """Parse scraped pages."""
    reader = pq.ParquetFile(src)

    with tqdm(total=reader.metadata.num_rows) as progress:
        with open(dst, 'w', newline='', encoding="utf-8") as f:
            #write symbol and description from html in csv file
            writer = csv.DictWriter(f, fieldnames=['symbol', 'sector', 'industry', 'employees', 'description'])
            writer.writeheader()
            #loop for fetching parquet groups
            #to_pydict возвращает колоночное представление pyarrow таблицы
            for g in range(reader.metadata.num_row_groups):
                table = reader.read_row_group(g).to_pydict()
                #режим debug-а
                #breakpoint() c-continue, q-quit
                for symbol, html in zip(table['symbol'], table['html']):
                    tree = lxml.html.fromstring(html)
                    #обявляем словарь
                    row = {'symbol':symbol.strip()}
                    #// - пропускаем элементы до вложенности . - стартовать от текущего элимента
                    #получить тег section, что содержит тег h2, в котором есть Description текст
                    row['description'] = '\n'.join(tree.xpath('//section[h2//*[text()="Description"]]/p/text()'))
                    info = (tree.xpath('//div[@class="asset-profile-container"]//p[span[text()="Sector"]]') or [None])[0]
                    if info is not None:
                        row['sector'] = (info.xpath('./span[text()="Sector"]/following-sibling::span[1]/text()') or [''])[0]
                        row['industry'] = (info.xpath('./span[text()="Industry"]/following-sibling::span[1]/text()') or [''])[0]
                        row['employees'] = (info.xpath('./span[text()="Full Time Employees"]/following-sibling::span[1]/span/text()') or [''])[0].replace(',', '')
                    writer.writerow(row)
                    progress.update()

def parse_descriptions(src=YAHOO_PARQUET, dst=YAHOO_DATA):
    """Parse scraped pages."""

    reader = pq.ParquetFile(src)

    with tqdm(total=reader.metadata.num_rows) as progress:

        with open(dst, 'w', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['symbol', 'sector', 'industry', 'employees', 'description'])
            writer.writeheader()

            for g in range(reader.metadata.num_row_groups):
                table = reader.read_row_group(g).to_pydict()
                for symbol, html in zip(table['symbol'], table['html']):
                    tree = lxml.html.fromstring(html)

                    row = {'symbol': symbol.strip()}
                    row['description'] = '\n'.join(tree.xpath('//section[h2//*[text()="Description"]]/p/text()'))
                    info = (tree.xpath('//div[@class="asset-profile-container"]//p[span[text()="Sector"]]') or [None])[0]
                    if info is not None:
                        row['sector'] = (info.xpath('./span[text()="Sector"]/following-sibling::span[1]/text()') or [''])[0]
                        row['industry'] = (info.xpath('./span[text()="Industry"]/following-sibling::span[1]/text()') or [''])[0]
                        row['employees'] = (info.xpath('./span[text()="Full Time Employees"]/following-sibling::span[1]/span/text()') or [''])[0].replace(',', '')

                    writer.writerow(row)
                    progress.update()


def main():
    parse_descriptions()


if __name__ == '__main__':
    main()
