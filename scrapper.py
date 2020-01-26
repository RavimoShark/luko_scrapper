import pandas as pd
import requests
from bs4 import BeautifulSoup
from threading import Thread
from urllib.request import urlopen
import re
import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
import datetime

import aiofiles
import aiohttp
from aiohttp import ClientSession
import pathlib
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("luko_scrapper")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')
CODE_RE = re.compile(r'SHARETHELOVE\+\S*')
DOMAINN_RE = re.compile(r"^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)")

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}



def get_google_search_results(search_term, number_results, language_code):
    assert isinstance(search_term, str), 'Search term must be a string'
    assert isinstance(number_results, int), 'Number of results must be an integer'
    escaped_search_term = search_term.replace(' ', '+')
    google_url = 'https://www.google.com/search?q={}&num={}&hl={}'.format(escaped_search_term, number_results, language_code)
    response = requests.get(google_url, headers=USER_AGENT)
    response.raise_for_status()
    logger.info("Got response [%s] for URL: %s", response.status_code, google_url)
 
    return search_term, response.text

def parse_results(html, keyword):
    soup = BeautifulSoup(html, 'html.parser')
    found_results = []
    rank = 1
    result_block = soup.find_all('div', attrs={'class': 'g'})
    for result in result_block:
        link = result.find('a', href=True)
        title = result.find('h3')
        # description = result.find('span', attrs={'class': 'st'})
        if link and title:
            link = link['href']
            title = title.get_text()
            # if description:
            #    description = description.get_text()
            if link != '#':
                domain = DOMAINN_RE.findall(link)[0]
                found_results.append({'keyword':keyword, 'rank':rank, 'title':title,'link':link, 'domain':domain})
                rank += 1
    return pd.DataFrame(found_results, columns=['keyword','rank','title','link','domain'])


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html

async def parse(url: str,domain:str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
    found_urls = set()
    found_codes = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found_urls, found_codes
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return found_urls, found_codes
    else:
        for link in HREF_RE.findall(html):
            try:
                abslink = urllib.parse.urljoin(url, link)
            except (urllib.error.URLError, ValueError):
                logger.exception("Error parsing URL: %s", link)
                pass
            else:
                if domain in abslink :
                    found_urls.add(abslink)
        for code in CODE_RE.findall(html):
            found_codes.add(code)

        logger.info("Found %d links for %s", len(found_urls), url)
        logger.info("Found %d links for %s", len(found_codes), url)
        
        return found_urls, found_codes



async def write_one(url_file: IO, code_file:IO, url: str, domain:str, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""
    res = await parse(url=url, domain=domain,  **kwargs)
    nb_code_found =0
    if not res:
        return None
    async with aiofiles.open(url_file, "a") as f:
        for p in res[0]:
            await f.write(f"{datetime.datetime.now().timestamp()}\t{url}\t{domain}\t{p}\t{False}\n")
    async with aiofiles.open(code_file, "a") as f:
        for code in res[1]:
            await f.write(f"{datetime.datetime.now().timestamp()}\t{url}\t{domain}\t{code}\n")
            nb_code_found+=1

        logger.info("Wrote results for code found: %s", nb_code_found)
    return nb_code_found    

async def bulk_crawl_and_write(url_file: IO, code_file:IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url, domain in urls:
            tasks.append(
                write_one(url_file=url_file,code_file=code_file, url=url, domain=domain, session=session, **kwargs)
            )
        res = await asyncio.gather(*tasks)
    return res

def process_batch_res(fetch_url :set,nb_code_found:int, url_file:IO, code_file:IO, domain:str ):
    
    data_url = pd.read_csv(url_file.resolve(), sep='/t', header=0)
    data_url.loc[data_url['parsed_url'].isin(fetch_url),'processed'] = True
    urls_to_parse = set()
    codes_found = set()
    if nb_code_found>0:
        data_code = pd.read_csv(code_file.resolve(), sep='/t', header=0)
        codes_found = data_code.loc[data_code['domain']==domain,'code'].unique()
        urls_to_parse = data_url.loc[(data_url['domain']==domain) & (~data_url['processed']),'parsed_url'].unique()
        if not urls_to_parse :
            print('no more url in domain')
        elif len(urls_to_parse)>100:
            urls_to_parse=urls_to_parse[:100]
    return codes_found, urls_to_parse



if __name__ == '__main__':
    keyword, html = get_google_search_results('SHARETHELOVE*+LUKO', 100, 'en')
    found_results = parse_results(html, keyword)
    urls = found_results.groupby('domain')['link'].first().values
    # first time we explore all th url
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent

    # with open(here.joinpath("urls.txt")) as infile:
    #    urls = set(map(str.strip, infile))

    outpath_urls = here.joinpath("foundurls.txt")
    outpath_code = here.joinpath("foundcodes.txt")
    with open(outpath_urls, "w") as outfile:
        outfile.write("timestamp\tsource_url\tdomain\tparsed_url\tprocessed\n")
    with open(outpath_code, "w") as outfile:
        outfile.write("timestamp\tsource_url\tdomain\tcode\n")
    #loop over domain
    # process batch
    # iter
    #asyncio.run(bulk_crawl_and_write(file=outpath, urls=urls))
    #for domain in found_results.domain.unique():
    #    codes_found, urls_to_parse = process_batch_res()