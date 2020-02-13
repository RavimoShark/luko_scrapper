import pandas as pd
import numpy as np
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
CODE_RE = re.compile(r'SHARETHELOVE[a-zA-Z0-9]*\+[a-zA-Z0-9]*')
DOMAIN_RE = re.compile(r"^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)")
LUKO = 'luko'
np.random.RandomState(seed=26)
USER_AGENT = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36'}

RESTRICTED_DOMAINS =['facebook.com', 'twitter.com']
EXPL_LIMITS = 500


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
                domain = DOMAIN_RE.findall(link)[0]
                found_results.append({'keyword':keyword, 'rank':rank, 'title':title,'link':link, 'domain':domain})
                rank += 1
    return pd.DataFrame(found_results, columns=['keyword','rank','title','link','domain'])


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    #logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html

async def parse(url: str,domain:str, session: ClientSession, urls_found: set, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
    found_urls = set()
    found_codes = set()
    try: 
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e: # manage too many requests error
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
                if domain in abslink and abslink not in urls_found:
                    found_urls.add(str(abslink))
                    #logger.info("Found %s for url %s", abslink, url)
        for code in CODE_RE.findall(html):
            found_codes.add(code.replace('<[^>]*>', ''))
                #logger.info("Found %s for url %s", code, url)
        #logger.info("Found %d links for %s", len(found_urls), url)
        #logger.info("Found %d links for %s", len(found_codes), url)
    
        return list(found_urls), list(found_codes)



async def write_one(file_res:IO, url: str, domain:str, urls_found:set, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""
    res = await parse(url=url, domain=domain, urls_found=urls_found,  **kwargs)
    new_elt_per_domain ={domain :{}}
    count =0
    if not res:
        return None
    async with aiofiles.open(file_res, "a") as f:
        for u_found,c_found in zip(res[0], res[1]):
            await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;{u_found};##;{c_found};##;{False};##;{False}\n")
            new_elt_per_domain[domain]['url'] = new_elt_per_domain[domain].get('url',0)+1
            new_elt_per_domain[domain]['code'] = new_elt_per_domain[domain].get('code',0)+1
            count += 1
        if len(res[0]) > len(res[1]):
            for u_found  in res[0][count:] : 
                await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;{u_found};##;;##;{False};##;{False}\n")
                new_elt_per_domain[domain]['url'] = new_elt_per_domain[domain].get('url',0)+1
                count += 1
        elif len(res[0])<len(res[1]):
             for c_found in res[1][count:]: 
                await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;;##;{c_found};##;{False};##;{False}\n")
                new_elt_per_domain[domain]['code'] = new_elt_per_domain[domain].get('code',0)+1
                count += 1
    #logger.info("Wrote results for code found: %s for url: %s", new_elt_per_domain[domain].get('code',0), domain)
    #logger.info("Wrote results for url found: %s for url: %s", new_elt_per_domain[domain].get('url',0), domain)
    
    return new_elt_per_domain    

async def bulk_crawl_and_write(file_res: IO, sel_data: dict, urls_found: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession(read_timeout=10) as session:
        tasks = []
        urls_parsed =[]
        for domain, urls in sel_data.items():
            #logger.info("urls input %s", urls)
            ## if domain in RESTRICTED_DOMAINS:
            ##    continue
            for url in urls:
                urls_parsed.append(url)
                tasks.append(write_one(file_res=file_res, url=url, domain=domain,
                 session=session, urls_found = urls_found, **kwargs))
        res = await asyncio.gather(*tasks)
        merged_res ={}
        for res_dict in res:
            if merged_res.get(list(res_dict.keys())[0]):
                merged_res[list(res_dict.keys())[0]] = {**merged_res.get(list(res_dict.keys())[0]),**res_dict[list(res_dict.keys())[0]]}
            else:
                merged_res[list(res_dict.keys())[0]] = res_dict[list(res_dict.keys())[0]]
        for domain in merged_res.keys():
            logger.info("Wrote results for code found: %s for url: %s", merged_res[domain].get('code',0), domain)
            #logger.info("Wrote results for url found: %s for url: %s", merged_res[domain].get('url',0), domain)


def process_batch_res(data : pd.DataFrame, res_file: IO, df_file: IO, url_count=100):
    
    temp = pd.read_csv(res_file.resolve(), sep=';##;', header=0, engine='python')
    data = pd.concat([data,temp], axis=0, ignore_index=True)
    data.loc[data['parsed_url'].isin(data['source_url'].unique()),'processed'] = True
    data.loc[data['parsed_url'].isin(data['urls_sent'].unique()),'processed'] = True
    data.loc[~data['processed'],'contains_luko'] = data.loc[~data['processed'],'parsed_url'].str.contains(pat=LUKO, case=False)
    #data.sort_values(by='contains_luko', ascending=False, inplace=True)
    #codes_found = data['code'].unique()
    urls_found = data['parsed_url'].unique()
    domains = data.loc[~data.processed,'domain'].unique()
    domains_url = {}
    for domain in np.random.choice(domains,10, replace=False):
        if url_count <=0 :
            break
        expl = data[(data['code'].isnull()) & (data['domain']==domain) & (data['processed'])].shape[0]
        if expl> EXPL_LIMITS:
            logger.info('The domain %s has reached his exploration limits %d links without any new codes', domain,EXPL_LIMITS)
            continue
        urls = list(data.loc[(data['domain']==domain) & (~data['processed']),'parsed_url'].unique())
        if not urls:
            data.loc[data['domain']==domain,'processed'] = True
            logger.info('The domain %s has been entirerely processed', domain)
            continue
        else :
            limit = min(url_count,20,len(urls))
            if limit <=1:
                elt=1
            else:
                elt = np.random.randint(1,limit)
            url_count-= elt
            domains_url[domain] = np.random.choice(urls, elt, False)
    data[data.parsed_url.isin([v for val in domains_url.values() for v in val],'urls_sent')] = True
    data.to_csv(df_file.resolve(), sep=';')
    return urls_found, domains_url, data[data['code'].notnull()].shape[0]

def main_proc(n_iter=1000, search_term='SHARETHELOVE*+LUKO', search_items=50, lang='fr',
    domain_url=dict(), outpath_str='res.txt',df_file_str='final_df.csv', from_scratch=True):
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent
    outpath_res = here.joinpath(outpath_str)
    df_file = here.joinpath(df_file_str)
    with open(outpath_res, "w") as outfile:
        outfile.write("timestamp;##;source_url;##;domain;##;parsed_url;##;code;##;processed;##;contains_luko;##;urls_sent\n")
    if from_scratch:
        keyword, html = get_google_search_results(search_term, search_items, lang)
        found_results = parse_results(html, keyword)
        sel_data = pd.Series(found_results.groupby('domain')['link'].agg(lambda x :list(x.values))).to_dict()
        sel_data = {**sel_data, **domain_url}
        asyncio.run(bulk_crawl_and_write(outpath_res, sel_data,sel_data.values())) 
        res = process_batch_res(pd.DataFrame(), outpath_res, df_file)
    else :
        data = pd.read_csv(df_file.resolve(), sep=';')
    start = datetime.datetime.now()
    for i in range(1000):
        with open(outpath_res, "w") as outfile:
            outfile.write("timestamp;##;source_url;##;domain;##;parsed_url;##;code;##;processed;##;contains_luko;##;urls_sent\n")
        asyncio.run(bulk_crawl_and_write(outpath_res, res[1],res[0])) 
        res = process_batch_res(data,outpath_res, df_file)
        if not res[1] :
            duration = datetime.datetime.now() -start
            logger.info("All domain explored : %d",duration) 
        if i % 10 ==0:
           duration = datetime.datetime.now() -start
           logger.info("Scrapper duration for 10 : %d",duration)
           logger.info("We have found %d codes",res[2])


if __name__ == '__main__':
    main_proc()