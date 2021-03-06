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
import argparse


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
DOMAIN_RE = re.compile(
    r"^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)")
LUKO = 'luko'
URL_REGEX = {'1parrainage.com': re.compile(r'.*1671.*')}
DOMAIN_URL = {
    'joemobile-avis.fr': ['https://www.joemobile-avis.fr/luko/code-promo-luko']}
USER_AGENT = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36'}
RESTRICTED_DOMAINS = ['facebook.com', 'twitter.com']
EXPL_LIMITS = 1000


def get_user_agent_list(user_agent_file: str, n_iter: int) -> list:
    """get a list of random user agent among csv file with a column [User Agent]

    Arguments:
        user_agent_file {str} -- path to user_agent dataframe in csv
        n_iter {int} -- number of user agent requested

    Returns:
        list -- list of user agent strings
    """
    df_user_agent = pd.read_csv(user_agent_file)
    return list(np.random.choice(df_user_agent['User Agent'].values, n_iter))


def get_google_search_results(search_term, number_results, language_code):
    """return the result of a google search as an ht;l response

    Arguments:
        search_term {str} -- search string  put into the google search engine
        number_results {int} -- number of results wanted
        language_code {str} -- language trigram for the google search engine

    Returns:
        [tuple] -- search term used and html response of the google search engine
    """
    assert isinstance(search_term, str), 'Search term must be a string'
    assert isinstance(
        number_results, int), 'Number of results must be an integer'
    escaped_search_term = search_term.replace(' ', '+')
    google_url = 'https://www.google.com/search?q={}&num={}&hl={}'.format(
        escaped_search_term, number_results, language_code)
    response = requests.get(google_url, headers=USER_AGENT)
    response.raise_for_status()
    logger.info("Got response [%s] for URL: %s",
                response.status_code, google_url)

    return search_term, response.text


def parse_results(html, keyword):
    """[summary]

    Arguments:
        html {str} -- google search engine html response
        keyword {str} -- search term

    Returns:
        pandas.DataFrame -- Dataframe with the following columns ['keyword', 'rank', 'title', 'link', 'domain']
    """
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
                found_results.append(
                    {'keyword': keyword, 'rank': rank, 'title': title, 'link': link, 'domain': domain})
                rank += 1
    return pd.DataFrame(found_results, columns=['keyword', 'rank', 'title', 'link', 'domain'])


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.

    Arguments:
        url {str} -- url to fetch
        session {ClientSession} -- ClientSession

    Returns:
        str -- html response
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    # logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html


async def parse(url: str, domain: str, session: ClientSession, urls_found: set,  **kwargs) -> tuple:
    """parse the url, while looking href links and string which has the pattern
    CODE_RE

    Arguments:
        url {str} -- url
        domain {str} -- domaim
        session {ClientSession} -- request session
        urls_found {set} -- set of urls already found

    Returns:
        tuple -- list found urls within a page, list of found codes within a page
    """
    found_urls = set()
    found_codes = set()
    url_re = URL_REGEX.get(domain)
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:  # manage too many requests error
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
                    if url_re:
                        for url in url_re.findall(str(abslink)):
                            found_urls.add(str(abslink))
                    #        # logger.info("Found %s for url %s", abslink, url)
                    else:
                        found_urls.add(str(abslink))
        for code in CODE_RE.findall(html):
            found_codes.add(code.replace('<[^>]*>', ''))
            # logger.info("Found %s for url %s", code, url)
        # logger.info("Found %d links for %s", len(found_urls), url)
        # logger.info("Found %d links for %s", len(found_codes), url)

        return list(found_urls), list(found_codes)


async def write_one(file_res: IO, url: str, domain: str, urls_found: set, **kwargs) -> dict:
    """write the result asynchronously in text file

    Arguments:
        file_res {IO} -- file where we put the result
        url {str} -- url scrapped
        domain {str} -- domain scrapped
        urls_found {set} -- urls found

    Returns:
        dict -- dictionnary counting new urls and codes found per domain
    """
    res = await parse(url=url, domain=domain, urls_found=urls_found,  **kwargs)
    new_elt_per_domain = {domain: {}}
    count = 0
    if not res:
        return None
    async with aiofiles.open(file_res, "a") as f:
        for u_found, c_found in zip(res[0], res[1]):
            await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;{u_found};##;{c_found};##;{False};##;{False}\n")
            new_elt_per_domain[domain]['url'] = new_elt_per_domain[domain].get(
                'url', 0)+1
            new_elt_per_domain[domain]['code'] = new_elt_per_domain[domain].get(
                'code', 0)+1
            count += 1
        if len(res[0]) > len(res[1]):
            for u_found in res[0][count:]:
                await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;{u_found};##;;##;{False};##;{False}\n")
                new_elt_per_domain[domain]['url'] = new_elt_per_domain[domain].get(
                    'url', 0)+1
                count += 1
        elif len(res[0]) < len(res[1]):
            for c_found in res[1][count:]:
                await f.write(f"{datetime.datetime.now().timestamp()};##;{url};##;{domain};##;;##;{c_found};##;{False};##;{False}\n")
                new_elt_per_domain[domain]['code'] = new_elt_per_domain[domain].get(
                    'code', 0)+1
                count += 1
    # logger.info("Wrote results for code found: %s for url: %s", new_elt_per_domain[domain].get('code',0), domain)
    # logger.info("Wrote results for url found: %s for url: %s", new_elt_per_domain[domain].get('url',0), domain)

    return new_elt_per_domain


async def bulk_crawl_and_write(file_res: IO, sel_data: dict, urls_found: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`.

    Arguments:
        file_res {IO} -- the text file where we write result for the scrapped batched url
        sel_data {dict} -- a dictionnary with key domains and values list of urls to be crawled
        urls_found {set} -- a set of url already found
    """

    # timeout strategy a lot of computer memory
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
    # and cpu i have a big gap between Luko laptop and my beast at home
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        urls_parsed = []
        for domain, urls in sel_data.items():
            # logger.info("urls input %s", urls)
            # if domain in RESTRICTED_DOMAINS:
            # continue
            for url in urls:
                urls_parsed.append(url)
                tasks.append(write_one(file_res=file_res, url=url, domain=domain,
                                       session=session, urls_found=urls_found, **kwargs))
        res = await asyncio.gather(*tasks)
        merged_res = {}
        for res_dict in res:
            if merged_res.get(list(res_dict.keys())[0]):
                merged_res[list(res_dict.keys())[0]] = {
                    **merged_res.get(list(res_dict.keys())[0]), **res_dict[list(res_dict.keys())[0]]}
            else:
                merged_res[list(res_dict.keys())[0]
                           ] = res_dict[list(res_dict.keys())[0]]
        for domain in merged_res.keys():
            logger.info("Wrote results for code found: %s for url_found: %s for url: %s",
                        merged_res[domain].get('code', 0), merged_res[domain].get('url', 0), domain)
            # logger.info("Wrote results for url found: %s for url: %s", merged_res[domain].get('url',0), domain)


def process_batch_res(data: pd.DataFrame, res_file: IO, df_file: IO, url_count=100) -> tuple:
    """processing the results of each batch:
       updating the dataframe containing the following columns [timestamp, source_url, domain,
       parsed_url, code, processed, contains_luko, urls_sent]
       updating the set of urls found
       builting the new dictionnary with keys domain and values list of urls to be scrapped


    Arguments:
        data {pd.DataFrame} -- DataFrame  contaiining [timestamp, source_url, domain,
                               parsed_url, code, processed, contains_luko, urls_sent].
        res_file {IO} -- path where we write batch of 100 requests.
        df_file {IO} -- path where we store the pd.DataFrame data

    Keyword Arguments:
        url_count {int} -- number of requests processes asynchronously at each batch (default: {100})

    Returns:
        tuple -- list of urls already found, dictionnary domain :[url to be scrapped], number of codes found, dataframe data
    """
    print('######## initial shape ', data.shape)
    temp = pd.read_csv(res_file.resolve(), sep=';##;',
                       header=0, engine='python')
    print('######## Temp shape ', temp.shape)
    if temp.shape[0] > 0:
        data = pd.concat([data, temp], axis=0, ignore_index=True)
    print('######## data shape ', data.shape)
    data.loc[data['parsed_url'].isin(
        data['source_url'].unique()), 'processed'] = True
    data.loc[data['parsed_url'].isin(
        data['urls_sent'].unique()), 'processed'] = True
    # data.loc[~data['processed'], 'contains_luko'] = data.loc[~data['processed'],
    #                                                         'parsed_url'].str.contains(pat=LUKO, case=False)
    # data.sort_values(by='contains_luko', ascending=False, inplace=True)
    # codes_found = data['code'].unique()
    urls_found = data['parsed_url'].unique()
    domains = data.loc[~data.processed, 'domain'].unique()
    print("####domain left to explore : ", domains)
    domains_url = {}
    for domain in np.random.choice(domains, min(10, len(domains)), replace=False):
        if url_count <= 0:
            break
        expl = data[(data['code'].isnull()) & (
            data['domain'] == domain) & (data['processed'])].shape[0]
        if expl > EXPL_LIMITS:
            logger.info(
                'The domain %s has reached his exploration limits %d links without any new codes', domain, EXPL_LIMITS)
            data.loc[data['domain'] == domain, 'processed'] = True
            continue
        urls = list(data.loc[(data['domain'] == domain) & (
            ~data['processed']), 'parsed_url'].unique())
        if not urls:
            data.loc[data['domain'] == domain, 'processed'] = True
            logger.info('The domain %s has been entirely processed', domain)
            continue
        else:
            limit = min(url_count, 20, len(urls))
            if limit <= 1:
                elt = 1
            else:
                elt = np.random.randint(1, limit)
            url_count -= elt
            domains_url[domain] = np.random.choice(urls, elt, False)
    data.loc[data.parsed_url.isin(
        [v for val in domains_url.values() for v in val]), 'urls_sent'] = True

    return urls_found, domains_url, data[data['code'].notnull()].shape[0], data


def main_proc(n_iter=1000, search_term='SHARETHELOVE+Luko+code', search_items=50, lang='fr',
              domain_url=dict(), outpath_str='res.txt', df_file_str='final_df.csv', from_scratch=False,
              user_agent_file='./parameters/User_Agent.csv'):
    """ Function wrapping the scrapper

    Keyword Arguments:
    n_iter {int} -- request batch number (default: {1000})
    search_term {str} -- google search term (default: {'SHARETHELOVE+Luko+code'})
    search_items {int} -- number of  top links retreieved from the search (default: {50})
    lang {str} -- trigramm language code for the google search engine (default: {'fr'})
    domain_url {[type]} -- domain :list of urls to add to the search result (default: {dict()})
    outpath_str {str} -- path where we store each batch result (default: {'res.txt'})
    df_file_str {str} -- path where we store csv dataframe result (default: {'final_df.csv'})
    from_scratch {bool} -- do we start from an existing dataframe (default: {False})
    user_agent_file {str} -- path to user agent dataframe (default: {'./parameters/User_Agent.csv'})
    """
    assert sys.version_info >= (3, 7), 'Script requires Python 3.7+.'
    np.random.seed(26)
    user_agent_list = get_user_agent_list(user_agent_file, n_iter)
    here = pathlib.Path(__file__).parent
    outpath_res = here.joinpath(outpath_str)
    df_file = here.joinpath(df_file_str)
    with open(outpath_res, "w") as outfile:
        outfile.write(
            "timestamp;##;source_url;##;domain;##;parsed_url;##;code;##;processed;##;contains_luko;##;urls_sent\n")
    if from_scratch:
        keyword, html = get_google_search_results(
            search_term, search_items, lang)
        found_results = parse_results(html, keyword)
        sel_data = pd.Series(found_results.groupby('domain')[
                             'link'].agg(lambda x: list(x.values))).to_dict()
        sel_data = {**sel_data, **domain_url}
        search_res = here.joinpath(
            './search/search_result_' + datetime.datetime.now().isoformat())
        url_batch = [url for urls in sel_data.values() for url in urls]
        pd.DataFrame(url_batch).to_csv(search_res.resolve())
        asyncio.run(bulk_crawl_and_write(
            outpath_res, sel_data, set(url_batch), **{'headers': {'User-Agent': user_agent_list[0]}}))
        data = pd.DataFrame()
        res = process_batch_res(data, outpath_res, df_file)
        res[3].to_csv(df_file.resolve(), sep=';', index=False)
    else:
        data = pd.read_csv(df_file.resolve(), sep=';')
        res = process_batch_res(data, outpath_res, df_file)
    start = datetime.datetime.now()
    for i in range(n_iter):
        with open(outpath_res, "w") as outfile:
            outfile.write(
                "timestamp;##;source_url;##;domain;##;parsed_url;##;code;##;processed;##;contains_luko;##;urls_sent\n")
        asyncio.run(bulk_crawl_and_write(
            outpath_res, res[1], res[0], **{'headers': {'User-Agent': user_agent_list[i]}}))
        res = process_batch_res(res[3], outpath_res, df_file)
        if not res[1]:
            duration = datetime.datetime.now() - start
            logger.info("All domain explored : %d", duration)
            res[3].to_csv(df_file.resolve(), sep=';', index=False)
        if i % 10 == 0:
            duration = datetime.datetime.now() - start
            logger.info("Scrapper duration for 10 batches: %d", duration)
            logger.info("We have found %d codes", res[2])
            res[3].to_csv(df_file.resolve(), sep=';', index=False)
    res[3].to_csv(df_file.resolve(), sep=';', index=False)


parser = argparse.ArgumentParser(description='Luko Scrapper')
parser.add_argument('--n_iter', default=1000, type=int,
                    help='number of maximum iteratons')
parser.add_argument('--search_term', default='SHARETHELOVE+Luko+code',
                    type=str, help='Google search string')
parser.add_argument('--lang', default='fr', type=str,
                    help='Google search language')
parser.add_argument('--domain_url', default=DOMAIN_URL, type=dict,
                    help='Domain url dictionnary to add to google search results')
parser.add_argument('--outpath_str', default='res.txt', type=str,
                    help='path file where each result iteration is saved')
parser.add_argument('--df_file_str', default='final_df.csv',
                    type=str, help='csv path where result dataframe are saved')
parser.add_argument('--from_scratch', default=False, type=bool,
                    help='Bool variable to start from scratch Google search')
parser.add_argument('--user_agent_file', default='./parameters/User_Agent.csv', type=str,
                    help='Path to the user agent dataframe csv')


if __name__ == '__main__':
    args = parser.parse_args()
    n_iter = args.n_iter
    search_term = args.search_term
    lang = args.lang
    domain_url = args.domain_url
    outpath_str = args.outpath_str
    df_file_str = args.df_file_str
    from_scratch = args.from_scratch
    user_agent_file = args.user_agent_file
    main_proc(n_iter=n_iter, search_term=search_term, lang=lang, domain_url=domain_url,
              outpath_str=outpath_str, df_file_str=df_file_str, from_scratch=from_scratch, user_agent_file=user_agent_file)
