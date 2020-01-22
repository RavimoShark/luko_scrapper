import pandas as pd
import requests
from bs4 import BeautifulSoup
from threading import Thread
from urllib.request import urlopen
import re

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}




def get_google_search_results(search_term, number_results, language_code):
    assert isinstance(search_term, str), 'Search term must be a string'
    assert isinstance(number_results, int), 'Number of results must be an integer'
    escaped_search_term = search_term.replace(' ', '+')
    google_url = 'https://www.google.com/search?q={}&num={}&hl={}'.format(escaped_search_term, number_results, language_code)
    response = requests.get(google_url, headers=USER_AGENT)
    response.raise_for_status()
 
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
                found_results.append({'keyword': keyword, 'rank': rank, 'title': title, 'link': link})
                rank += 1
    return found_results

def extract_domain(links):
    regex = r"^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)"
    results = re.findall(regex,','.join(links))
    domains=[]
    if results :
        domains = [domain.group(1) for domain in results]
    return domains

class ThreaderDomainExplorer(Thread):
    """scrap a webpage looking for string corresponding to
    a regex and return all links found within the same domain
    
    Arguments:
        Thread {[type]} -- [description]
    
    Returns:
        [tuple] -- [two lists containing respectively links, and codes]
    """

    def __init__(self, domain ,url, user_agent, code_regex):
        """[summary]
        
        Arguments:
            domain {[type]} -- [description]
            url {[type]} -- [description]
            user_agent {[type]} -- [description]
            code_regex {[type]} -- [description]
        """
        Thread.__init__(self)
        self.url = url
        self.user_agent = user_agent
        self.links=[]
        self.codes=[]
        self.domain=domain
        self.regex = r''+domain
        self.code_regex = code_regex
        self.page 

    def get_data(self):
        """[summary]
        
        Returns:
            [type] -- [description]
        """
        if re.match(self.regex, self.url, re.IGNORECASE):
            response = requests.get(self.url, headers=self.user_agent)
            response.raise_for_status()
            self.page = response.text
        return self.page
    

    def get_links(self):
        """get all links of the webpage within the same domain
        
        Arguments:
            soup {[type]} -- [description]
        """
        soup = BeautifulSoup(self.page, 'html.parser')
        for link in soup.findAll('a', attrs={'href': re.compile("^http?s://")}):
            if self.domain in link:
                self.links.append(link.get('href'))
        return self.links

    def get_code(self):
        """[summary]
        """
        self.codes = re.findall(self.code_regex,self.page)
        return self.codes

    
    def run(self):
        """'[summary]'
        """
        self.get_data()
        self.get_code()
        self.get_links()
        return True


    

if __name__ == '__main__':
    keyword, html = get_google_search_results('SHARETHELOVE*+LUKO', 100, 'en')
    found_results = parse_results(html, keyword)
    for dic in found_results:
        print(dic['title'])
        print(dic['link'])
