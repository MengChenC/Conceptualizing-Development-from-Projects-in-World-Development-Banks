import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
import random
import pytz
import os
import concurrent.futures

PATH = os.path.abspath(os.getcwd())
URL = 'https://www.iadb.org/en/projects-search?country=&sector=&status=&query='
BASE_URL = 'https://www.iadb.org'
num_workers = 8

columns = ["Project Status", "Country", "Date", "Project Name", "Project Number", "Sector", "Subsector", "Project Type", "Social Impact Category", "Loan Amount", "Bank", "URL", "Description"]
project_urls_global = []
project_list = []

def get_random_ua():
    '''
    return a random user-agent for headers
    '''
    random_ua = ''
    ua_file = 'ua_file.txt'
    try:
        with open(ua_file) as f:
            lines = f.readlines()
        if len(lines) > 0:
            prng = np.random.RandomState()
            index = prng.permutation(len(lines) - 1)
            idx = np.asarray(index, dtype=np.integer)[0]
            random_ua = lines[int(idx)].strip()
    except Exception as ex:
        print('Exception in random_ua')
        print(str(ex))
    finally:
        return random_ua

def get_one_project_url(URL):
    '''
    Process one page and saves project tuple (url, name) into project_tuples object.
    - Inputs: (string) URL of bank website
    - Outputs: none, adds data to global project_tuples variable
    '''
    ua = get_random_ua()
    headers = {"User-Agent": ua}
    source = requests.get(URL, headers=headers).text
    soup = BeautifulSoup(source, "html.parser")
    for project in soup.find_all('tr', {'class':['odd','even']}):
        project_link = project.find('a')['href']
        project_date = project.find_all('td')[-1].text
        project_urls_global.append((BASE_URL + project_link, project_date))

def find_last_page():
    '''
    Returns integer value of last page on website.
    - Inputs: none
    - Outputs: (int) last page
    '''
    source = requests.get(URL).text
    soup = BeautifulSoup(source, "html.parser")
    last = soup.find('li', {"class":"pager__item pager__item--last"})
    last_page = int(last.find("a")["href"].split('=')[5])
    return last_page

def scrape_projects(URL_tuple):
    '''
    Scrapes one URL page for project data.
    - Inputs: (string) URL for one project
    - Outputs: none, adds project to global project_df variable
    '''
    url = URL_tuple[0]
    time.sleep(random.uniform(1, 5))
    ua = get_random_ua()
    headers = {"User-Agent": ua}
    response = requests.get(url, headers=headers).text
    soup = BeautifulSoup(response, 'html.parser')
    try:
        country = soup.find(text="\n                    Project Country                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        country = "N/A"
    try:
        name = soup.find("h1", {"class":"project-title"}).text.split(":")[1].strip()
    except AttributeError:
        name = "N/A"
    try:
        status = soup.find(text="\n                    Project Status                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        status = "N/A"
    try:
        # approval_date = soup.find(text="\n                    Approval date                ").findParent().findNextSibling().text.strip()
        approval_date = URL_tuple[1]
    except AttributeError:
        approval_date = "N/A"
    try:
        project_number = soup.find(text="\n                    Project Number                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        project_number = "N/A"
    try:
        sector = soup.find(text="\n                    Project Sector                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        sector = "N/A"
    try:
        subsector = soup.find(text="\n                    Project Subsector                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        subsector = "N/A"
    try:
        project_type = soup.find(text="\n                    Project Type                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        project_type = "N/A"
    try:
        impact_category = soup.find(text="\n                    Environmental and social impact category                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        impact_category = "N/A"
    try:
        amount = soup.find(text="\n                    Amount                ").findParent().findNextSibling().text.strip()
    except AttributeError:
        amount = "N/A"
    try:
        description = soup.find("p", {"class":"project-description"}).text.strip()
    except AttributeError:
        description = "N/A"
    site = "IDB"
    next_row = [status, country, approval_date, name, project_number, sector, subsector, project_type, impact_category, amount, site, url, description]
    project_list.append(next_row)

def multiprocess():
    '''
    Multiprocesses the scrape_projects function, by running this function on multiple URLs at once.
    '''
    print('Multiprocessing')
    last_page = find_last_page()
    # last_page = 1
    for page_num in range(last_page+1):
        get_one_project_url('https://www.iadb.org/en/projects-search?country=&sector=&status=&query=&page=' + str(page_num))

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # futures = [executor.submit(get_one_project_url, 'https://www.iadb.org/en/projects-search?country=&sector=&status=&query=&page=' + str(page_num)) for page_num in range(last_page+1)]
        # concurrent.futures.wait(futures)
        # futures = [executor.submit(scrape_projects, project_urls_global)]
        # concurrent.futures.wait(futures)
        executor.map(scrape_projects, project_urls_global)

if __name__ == "__main__":
    start = time.time()
    multiprocess()
    print("There are {} project urls".format(len(project_urls_global)))
    project_df = pd.DataFrame(project_list, columns=columns)
    print("There are {} projects in the df".format(len(project_df)))
    current_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%m_%d_%Y_%H_%M_%S')
    project_df.to_csv(f'idb_{current_time}.csv',index=False)
    end = time.time()
    print("Elapsed time: " + str(end - start))
