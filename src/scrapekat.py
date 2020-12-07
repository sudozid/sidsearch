from bs4 import BeautifulSoup
import re
import np
import pandas as pd
import cfscrape
import sys
import threading
import sqlite3
from multiprocessing.pool import ThreadPool

def main(searchstring,pageno):
    if(pageno==""):
        pageno="1"
    if(pageno.isnumeric()==False):
        sys.exit("Error")
    class DBStuff:
        cache = sqlite3.connect('db/cache.db')
        results = sqlite3.connect('db/results.db')
        cachecur = cache.cursor()
        cacheresults = pd.read_sql_query("SELECT * FROM cache_kat", cache)

    scraper = cfscrape.create_scraper()
    source = scraper.get("https://kickasstorrents.to/usearch/"+searchstring+"/"+pageno+"/").content
    soup = BeautifulSoup(source, 'lxml')
    print('Torrent List Page Scrape Sucessful')


    class Output:
        urllist = []
        seederlist = []
        leecherlist = []
        filenamelist = []
        categorylist = []
        sizelist = []
        newarr = []
        magnetlist = []
        datelist = []
        hashlist = []
        souparr = []
        splitlist = []


    def task(splitlist_column):
        print(threading.current_thread().name + ' Started')
        for url in splitlist_column:
            source = scraper.get(url).text
            soup = BeautifulSoup(source, "lxml")
            Output.urllist.append(
            url)  # URL List gets messed up due to race condition, so new urllist is created to avoid it
            Output.souparr.append(soup)
        print('{0} Finished'.format(threading.current_thread().name))


    def extract_induvidual(each_soup):
        soup = each_soup

        Output.filenamelist.append((soup.find('span', {'itemprop': 'name'}).text.strip()))
        magnet = soup.find('a', {'class': 'kaGiantButton'}).get('href')
        Output.magnetlist.append(magnet)
        # find seeder count by finding 'strong' child in div 'seedBlock' and extracting numbers using list comprehension
        # then removing inner list using * , same with leecher count
        Output.seederlist.append(
            *[int(sub.split('.')[0]) for sub in soup.find('div', {'class': 'seedBlock'}).findChildren('strong')[0]])
        Output.leecherlist.append(
            *[int(sub.split('.')[0]) for sub in soup.find('div', {'class': 'leechBlock'}).findChildren('strong')[0]])

        # date from timeago class seems to create empty space and newlines, it must be stripped out,similar thing happens with sizelist
        Output.datelist.append(soup.find('time', {'class': 'timeago'}).text.strip())
        Output.sizelist.append(soup.find('div', {'class': 'widgetSize'}).text.strip())
        Output.categorylist.append((soup.find('span', {'id': re.compile(r'^cat')}).text.partition('>')[0]).strip())
        matches = re.search(r"\burn:btih:([A-F\d]+)\b", magnet, re.IGNORECASE)
        if matches:
            Output.hashlist.append(matches.group(1))

    if(searchstring==""):
        sys.exit("Error")
    tablebody = (soup.find('table', {'class': 'data frontPageWidget'}))

    for tag in tablebody.find_all('a', {'class': 'cellMainLink'}):
        Output.urllist.append('https://kickasstorrents.to' + tag.get('href'))
    del tablebody
    splitarr = np.array_split(Output.urllist, 8)
    for x in [*splitarr]:  # convert arrray to list for cache lookup
        Output.splitlist.append(x.tolist())
    del splitarr
    splitlist = ([x for x in Output.splitlist if x])  # remove empty lists
    # threads for parralel scraping
    Output.urllist.clear()
    templist = []  # list for searching in arrays
    for sublist in splitlist:  # turn list in list to flat list for cache lookup
        for item in sublist:
            templist.append(item)
    if len(splitlist) > 0:  # don't search in cache if there is nothing to search
        for row in DBStuff.cacheresults.itertuples(index=True, name='Pandas'):
            if getattr(row, 'URL') in templist:
                print('Result ', getattr(row, 'URL'), ' retrieved from cache')
                Output.urllist.append(getattr(row, 'URL'))
                Output.filenamelist.append(getattr(row, 'File_Name'))
                Output.categorylist.append(getattr(row, 'Category'))
                Output.datelist.append(getattr(row, 'Date'))
                Output.seederlist.append(getattr(row, 'Seeders'))
                Output.leecherlist.append(getattr(row, 'Leechers'))
                Output.sizelist.append(getattr(row, 'Size'))
                Output.magnetlist.append(getattr(row, 'Magnet'))
                Output.hashlist.append(getattr(row, 'Hash'))
                splitlist = [[ele for ele in sub if ele != (getattr(row, 'URL'))] for sub in
                             splitlist]  # remove cached results from splitlist so it wont be scraped
                splitlist = ([x for x in splitlist if x])  # remove empty list from list
    del templist
    if len(splitlist) > 0:  # don't scrape if all results are in the cache
        with ThreadPool(len(splitlist)) as pool:
            result = pool.map(task, splitlist)
        for each_soup in Output.souparr:
            extract_induvidual(each_soup)
    del Output.souparr
    combined = (np.column_stack(
        [Output.urllist, Output.filenamelist, Output.categorylist, Output.datelist, Output.seederlist, Output.leecherlist,
         Output.sizelist, Output.magnetlist, Output.hashlist]))
    df = pd.DataFrame(combined)
    del combined
    df.columns = ['URL', 'File_Name', 'Category', 'Date', 'Seeders', 'Leechers', 'Size', 'Magnet', 'Hash']
    # df.to_sql(name='katcr_results', con=DBStuff.results, if_exists='replace', index=False)
    # dont add duplicates, probably a bad way to do this lol idk still faster than scraping individual pages
    for i in range(len(df)):
        try:
            df.iloc[i:i + 1].to_sql(name='cache_kat', con=DBStuff.cache, if_exists='append', index=False)
        except sqlite3.IntegrityError:
            pass
    df["URL"] = df["URL"].apply(  # insert links
        lambda x: "<a href='{}'>Link</a>".format(
            re.findall("^https://.*", x)[0], x
        )
    )
    df["Magnet"] = df["Magnet"].apply(  # insert links
        lambda x: "<a href='{}'>Magnet</a>".format(
            re.findall("^magnet.*", x)[0], x
        )
    )
    return df