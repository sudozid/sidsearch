import re
import sqlite3
import threading
from multiprocessing.pool import ThreadPool
import sys
import cfscrape
import np
import pandas as pd
from bs4 import BeautifulSoup

def main(searchstring,pageno):

    if(searchstring==""):
        sys.exit("Error")
    searchstring.replace('"',"")
    searchstring.replace("'","")
    if(pageno==""):
        pageno="1"
    if(pageno.isnumeric()==False):
        sys.exit("Error")

    class DBStuff:
        cache = sqlite3.connect('db/cache.db')
        results = sqlite3.connect('db/results.db')
        cachecur = cache.cursor()
        cacheresults = pd.read_sql_query("SELECT * FROM cache_leetx", cache)
    scraper = cfscrape.create_scraper()

    source = scraper.get("https://1337x.to/search/"+searchstring+"/"+pageno+"/").content
    soup = BeautifulSoup(source, 'lxml')
    print('Page Scrape Done')
    tablebody = soup.find('tbody')


    class Output:
        urllist = []
        filenamelist = []
        seederlist = []
        leecherlist = []
        sizelist = []
        datelist = []
        hashlist = []
        categorylist = []
        splitarr = []
        magnetlist = []
        souparr = []
        splitlist = []


    # split scraping process into three threads
    def task(splitlist_column):
        print(threading.current_thread().name + ' Started')
        for url in splitlist_column:
            source = scraper.get(url).text
            soup = BeautifulSoup(source, 'lxml')
            Output.souparr.append(soup)
            Output.urllist.append(
                url)  # URL List gets messed up due to race condition, so new urllist is created to void it
        print(threading.current_thread().name + ' Finished')


    def extract_induvidual(each_soup):
        soup = each_soup
        leftside = []
        rightside = []

        Output.filenamelist.append(soup.find('h1').text)
        # find torrent details
        for ul_tag in soup.find_all('ul', {'class': 'list'}):
            for li_tag in ul_tag.find_all('li'):
                for strong_tag in li_tag.find_all('strong'):
                    leftside.append(strong_tag.text)
                for span_tag in li_tag.find_all('span'):
                    rightside.append(span_tag.text)
        combined = (np.column_stack([leftside, rightside]))
        del leftside, rightside
        for each_detail in combined:
            if 'Seeders' in each_detail[0]:
                Output.seederlist.append(each_detail[1])
            if 'Leechers' in each_detail[0]:
                Output.leecherlist.append(each_detail[1])
            if 'Total size' in each_detail[0]:
                Output.sizelist.append(each_detail[1])
            if 'Date uploaded' in each_detail[0]:
                Output.datelist.append(each_detail[1])
            if 'Category' in (each_detail[0]):
                Output.categorylist.append(each_detail[1])
        del each_detail, combined
        # get magnet link by looking for Magnet Download text and getting its parent anchor tag
        magnet = (soup.find(string="Magnet Download").find_parent('a').get('href'))
        Output.magnetlist.append(magnet)
        matches = re.search(r"\burn:btih:([A-F\d]+)\b", magnet, re.IGNORECASE)
        if matches:
            Output.hashlist.append(matches.group(1))


    # extract page links for every torrent and store it in an array
    for tag in tablebody.find_all('a'):
        temp_url = tag.get('href').split('/')
        if "torrent" in temp_url[1]:
            Output.urllist.append('https://1337x.to' + (tag.get('href')))

    # split array for parralel scraping
    splitarr = np.array_split(Output.urllist, 4)
    Output.urllist.clear()
    for x in [*splitarr]:  # convert arrray to list for ease
        Output.splitlist.append(x.tolist())
    del splitarr
    splitlist = ([x for x in Output.splitlist if x])  # remove empty lists
    # threads for parralel scraping
    templist = []  # list for searching in cache
    for sublist in splitlist:  # turn list in list to flat list for cache lookup
        for item in sublist:
            templist.append(item)

    if (len(splitlist) > 0):  # don't search in cache if there is nothing to search
        for row in DBStuff.cacheresults.itertuples(index=True, name='Pandas'):
            if (getattr(row, 'URL') in templist):
                print('Result ', getattr(row, 'URL'), (' retrieved from cache'))
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

    # threads for parralel requests
    if (len(splitlist) > 0):
        with ThreadPool(len(splitlist)) as pool:
            result = pool.map(task, splitlist)
        for each_soup in Output.souparr:
            extract_induvidual(each_soup)
    del Output.souparr
    combined = (np.column_stack(
        [Output.urllist, Output.filenamelist, Output.categorylist, Output.datelist, Output.seederlist, Output.leecherlist,
         Output.sizelist, Output.magnetlist, Output.hashlist]))
    df = pd.DataFrame(combined)
    df.columns = ['URL', 'File_Name', 'Category', 'Date', 'Seeders', 'Leechers', 'Size', 'Magnet', 'Hash']
    df.to_sql(name='leetx_results', con=DBStuff.results, if_exists='replace', index=False)
    for i in range(len(df)):
        try:
            df.iloc[i:i + 1].to_sql(name='cache_leetx', con=DBStuff.cache, if_exists='append', index=False)
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