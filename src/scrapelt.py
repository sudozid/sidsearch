from bs4 import BeautifulSoup
import re
from multiprocessing.pool import ThreadPool
import np
import pandas as pd
import sys
import cfscrape
import os
import sqlite3
import threading
def main(searchstring,pageno):

    if(searchstring==""):
        sys.exit("Error")
    if(pageno==""):
        pageno="1"
    if(pageno.isnumeric()==False):
        sys.exit("Error")
    class DBStuff:
        cache = sqlite3.connect('db/cache.db')
        results = sqlite3.connect('db/results.db')
        cachecur = cache.cursor()
        cacheresults = pd.read_sql_query("SELECT * FROM cache_lt", cache)
    scraper = cfscrape.create_scraper()
    '''searchstring= sys.argv[1]'''
    source=scraper.get("https://www.limetorrents.info/search/all/"+searchstring+"/"+pageno+"/").content
    soup = BeautifulSoup(source, 'lxml')
    print('Page Scrape Done')

    class Output:
        urllist=[]
        filtered=[]
        seederlist=[]
        leecherlist=[]
        filenamelist=[]
        sizelist=[]
        magnetlist=[]
        datelist=[]
        categorylist=[]
        hashlist=[]
        splitarr=[]
        souparr=[]
        splitlist=[]

    def task(splitlist_column):
        print(threading.current_thread().name + ' Started')
        for url in splitlist_column:
            source = scraper.get(url).text
            soup=BeautifulSoup(source,'lxml')
            Output.urllist.append(url)#URL List gets messed up due to race condition, so new urllist is created to avoid it
            Output.souparr.append(soup)
        print(threading.current_thread().name + ' Finished')

    def extract_induvidual(each_soup):
        soup=each_soup
        hrefs=[]
        rightside=[]
        templine=[]
        datecat=[]
        flag=False

        for h1_tag in soup.find('h1'):
            Output.filenamelist.append(h1_tag)

        Output.seederlist.append(*(re.findall(r'\d+',soup.find('span',{'class':'greenish'}).text)))
        Output.leecherlist.append(*(re.findall(r'\d+',soup.find('span',{'class':'reddish'}).text)))

        torrentinfo=soup.find('div',{'class':'torrentinfo'})

        dateandsize_temp=(torrentinfo.find('table').text).splitlines()

        for each in dateandsize_temp:
            if("Torrent Added" in each or "Torrent Size" in each):
                templine.append(each.split(" :"))

        rightside = [i[1] for i in templine]
        Output.sizelist.append(rightside[1])

        matches = re.finditer(r"(\d+ \w+\+?) by \w+ in (\w+)|([^\n]+ ago) in (\w+)|([^\n]+) by [^\n]+ in (\w+)|(\d+ \w+\+?) in (\w+)|(\w+ \w+) in (\w+)", rightside[0])

        for matchNum, match in enumerate(matches, start=1):
            datecat.append(list(filter(None, match.groups())))
        [[datecat]]=[datecat]
        Output.datelist.append(datecat[0])
        Output.categorylist.append(datecat[1])

        ###########MAGNET LINK EXTRACTION####################
        for anchor_tags in torrentinfo.find_all('a'):
            hrefs.append(anchor_tags.get('href')) #extract hrefs
        for magnet in hrefs:
            if(re.search("^magnet",magnet) and flag==False): #extract magnet links
                flag=True #don't run more than once otherwise duplicate magnets will show up
                Output.magnetlist.append(magnet)
                matches = re.search(r"\burn:btih:([A-F\d]+)\b", magnet, re.IGNORECASE)#HASH EXTRACTION#
                if matches:
                    Output.hashlist.append(matches.group(1))
        ###########MAGNET LINK EXTRACTION####################
    tablebody = soup.find('table',{'class':'table2'})

    for tag in tablebody.find_all('a'):
        Output.filtered.append(tag.get('href'))

    for url in Output.filtered:
        if(re.search("html$",url)):
            Output.urllist.append('https://www.limetorrents.info'+url)

    #splitting urllist for parrelscraping using threads
    splitarr=np.array_split(Output.urllist, 4)
    Output.urllist.clear()

    for x in [*splitarr]: #convert arrray to list for ease
        Output.splitlist.append(x.tolist())
    del splitarr
    splitlist= ([x for x in Output.splitlist if x]) #remove empty lists
    #threads for parralel scraping

    templist=[] #list for searching in cache
    for sublist in splitlist: #turn list in list to flat list for cache lookup
        for item in sublist:
            templist.append(item)
    if(len(splitlist)>0): #don't search in cache if there is nothing to search
        for row in DBStuff.cacheresults.itertuples(index = True, name ='Pandas'):
            if (getattr(row,'URL') in templist):
                print('Result ',getattr(row,'URL'),(' retrieved from cache'))
                Output.urllist.append(getattr(row,'URL'))
                Output.filenamelist.append(getattr(row,'File_Name'))
                Output.categorylist.append(getattr(row,'Category'))
                Output.datelist.append(getattr(row,'Date'))
                Output.seederlist.append(getattr(row,'Seeders'))
                Output.leecherlist.append(getattr(row,'Leechers'))
                Output.sizelist.append(getattr(row,'Size'))
                Output.magnetlist.append(getattr(row,'Magnet'))
                Output.hashlist.append(getattr(row,'Hash'))
                splitlist= [[ele for ele in sub if ele != (getattr(row,'URL'))] for sub in splitlist] #remove cached results from splitlist so it wont be scraped
                splitlist= ([x for x in splitlist if x])#remove empty list from list
    del templist

    #threads for parralel scraping
    if(len(splitlist)>0):
        with ThreadPool(len(splitlist)) as pool:
            result = pool.map(task, splitlist)
        for each_soup in Output.souparr:
            extract_induvidual(each_soup)
    del Output.souparr

    combined=(np.column_stack([Output.urllist,Output.filenamelist,Output.categorylist,Output.datelist,Output.seederlist,Output.leecherlist,Output.sizelist,Output.magnetlist,Output.hashlist]))
    df = pd.DataFrame(combined)
    del combined
    df.columns=['URL','File_Name','Category','Date','Seeders','Leechers','Size','Magnet','Hash']
    df.to_sql(name='lt_results', con=DBStuff.results,if_exists='replace',index=False)
    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql(name='cache_lt',con=DBStuff.cache,if_exists='append',index=False)
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