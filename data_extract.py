import requests
import logging
import os
import datetime
import threading
import pandas as pd
import time
import json


'''
Initializing logger
log created in app.log file
'''
logging.basicConfig(filename='app.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)



class downloadThread(threading.Thread):
    '''
    Inheriting threading.Thread class and initializing it in constructor 
    '''
    def __init__(self, url, name, chunk_size):
        threading.Thread.__init__(self)
        self.url = url
        self.name = name
        self.chunk_size = chunk_size
        
    '''
    Overwriting run method of Thread class to download individual file in chunk
    chunk_size provided in parameter file
    '''
    def run(self):
        try:
            req = requests.get(self.url)
            fname = os.path.basename(self.url)
            with open(fname, 'wb') as f:
                for chunk in req.iter_content(self.chunk_size):
                    f.write(chunk)
        except Exception as e:
            logger.error("Error occured while requesting URL")
            logger.error(e)
            raise

class download:
    '''
    Wrapper class that calls threading module to download file concurrently
    '''
    def __init__(self,  col_names):
        self.URLS = []
        self.col_names = col_names
    def download_all_urls(self,chunk_size):
        threads = []
        for item, url in enumerate(self.URLS):
            name = "Thread:{}".format(item)
            thread = downloadThread(url, name, chunk_size)
            threads.append(thread)
            thread.start()
        for i in threads:
            i.join()

    ''' Get a list of URLS based on pattern'''
    def get_url_list(self,start_date,holiday,url_base, url_ext, url_mid):
        for i in range(1,30):
            today = start_date
            y = today - datetime.timedelta(days=i)
            year = y.strftime('%Y')
            month=y.strftime('%b').upper()
            day = y.strftime('%d')
            if y.weekday() < 5 and y.date() not in holiday:
                URL = url_base + str(year) + '/' + str(month) + '/' + url_mid + str(day) + str(month) + str(year) + url_ext
                self.URLS.append(URL)
        return self.URLS

    '''
    parsing all zip files into a dataframe
    extracting files with timestamp based on symbols
    '''
    def extract_files(self):
        try:
            d_frames = []
            for file in self.URLS:
                fname = os.path.basename(file)
                df_temp = pd.read_csv(fname, usecols=self.col_names)
                d_frames.append(df_temp)
            df = pd.concat(d_frames)
            path = os.getcwd() +'\\' + 'parsed_file.csv'
            #db fie
            df.to_csv(path, index=None)
            #individual files based on symbol
            
            for i in df['SYMBOL'].unique():
                path = os.getcwd() + '\\' + i + "_" + datetime.datetime.today().strftime("%m%d%Y") + '.csv'
                df[df.SYMBOL == i].to_csv(path, index=None)
        except Exception as e:
            logger.error("Error occured while extracting file")
            logger.error(e)
            raise



class dateUtil:
    def convert_str_to_datetime(self,s):
        d_list = []
        for i in s:
            d_list.append(datetime.datetime.strptime(i,"%d/%m/%Y").date())
        return d_list


def main():
    '''Extracting parameters from json file'''
    with open('parameter.json', "r") as f:
        parameter=json.load(f)
    dateObj=dateUtil()
    holiday_list = dateObj.convert_str_to_datetime(parameter["holiday_list"])
    column_names = parameter["column_names"]
    url_base = parameter["url_base"]
    url_ext = parameter["url_ext"]
    url_mid = parameter["url_mid"]
    chunk_size = parameter["chunk_size"]
    start_date = datetime.datetime.today()

    '''Download class object'''
    a = download(column_names)

    logger.info("Creating list of urls")
    URLS = a.get_url_list(start_date, holiday_list ,url_base, url_ext, url_mid)
    logger.info(URLS)

    
    logger.info("starting Download")
    a.download_all_urls(chunk_size)
    logger.info("Download Complete")

    logger.info("Extracting files based on symbols")
    a.extract_files()
    logger.info("Extract complete")


    
    


if __name__=="__main__":
    main()
