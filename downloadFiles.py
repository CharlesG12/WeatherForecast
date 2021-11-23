import requests
import os
from bs4 import BeautifulSoup

file_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/"


def get_file_links():

    # create response object
    r = requests.get(file_url)

    # create beautiful-soup object
    page = BeautifulSoup(r.content)

    # find all links on web-page
    start_link = page.findAll('a')

    # filter the link sending with .gz
    file_links = [file_url + link['href']
                  for link in start_link if link['href'].endswith('gz')]

    return file_links


def download_file_series(file_links):

    for link in file_links:
        module_path = os.path.dirname(os.path.realpath(__file__))
        output_path = os.path.join(module_path, 'data/weather')

        '''iterate through all links in video_links 
        and download them one by one'''

        # obtain filename by splitting url and getting
        # last string
        file_name = link.split('/')[-1]

        print("Downloading file:%s" % file_name)

        # create response object
        r = requests.get(link, stream=True)

        # download started
        with open(output_path+"/"+file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)

        print("%s downloaded!\n" % file_name)

    print("All videos downloaded!")
    return


file_links = get_file_links()
download_file_series(file_links)
