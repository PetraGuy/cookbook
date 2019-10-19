from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from collections import namedtuple
import re
import pickle
import os
from credentials import api_key

start_page_url = r'https://www.metoffice.gov.uk/research/climate/maps-and-data/historic-station-data'

def simple_get(url, params):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, **params)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    """
    It is always a good idea to log errors.
    This function just prints them, but you can
    make it do anything.
    """
    print(e)

# parameters to send to request
params = {'stream': True}
params_with_key = params.copy()
params_with_key['api_key'] = api_key

# main page
response = simple_get(start_page_url, params)
html = BeautifulSoup(response, 'html.parser')
station_name_tags = html.find_all('option')
station_name_urls = [tag['value'] for tag in station_name_tags if tag['value']]

# data pages
success = 0
all_station_data = []
for url in station_name_urls:
    response = get(url, params=params_with_key).content
    html = BeautifulSoup(response, 'html.parser')
    big_string = html.contents[0]

    # treat string as a file and move cursor to start
    data = StringIO(big_string)
    data.seek(0)

    # parse the header
    header = []
    for line in data:
        text = line.lstrip().replace('\r\n', '')
        if text[:4] == 'yyyy':  # first line of the column headers
            break
        header.append(text)
    skiprows = len(header) + 2

    # station name from url
    pattern = '([^/]+).txt$'
    result = re.search(pattern, url)
    station_name = result.group()

    # create dataframe
    data.seek(0)
    columns = ['yyyy', 'mm', 'tmax degC', 'tmin degC', 'af days', 'rain mm', 'sun hours']
    skipfooter = big_string.count('Provisional')  # these appear in an extra column at the end of the df that screws the parser
    df = pd.DataFrame()
    try:
        df = pd.read_table(data, skiprows=skiprows, header=None, delim_whitespace=True, skipfooter=skipfooter, engine="python")
        df.columns = columns
        success += 1
    except:
        print('failed----------------')
        print(url)

    # add to data structure
    station_data = namedtuple('station_data', 'station_name, url, header, df')
    station_data.station_name = station_name
    station_data.url = url
    station_data.header = header
    station_data.df = df

    all_station_data.append(station_data)

# stats
print('successes:', success)

# save to a file in this location
file_location = r'C:\dev\code\cookbook\cookbook\scrape'
file_name = r'metoffice_station_data.dat'
file = os.path.join(file_location, file_name)

# create file
filehandler = open(file, 'w')
pickle.dump(all_station_data, filehandler)
filehandler.close()

pass

