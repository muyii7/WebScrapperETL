import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine  
import psycopg2
from datetime import datetime
import os

def scrape_data():
    # Use the request library to scrape data from the specified link
    web_data = requests.get('https://www.football-data.co.uk/englandm.php')

    #Create a BeautifulSoup object to clean & extract our target data
    soup = BeautifulSoup(web_data.content, 'html.parser')
    links = soup.find_all('a') # use the find_all() function of beautiful soup to extract all links from the html file
    # A list to aggregate the matched/desired csv links  
    csv_links = []
    for link in links:
        # A regular expression to match the ending strings of a link
        if re.search(r'mmz\d+\/\d+\/(E0|E1|E2)\.csv', str(link)):
            csv_link = re.search(r'mmz\d+\/\d+\/(E0|E1|E2)\.csv', str(link)).group()
            csv_link = 'https://www.football-data.co.uk/'+ csv_link # Append ending string to base link
            csv_links.append(csv_link)
        else:
            continue
    return csv_links