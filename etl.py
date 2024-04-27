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

#Read data from the csv links and merge into one data file
def extract_data():
    scrapped_links = scrape_data() # Create an object to recieve scrapped data ['https://www.football-data.co.uk/mmz4281/0203/E1.csv', https://www.football-data.co.uk/mmz4281/0203/E1.csv']
    datafiles = []
    data_columns = ['Div','Date','HomeTeam','AwayTeam','FTHG','FTAG'] #This is a list of the specific columns of data required.
    # Iterate through scrapped csv links, genetate dataframes and combine into a unified dataframe
    for link in scrapped_links:
        csv_data = pd.read_csv(link,usecols = data_columns,sep = ',', engine = 'python')
        datafiles.append(csv_data) 
    combined_data = pd.concat(datafiles, axis=0, ignore_index=True) # Merge all data from each csv file into a single dataframe
    # Write data to a csv file. This file serves as a staging layer before performing transformation
    combined_data.to_csv('raw/football_data.csv', header = ['div','date','home_team','away_team','fthg','ftag'], index = False)
extract_data()