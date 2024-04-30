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


#Transform all the combined extracted data to suit the purpose of future analytics
def transform_data():
    football_data = pd.read_csv('./football_data.csv')
    # Define a function to convert the date column to a uniform date format
    def convert_date(value):
        # RegEx to match the date string format - dd\mm\yyyy 03/02/2023
        if re.search(r'\d+\/\d+\/\d\d\d\d', str(value)):
            new_date = datetime.strptime(str(value), '%d/%m/%Y').date() # Convert date string to date type
            return new_date
        elif re.search(r'\d+\/\d+\/\d\d', str(value)): # RegEx to match the date string format - dd\mm\yy
            new_date = datetime.strptime(str(value), '%d/%m/%y').date() # Convert to dd\mm\yyyy format & date type
            return new_date
        else:
            pass
    football_data['date'] = football_data['date'].apply(convert_date) # Apply convert function to date column
    football_data.to_csv('transformed/transformed_data.csv') # Export cleaned data to an external file
    return football_data


def load_data_to_db():
    # Connection engine is created to the sport_data database using the sqlAlchemy create_engine() function.
    engine = create_engine('postgresql+psycopg2://{user}:{pw}@localhost/{db}')#format(user = 'postgres', pw = 'root', db = 'sport_data'))
    # get the cleanned data from the transform_data() function above
    football_data = transform_data()
    #load data to postgresql sport_data database using the pandas to_sql() function
    football_data.to_sql('football_data', con = engine, if_exists = 'append', index= False)
    print('Data is sucessfully loaded to database!')
load_data_to_db()