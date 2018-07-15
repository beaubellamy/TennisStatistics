
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
#import unidecode
import numpy as np
#import math
#from fractions import Fraction
#import re
#from datetime import datetime
import time
import random
#import itertools
#import collections.abc
#from lxml.html import fromstring
#from itertools import cycle
#import traceback
#import requests.exceptions
import os
import warnings


def is_good_response(resp):
    """
    Ensures that the response is a html object.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 and 
            content_type is not None 
            and content_type.find('html') > -1)

def get_html_content(url, multiplier=1):
    """
    Retrieve the contents of the url, using the proxy and user 
    agent to help avoid scraper detection.
    """
    #if not bool(proxy_pool):
    #    print('There are no proxies in the set list')
    #    return None

    # Be a responisble scraper.
    # The multiplier is used to exponentially increase the delay when 
    # there are several attempts at connecting to the url
    randomSleep = random.uniform(2,10)  # Randomise the time to sleep to reduce predictability
    time.sleep(randomSleep*multiplier)

    #Choose the next proxy in the list
    #proxy = next(proxy_pool)
    
    # Get the html from the url
    try:
        with closing(get(url)) as resp: #,
        #                 headers={'User-Agent': random.choice(agents).rstrip()},
        #                 proxies={"http": proxy, "https": proxy})) \
        #as resp:
            # Check the response status
            if is_good_response(resp):
                print ("Success: ",url)
                return resp.content
            else:
                # Unable to get the url response
                return None

    except RequestException as e:
        print('Error during requests to {0} : {1}'.format(url, str(e)))
        # Error messages recieved. I should be catching these and properly dealing with them. 
        # I'm not sure how to deal with them properly though.

def appendDFToCSV_void(df, csvFilePath, sep=","):
    """
    Append the dataframe to an existing file.
    
    This alllows batch processing of the dataframes and 
    reduces the impact of lost data when there is an error.
    """

    # Check if the file already exists
    if not os.path.isfile(csvFilePath):
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep)

    # Check if the dataframes match before adding to file
    elif len(df.columns) != len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns):
        df.to_csv(csvFilePath+str(datetime.utcnow().time()), mode='a', index=False, sep=sep)
        warnings.warn('Columns do not match!! Dataframe has ' + str(len(df.columns)) + ' columns. CSV file has ' + str(len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + ' columns.')
        
    elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
        df.to_csv(csvFilePath+str(datetime.utcnow().time()), mode='a', index=False, sep=sep)
        warnings.warn('Columns and column order of dataframe and csv file do not match!!')
    
    # Append the dataframe to the existing file
    else:
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False)

def getPlayersFromPage(mensATPranking,rankDate,startRank,endRank):
    """
    Extract all the player names from the rankings list
    """

    rankPage = mensATPranking+'?rankDate='+rankDate+'&countryCode=all&rankRange='+str(startRank)+'-'+str(endRank)
    content = get_html_content(mensATPranking)
    html = BeautifulSoup(content, 'html.parser')
    # Find all the players
    
    players = html.findAll('td', {'class', 'player-cell'})

    return players


if __name__ == '__main__':
    """
    Extract statistics for each player
    """

    # Create the list of proxies.
    #proxies = get_proxies()
    #proxy_pool = cycle(proxies)

    # Create the list of user agents
    #agents = get_user_agents()

    # Get a list of each players
    #playerFile = 'C:\\Users\Beau\Documents\DataScience\Tennis\Tennis Players.xlsx'
    #players = pd.read_excel(playerFile,header=0)
    #playerNames = players['Name']
    
    # Set the base url to start searching for each player profile.
    mensATPranking = 'https://www.atpworldtour.com/en/rankings/singles'
    home = 'https://www.atpworldtour.com'
    playerTabs = ['bio',
                  'player-activity',
                  'fedex-atp-win-loss',
                  'titles-and-finals',
                  'player-stats',
                  'rankings-history',
                  'rankings-breakdown']

    content = get_html_content(mensATPranking)
    html = BeautifulSoup(content, 'html.parser')    
    rankDate = html.find('div', {'class', 'dropdown-label'}).text.strip().replace('.','-')
    
    # look through each of the ranking pages
    for page in range(1):
        startRank = page * 100
        endRank = (page + 1) * 100

        # build the ranking page
        #rankPage = mensATPranking+'?rankDate='+rankDate+'&countryCode=all&rankRange='+str(startRank)+'-'+str(endRank)
        #content = get_html_content(mensATPranking)
        #html = BeautifulSoup(content, 'html.parser')
        ## Find all the players
        #players = html.findAll('td', {'class', 'player-cell'})
        players = getPlayersFromPage(mensATPranking,rankDate,startRank,endRank)

        # Loop through each player profiles
        for player in players:
            urlExtension = player.contents[1].attrs['href']

            # Go to home+urlExtension and get all the player stats.

            # Loop through the profile tabs
            extension = urlExtension.split('/')

            for tab in playerTabs:
                # go to the next tab
                extension[-1] = tab
                urlExtension = '/'.join(extension)

                content = get_html_content(home+urlExtension)
                html = BeautifulSoup(content, 'html.parser')

                # get the data for each tab
                # create functions for each tab
                # each function will need different configurations.



            




