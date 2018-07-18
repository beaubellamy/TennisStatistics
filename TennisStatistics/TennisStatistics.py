
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


def getPlayerProfile(player, url):

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')

    statsTable = html.findAll('div', {'class', 'stat-value'})

    careerStats = {}

    # Career Single stats

    # Win Loss
    wins = statsTable[1].parent.contents[1].text.split('-')[0]
    careerStats['Win'] = wins
    loss = statsTable[1].parent.contents[1].text.split('-')[1]
    careerStats['Loss'] = loss
    
    # Titles
    titles = statsTable[2].parent.contents[1].text
    careerStats['Titles'] = titles

    # Prize money
    prizeMoney = statsTable[3].parent.contents[1].text.replace('$','')
    careerPrizeMoney =  prizeMoney.replace(',','')
    careerStats['Prize Money'] = careerPrizeMoney

    # Tournaments
    tournaments = html.findAll('div', {'class', 'activity-tournament-table'})
    tournamentDic = {}
    scores = {}
    tournamentResults = {}

    firstTournament = True

    for tournament in tournaments:
        
        tournamentTitle = tournament.find('td', {'class', 'title-content'}).contents[1].text
        location = tournament.find('span', {'class', 'tourney-location'}).text.strip().split(',')[0]
        tournamentDate =  tournament.find('span', {'class', 'tourney-dates'}).text.strip().split('-')[0].strip()
        tournamentDetails =  tournament.findAll('span', {'class', 'item-value'})
        singlesDraw = tournamentDetails[0].text.strip()
        doublesDraw = tournamentDetails[1].text.strip()
        surface = tournamentDetails[2].text.strip()
        prizeMoney = tournamentDetails[3].text.strip()
        financialCommitment = tournamentDetails[4].text.strip()
        
        if (firstTournament):
            tournamentDic['Tournament'] = [tournamentTitle]
            tournamentDic['Location'] = [location]
            tournamentDic['Date'] = [tournamentDate]
            tournamentDic['Surface'] = [surface]
            tournamentDic['# Singles Draw'] = [singlesDraw]
            tournamentDic['# Doubles Draw'] = [doublesDraw]
            tournamentDic['Prize Money'] = [prizeMoney]
            tournamentDic['Financial Commitment'] = [financialCommitment]
            firstTournament = False
        else:
            tournamentDic['Tournament'].append(tournamentTitle)
            tournamentDic['Location'].append(location)
            tournamentDic['Date'].append(tournamentDate)
            tournamentDic['Surface'].append(surface)
            tournamentDic['# Singles Draw'].append(singlesDraw)
            tournamentDic['# Doubles Draw'].append(doublesDraw)
            tournamentDic['Prize Money'].append(prizeMoney)
            tournamentDic['Financial Commitment'].append(financialCommitment)


        table = html.findAll('div', {'class', 'activity-tournament-table'})[0].find('table', {'class', 'mega-table'})
        results = table.findAll('th')

        headings = []
        for result in results:
            headings.append(result.text.strip())

        # Replace teh W-L heading with ...
        headings[3] = 'Result'

        rows = table.findAll('tr')
        # loop through values. Each value is a row in the table
        values = []
        firstRow = True

        
        for row in rows:
            for index in range(1,len(row.contents),2):
                values.append(row.contents[index].text.split())
                
            if (firstRow):
                scores[headings[0]] = [' '.join(values[0])]
                scores[headings[1]] = values[1]
                scores[headings[2]] = [' '.join(values[2])]
                scores[headings[3]] = values[3]

            else:
                scores[headings[0]].append(' '.join(values[0]))
                scores[headings[1]].append(values[1][0])
                scores[headings[2]].append(' '.join(values[2]))
                scores[headings[3]].append(values[3][0])

            # Scores, always display 5 set results
            for index in range(5):
                key = ' Set '+str(index+1)
                
                if (index >= len(values[4])):
                    value = '--'
                else:
                    value = values[4][index]

                if (firstRow):
                    scores[key] = [value]                    
                else:
                    scores[key].append(value)


            values = []
            firstRow = False
            # link the results to the tournament, currently the results get overwritten on each tournament.
            # how do you add a dictionary as an element to another dictionary.
            tournamentResults[tournamentTitle] = {scores}

        

        print (len(rows))
        print (len(values))
        print (len(headings))

#            careerStats[heading[0]]
            


def WinLossStats():
    pass

def titles():
    pass

def playerStats():
    pass

def rankingHistory():
    pass

def rankingBreakdown():
    pass


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
    playerTabs = ['player-activity',
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
                print(home+urlExtension)

                #content = get_html_content(home+urlExtension)
                #html = BeautifulSoup(content, 'html.parser')

                # get the data for each tab
                # create functions for each tab
                # each function will need different configurations.
                if tab is 'player-activity':
                    getPlayerProfile(player, home+urlExtension+'?year=all')
                    pass
                    # store this in its own file
                elif tab is 'fedex-atp-win-loss':
                    WinLossStats()
                    # Placehoder
                elif tab is 'titles-and-finals':
                    titles()
                    # Placehoder
                elif tab is 'player-stats':
                    playerStats()
                    # Placehoder
                elif tab is 'rankings-history':
                    rankingHistory()
                    # Placehoder
                elif tab is 'rankings-breakdown':
                    rankingBreakdown()
                    # Placehoder
                else:
                    print (tab+' profile for '+player.text.strip()+' is not supported')

            




