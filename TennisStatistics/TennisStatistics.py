
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
#import unidecode
import numpy as np
import csv
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
import xlwt



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
    randomSleep = random.uniform(2,5)  # Randomise the time to sleep to reduce predictability
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

def append_DF_To_CSV(df, csvFilePath, sep=","):
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
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep)
        warnings.warn('Columns do not match!! Dataframe has ' + str(len(df.columns)) + ' columns. CSV file has ' + str(len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + ' columns.')
        
    elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep)
        warnings.warn('Columns and column order of dataframe and csv file do not match!!')
    
    # Append the dataframe to the existing file
    else:
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False)

def get_Players_Page(mensATPranking,rankDate,startRank,endRank):
    """
    Get the players home page
    """

    rankPage = mensATPranking+'?rankDate='+rankDate+'&countryCode=all&rankRange='+str(startRank)+'-'+str(endRank)
    content = get_html_content(mensATPranking)
    html = BeautifulSoup(content, 'html.parser')
    # Find all the players
    
    players = html.findAll('td', {'class', 'player-cell'})

    return players

def get_Player_Details(player, playerProfile, url, firstPass = False):
    """
    Update the player profile dictionary with the biographical informaiton for the current player.
    """

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')
    
    # Find all the items in the main banner.
    profile = html.findAll('div', {'class', 'wrap'})
    # Get the main statistics
    statsTable = html.findAll('div', {'class', 'stat-value'})

    if (firstPass):
        playerProfile['Player'] = [player.text.strip()]

        for item in profile:
            key = item.contents[1].text.strip()
            value = item.contents[3].text.strip()

            # Replace the value when key is ...
            if (key == 'Age'):
                value = item.contents[3].text.strip().split()[0]
                playerProfile[key] = [value]
                key = 'D.O.B'
                value = item.contents[3].text.strip().split()[1]

            if (key == 'Weight'):
                # strip out the S.I. units
                value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

            if (key == 'Height'):
                # strip out the S.I. units
                value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

            playerProfile[key] = [value]

        
        # Win - Loss
        winLoss = statsTable[1].parent.contents[1].text.split('-')
        wins = winLoss[0]
        playerProfile['Win'] = [wins]
        loss = winLoss[1]
        playerProfile['Loss'] = [loss]
    
        # Titles
        titles = statsTable[2].parent.contents[1].text
        playerProfile['Titles'] = [titles]

        # Prize money
        prizeMoney = statsTable[3].parent.contents[1].text.replace('$','')
        careerPrizeMoney =  prizeMoney.replace(',','')
        playerProfile['Prize Money'] = [careerPrizeMoney]
    else:
        # Update the player profile dictionary
        playerProfile['Player'].append(player.text.strip())

        for item in profile:
            key = item.contents[1].text.strip()
            value = item.contents[3].text.strip()

            # Replace the value when key is ...
            if (key == 'Age'):
                value = item.contents[3].text.strip().split()[0]
                playerProfile[key].append(value)
                key = 'D.O.B'
                value = item.contents[3].text.strip().split()[1]

            if (key == 'Weight'):
                # strip out the S.I. units
                value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

            if (key == 'Height'):
                # strip out the S.I. units
                value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

            playerProfile[key].append(value)

        
        # Win - Loss
        winLoss = statsTable[1].parent.contents[1].text.split('-')
        wins = winLoss[0]
        playerProfile['Win'].append(wins)
        loss = winLoss[1]
        playerProfile['Loss'].append(loss)
    
        # Titles
        titles = statsTable[2].parent.contents[1].text
        playerProfile['Titles'].append(titles)

        # Prize money
        prizeMoney = statsTable[3].parent.contents[1].text.replace('$','')
        careerPrizeMoney =  prizeMoney.replace(',','')
        playerProfile['Prize Money'].append(careerPrizeMoney)

    return playerProfile


def get_Player_Activity(player, url):
    """
    Get all the results from each tournament.

    This function writes the player activity to a xlsx file.
    """

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')
    
    # Tournaments
    tournaments = html.findAll('div', {'class', 'activity-tournament-table'})
    
    # Set up the dictionaries
    tournamentDic = {}

    # Flag to identify when the first tournament has been read
    firstTournament = True


    for tournament in tournaments:
        
        # Strip out the details for each tournament
        tournamentTitle = tournament.find('td', {'class', 'title-content'}).contents[1].text.strip()
        location = tournament.find('span', {'class', 'tourney-location'}).text.strip().split(',')[0]
        tournamentDate =  tournament.find('span', {'class', 'tourney-dates'}).text.strip().split('-')[0].strip()
        caption = tournament.find('div', {'class', 'activity-tournament-caption'})
        tournamentDetails =  tournament.findAll('span', {'class', 'item-value'})
        singlesDraw = tournamentDetails[0].text.strip()
        doublesDraw = tournamentDetails[1].text.strip()
        surface = tournamentDetails[2].text.strip()

        pointsEarned = caption.text.split()[3].replace(',','')
        rank = caption.text.split()[6].replace(',','')
        prizeMoneyEarned = caption.text.split()[9]

        if (len(tournamentDetails) > 4):
            prizeMoney = tournamentDetails[3].text.strip()
            financialCommitment = tournamentDetails[4].text.strip()
        else:
            # For the national teams tournaments, that dont provide points or prize money
            prizeMoney = ''
            financialCommitment = ''
        
        # Get the results table for each tournament
        table = tournament.find('table', {'class', 'mega-table'})
        results = table.findAll('th')

        headings = []
        for result in results:
            headings.append(result.text.strip())
        # Replace the W-L heading with result
        headings[-2] = 'Result'
        
        rows = table.findAll('tr')

        values = []
        firstRow = True

        # Go through each round and extract the opponents details and the scores.
        for row in rows:

            for index in range(1,len(row.contents),2):
                values.append(row.contents[index].text.split())
            
            # Only need to use the last 5 values 
            roundResult = values[-5:]
            
            if (firstRow):
                if (firstTournament):
                    # First row of the first tournament
                    tournamentDic['Tournament'] = [tournamentTitle]
                    tournamentDic['Location'] = [location]
                    tournamentDic['Date'] = [tournamentDate]
                    tournamentDic['Surface'] = [surface]
                    tournamentDic['# Singles Draw'] = [singlesDraw]
                    tournamentDic['# Doubles Draw'] = [doublesDraw]
                    tournamentDic['Prize Money'] = [prizeMoney]
                    tournamentDic['Financial Commitment'] = [financialCommitment]
                    tournamentDic['Points'] = [pointsEarned]
                    tournamentDic['Rank at Tournament'] = [rank]
                    tournamentDic['Prize Money Earned'] = [prizeMoneyEarned]

                    tournamentDic[headings[0]] = [' '.join(roundResult[0])]
                    tournamentDic[headings[1]] = roundResult[1]
                    tournamentDic[headings[2]] = [' '.join(roundResult[2])]
                    tournamentDic[headings[3]] = roundResult[3]

                    # Get results for each set
                    for index in range(5):
                        key = ' Set '+str(index+1)
                
                        if (index >= len(roundResult[4])):
                            score = '--'
                        else:
                            score = roundResult[4][index]

                        tournamentDic[key] = [score]                    

                    firstTournament = False
                else:
                    # When we get on to the next tournament, we dont want to 
                    # overwrite the last tournament results.
                    tournamentDic['Tournament'].append(tournamentTitle)
                    tournamentDic['Location'].append(location)
                    tournamentDic['Date'].append(tournamentDate)
                    tournamentDic['Surface'].append(surface)
                    tournamentDic['# Singles Draw'].append(singlesDraw)
                    tournamentDic['# Doubles Draw'].append(doublesDraw)
                    tournamentDic['Rank at Tournament'].append(rank)

                    if (firstRow):
                        tournamentDic['Prize Money'].append(prizeMoney)
                        tournamentDic['Financial Commitment'].append(financialCommitment)
                        tournamentDic['Points'].append(pointsEarned)
                        tournamentDic['Prize Money Earned'].append(prizeMoneyEarned)
                    else:
                        tournamentDic['Prize Money'].append('')
                        tournamentDic['Financial Commitment'].append('')
                        tournamentDic['Points'].append('')
                        tournamentDic['Prize Money Earned'].append('')

                    tournamentDic[headings[0]].append(' '.join(roundResult[0]))  # Round
                    tournamentDic[headings[2]].append(' '.join(roundResult[2]))  # Opponent
                    if ('Bye' in roundResult[2]):
                        # Player has a bye for this round
                        tournamentDic[headings[1]].append('')         # Opponent rank
                        tournamentDic[headings[3]].append('')         # Result (W or L)
                    else:
                        tournamentDic[headings[1]].append(roundResult[1][0])         # Opponent rank
                        tournamentDic[headings[3]].append(roundResult[3][0])         # Result (W or L)

                    # Get results for each set
                    for index in range(5):
                        key = ' Set '+str(index+1)
                
                        if (index >= len(roundResult[4])):
                            score = '--'
                        else:
                            score = roundResult[4][index]

                        tournamentDic[key].append(score)
                        
            else:
                # Remaining round details
                tournamentDic['Tournament'].append(tournamentTitle)
                tournamentDic['Location'].append(location)
                tournamentDic['Date'].append(tournamentDate)
                tournamentDic['Surface'].append(surface)
                tournamentDic['# Singles Draw'].append(singlesDraw)
                tournamentDic['# Doubles Draw'].append(doublesDraw)
                tournamentDic['Prize Money'].append('')
                tournamentDic['Financial Commitment'].append('')
                tournamentDic['Points'].append('')
                tournamentDic['Rank at Tournament'].append(rank)
                tournamentDic['Prize Money Earned'].append('')

                tournamentDic[headings[0]].append(' '.join(roundResult[0]))  # Round
                tournamentDic[headings[2]].append(' '.join(roundResult[2]))  # Opponent
                if ('Bye' in roundResult[2]):
                    # Player has a bye for this round
                    tournamentDic[headings[1]].append('')         # Opponent rank
                    tournamentDic[headings[3]].append('')         # Result (W or L)
                else:
                    tournamentDic[headings[1]].append(roundResult[1][0])         # Opponent rank
                    tournamentDic[headings[3]].append(roundResult[3][0])         # Result (W or L)

                # Get results for each set
                for index in range(5):
                    key = ' Set '+str(index+1)
                
                    if (index >= len(roundResult[4])):
                        score = '--'
                    else:
                        score = roundResult[4][index]

                    tournamentDic[key].append(score)

            firstRow = False

    # Convert the dictionary to a pandas dataframe
    profileDateFrame = pd.DataFrame.from_dict(tournamentDic, orient='columns')

    # Write the dataframe to a csv file.
    playerName = player.text.strip().replace(' ','_')
    filename = 'C:\\Users\Beau\Documents\DataScience\Tennis\Ouput Files\Players Activity\\'+playerName+'.xlsx'
    sheet = 'Activity'
    profileDateFrame.to_excel(filename, sheet_name=sheet, index=False)
    


def WinLossStats():
    return

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')
    
    # Find all the items in the main banner.
    profile = html.findAll('div', {'class', 'wrap'})

    details ={}
    details['Player'] = player.text.strip()

    for item in profile:
        key = item.contents[1].text.strip()
        value = item.contents[3].text.strip()

        # Replace the value when key is ...
        if (key == 'Age'):
            value = item.contents[3].text.strip().split()[0]
            details[key] = value
            key = 'D.O.B'
            value = item.contents[3].text.strip().split()[1]

        if (key == 'Weight'):
            # strip out the S.I. units
            value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        if (key == 'Height'):
            # strip out the S.I. units
            value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        details[key] = value

    # Get the main statistics
    statsTable = html.findAll('div', {'class', 'stat-value'})

    # Win - Loss
    winLoss = statsTable[1].parent.contents[1].text.split('-')
    wins = winLoss[0]
    details['Win'] = wins
    loss = winLoss[1]
    details['Loss'] = loss
    
    # Titles
    titles = statsTable[2].parent.contents[1].text
    details['Titles'] = titles

    # Prize money
    prizeMoney = statsTable[3].parent.contents[1].text.replace('$','')
    careerPrizeMoney =  prizeMoney.replace(',','')
    details['Prize Money'] = careerPrizeMoney

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
    Extract the history of each player.
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
    # Set the url tabs that we want to look at
    playerTabs = ['player-activity',
                  'fedex-atp-win-loss',
                  'titles-and-finals',
                  'player-stats',
                  'rankings-history',
                  'rankings-breakdown']

    content = get_html_content(mensATPranking)
    html = BeautifulSoup(content, 'html.parser')    
    rankDate = html.find('div', {'class', 'dropdown-label'}).text.strip().replace('.','-')
    
    # Set up the profile dictionary
    playerProfile = {}
    firstPass = True

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

        # Get a list of players on each page
        players = get_Players_Page(mensATPranking,rankDate,startRank,endRank)

        # Loop through each player profiles
        for player in players[:5]:
            urlExtension = player.contents[1].attrs['href']

            
            # Loop through the profile tabs
            extension = urlExtension.split('/')
                     
            for tab in playerTabs:
                
                # remove the first unwanted tab from the url
                del extension[-1]
               
                # Go to the next tab
                extension.append(tab)
                urlExtension = '/'.join(extension)
                print(home+urlExtension)
                url = home+urlExtension

                #content = get_html_content(home+urlExtension)
                #html = BeautifulSoup(content, 'html.parser')

                # Get the details from each tab
                if tab is 'player-activity':                      # remove this comment when ready to complete
                    get_Player_Activity(player, url) #+'?year=all')

                    #playerProfile.update(get_Career_Activity(home+urlExtension))
                    if (firstPass):
                        playerProfile = get_Player_Details(player, playerProfile, url, firstPass=firstPass)
                        firstPass = False
                    else:
                        playerProfile = get_Player_Details(player, playerProfile, url)

                    

                elif tab is 'fedex-atp-win-loss':
                    WinLossStats()
                    # Placehoder
                    # Need to append each value to a key list to create the correct dictionary

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

        playerStats = pd.DataFrame.from_dict(playerProfile, orient='columns')
        filename = 'C:\\Users\Beau\Documents\DataScience\Tennis\Ouput Files\Player_Stats.xlsx'
        playerStats.to_excel(filename, index=False)


    #     profileDateFrame = pd.DataFrame.from_dict(tournamentDic, orient='columns')

    ## Write the dataframe to a csv file.
    #playerName = player.text.strip().replace(' ','_')
    #filename = playerName+'.xlsx'
    #sheet = 'Activity'
    #profileDateFrame.to_excel(filename, sheet_name=sheet, index=False)

