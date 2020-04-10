
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
from datetime import datetime, timedelta
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

filePath = 'C:\\Users\\Beau\\Documents\\DataScience\\Tennis\\Ouput Files\\'
#filePath = 'C:\\Users\\bbel1\\Documents\\SourceCode\\TennisStatistics\\TennisStatistics\\'
home = 'https://www.atpworldtour.com'
    


def intTryParse(value):
    """
    Parser function to test for integer values
    """

    try:
        return int(value)
    except ValueError:
        return value


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
    Retrieve the contents of the url
    """
    
    # Be a responisble scraper.
    # The multiplier is used to exponentially increase the delay when 
    # there are several attempts at connecting to the url
    randomSleep = random.uniform(2,5)  # Randomise the time to sleep to reduce predictability
    time.sleep(randomSleep*multiplier)

    try:
        # Get a response from the website
        resp = get(url)

        attempts = 0
        if is_good_response(resp):
                print ("Success: ",url)
                return resp.content

        while (resp.status_code != 200 and attempts < 5):
            if is_good_response(resp):
                print ('Success: ',url)
                return resp.content 
            else:
                attempts += 1
                print ('attempt: '+attempts)
                time.sleep(randomSleep*multiplier*attempts)
                resp = get(url)

        print ('Failed: '+ url)
        return None


    except RequestException as e:
        print('Error during requests to {0} : {1}'.format(url, str(e)))
        # Identify where the error occurd and skip the contents of this page.
   

def append_DF_To_CSV(df, csvFilePath, sep=","):
    """
    Append the dataframe to an existing file.
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


def get_Players(mens_ATP_Ranking,rankDate,startRank,endRank):
    """
    Get a list of players from the ranks.
    """

    rankPage = mens_ATP_Ranking+'?rankDate='+rankDate.replace('.','-')+'&countryCode=all&rankRange='+str(startRank)+'-'+str(endRank)
    content = get_html_content(rankPage)
    html = BeautifulSoup(content, 'html.parser')
    
    # Find all the players    
    players = html.findAll('td', {'class': 'player-cell'})

    return players


def player_physical_details(profile, playerProfile):
    """
    Extract the physical details of the player
    """

    for item in profile:
        key = item.contents[1].text.strip()
        value = item.contents[3].text.strip()
        
        # Replace the value when key is ...
        if (key == 'Age'):
            # Strip out the age and date of birth
            value = item.contents[3].text.strip().split()[0]
            playerProfile[key] = [value]
            key = 'D.O.B'
            value = item.contents[3].text.strip().split()[1]
            
        if (key == 'Weight'):
            # Strip out the S.I. units
            value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        if (key == 'Height'):
            # Strip out the S.I. units
            value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        # Add the values to the dictionary
        playerProfile[key] = [value]

    return playerProfile

def get_Player_Details(player, playerProfile, url, firstPass = False):
    """
    Update the player profile dictionary with the biographical information 
    and basic career stats for the current player.
    """
    url += '/player-activity'
    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')
    
    # Find all the items in the main banner.
    profile = html.findAll('div', {'class': 'wrap'})
    # Get the basic statistics
    statsTable = html.findAll('div', {'class': 'stat-value'})

    if (firstPass):
        playerProfile['Player'] = [player.text.strip()]
        
        playerProfile = player_physical_details(profile, playerProfile)
        #for item in profile:
        #    key = item.contents[1].text.strip()
        #    value = item.contents[3].text.strip()

        #    # Replace the value when key is ...
        #    if (key == 'Age'):
        #        # Strip out the age and date of birth
        #        value = item.contents[3].text.strip().split()[0]
        #        playerProfile[key] = [value]
        #        key = 'D.O.B'
        #        value = item.contents[3].text.strip().split()[1]

        #    if (key == 'Weight'):
        #        # Strip out the S.I. units
        #        value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        #    if (key == 'Height'):
        #        # Strip out the S.I. units
        #        value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        #    # Add the values to the dictionary
        #    playerProfile[key] = [value]

        
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

        playerProfile = player_physical_details(profile, playerProfile)

        #for item in profile:
        #    key = item.contents[1].text.strip()
        #    value = item.contents[3].text.strip()

        #    # Replace the value when key is ...
        #    if (key == 'Age'):
        #        # Strip out the age and date of birth.
        #        value = item.contents[3].text.strip().split()[0]
        #        playerProfile[key].append(value)
        #        key = 'D.O.B'
        #        value = item.contents[3].text.strip().split()[1]

        #    if (key == 'Weight'):
        #        # Strip out the S.I. units
        #        value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        #    if (key == 'Height'):
        #        # Strip out the S.I. units
        #        value = item.contents[3].text.strip().split('(')[-1].split(')')[0]

        #    # Add the values to the dictionary
        #    playerProfile[key].append(value)

        
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



def get_matchStats(urlExtension, playerIsLeft, tournamentDic, firstPass = False):
    """
    Get the match statistics for each player.
    """

    url = home+urlExtension
    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')

    if (not html.findAll('tr', {'class': 'match-stats-row percent-on'})):
        return tournamentDic
        # No match stats available

    duration = html.find('td', {'class': 'time'}).text.strip().split(':')
    if (len(duration) >= 3):
        # Convert time to minutes
        matchTime = float(duration[0])*60+float(duration[1])+float(duration[2])/60 
    else:
        matchTime = '-'
        
    if 'Match Duration' in tournamentDic.keys():
        tournamentDic['Match Duration'].append(matchTime)
    else:
        tournamentDic['Match Duration'] = [matchTime]

    stats = html.findAll('tr', {'class': 'match-stats-row percent-on'})

    # Cycle through each statistic
    for stat in stats:
        key = stat.contents[5].text.strip()

        # Seperate out each player statistics
        leftPlayer = stat.contents[1].text.strip().split()
        rightPlayer = stat.contents[9].text.strip().split()

        if (firstPass):

            if (len(leftPlayer) > 1):
                # If the statistic has more than 1 number, seperate out 
                # the raw numbers from the percentages.
                leftIn = int(leftPlayer[1].split('/')[0][1:])
                leftAttempt = int(leftPlayer[1].split('/')[1][:-1])

                rightIn = int(rightPlayer[1].split('/')[0][1:])
                rightAttempt = int(rightPlayer[1].split('/')[1][:-1])

                # Determine which statistic corresponds to the player and the opponent.
                if (playerIsLeft):
                    tournamentDic['Player '+key+' success'] = [leftIn]
                    tournamentDic['Opponent '+key+' success'] = [rightIn]

                    tournamentDic['Player '+key+' attempts'] = [leftAttempt]
                    tournamentDic['Opponent '+key+' attempts'] = [rightAttempt]
                else:
                    tournamentDic['Player '+key+' success'] = [rightIn]
                    tournamentDic['Opponent '+key+' success'] = [leftIn]

                    tournamentDic['Player '+key+' attempts'] = [rightAttempt]
                    tournamentDic['Opponent '+key+' attempts'] = [leftAttempt]

            else:

                # Determine which statistic corresponds to the player and the opponent.
                if (playerIsLeft):
                    tournamentDic['Player '+key] = [int(leftPlayer[0])]
                    tournamentDic['Opponent '+key] = [int(rightPlayer[0])]
                else:
                    tournamentDic['Player '+key] = [int(rightPlayer[0])]
                    tournamentDic['Opponent '+key] = [int(leftPlayer[0])]

        else:
            if (len(leftPlayer) > 1):
                # If the statistic has more than 1 number, seperate out 
                # the raw numbers from the percentages.
                leftIn = int(leftPlayer[1].split('/')[0][1:])
                leftAttempt = int(leftPlayer[1].split('/')[1][:-1])

                rightIn = int(rightPlayer[1].split('/')[0][1:])
                rightAttempt = int(rightPlayer[1].split('/')[1][:-1])
                
                # Determine which statistic corresponds to the player and the opponent.                
                if (playerIsLeft):
                    tournamentDic['Player '+key+' success'].append(leftIn)
                    tournamentDic['Opponent '+key+' success'].append(rightIn)

                    tournamentDic['Player '+key+' attempts'].append(leftAttempt)
                    tournamentDic['Opponent '+key+' attempts'].append(rightAttempt)
                else:
                    tournamentDic['Player '+key+' success'].append(rightIn)
                    tournamentDic['Opponent '+key+' success'].append(leftIn)

                    tournamentDic['Player '+key+' attempts'].append(rightAttempt)
                    tournamentDic['Opponent '+key+' attempts'].append(leftAttempt)

            else:

                # Determine which statistic corresponds to the player and the opponent.
                if (playerIsLeft):
                    tournamentDic['Player '+key].append(int(leftPlayer[0]))
                    tournamentDic['Opponent '+key].append(int(rightPlayer[0]))
                else:
                    tournamentDic['Player '+key].append(int(rightPlayer[0]))
                    tournamentDic['Opponent '+key].append(int(leftPlayer[0]))


    return tournamentDic

def tournementSeries(argument):
    """
    Map the series labels to actual Tournament Series titles.
    """

    switcher = {
        'grandslam': 'Grand Slam',
        '1000s': 'ATP Masters 1000',
        '500': 'ATP World Tour 500',
        '250': 'ATP World Tour 250',
        'itf': 'ITF Tournament',
        'atp': 'Nitto ATP Finals',
        'atpwt': 'ATP World Tour', # Pre 2009
        'challenger': 'ATP Challenger',
    }

    return switcher.get(argument, "Unknown")


def write_Player_Activity(player, url):
    """
    Get all the results from each tournament.

    This function writes the player activity to a xlsx file.
    """

    # Add the player activity extension
    url += '/player-activity?year=all&matchType=Singles'

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')
    
    # Tournaments
    tournaments = html.findAll('div', {'class': 'activity-tournament-table'})
    
    # Set up the dictionaries
    tournamentDic = {}

    # Flag to identify when the first tournament has been read
    firstTournament = True
    
    # Cycle through each tournament played.
    for tournament in tournaments:
                
        # Strip out the tournament details
        tournamentTitle = tournament.find('td', {'class': 'title-content'}).contents[1].text.strip()
        location = tournament.find('span', {'class': 'tourney-location'}).text.strip().split(',')[0]
        tournamentDate =  tournament.find('span', {'class': 'tourney-dates'}).text.strip().split('-')[0].strip()
        caption = tournament.find('div', {'class': 'activity-tournament-caption'})
        tournamentDetails =  tournament.findAll('span', {'class': 'item-value'})
        singlesDraw = int(tournamentDetails[0].text.strip())
        doublesDraw = int(tournamentDetails[1].text.strip())
        surface = tournamentDetails[2].text.strip()
        if (len(tournament.find('td', {'class': 'tourney-badge-wrapper'})) > 1):
            Series = tournament.find('td', {'class': 'tourney-badge-wrapper'}).contents[1].attrs['src'].split('_')[1]
        else:
            Series = 'Qualifying Series'

        bestOf = 3
        if (Series == 'grandslam'):
            bestOf = 5

        pointsEarned = caption.text.split()[3].replace(',','')
        rank = caption.text.split()[6].replace(',','')
        # Strip the Tie flag from the rank
        if ('T' in rank):
            rank = rank[:-1]
        rank = int(rank)

        prizeMoneyEarned = caption.text.split()[9]

        if (len(tournamentDetails) > 4):
            prizeMoney = tournamentDetails[3].text.strip()
            financialCommitment = tournamentDetails[4].text.strip()
        else:
            # For the national teams tournaments, that dont provide points or prize money, 
            # Eg. olympics and Davis Cup
            prizeMoney = ''
            financialCommitment = ''
        
        # Get the results table for each tournament
        table = tournament.find('table', {'class': 'mega-table'})
        results = table.findAll('th')

        headings = []
        for result in results:
            headings.append(result.text.strip())
        # Replace the W-L heading with result
        headings[-2] = 'Result'
        
        # Find all the rounds played by the current player
        rows = table.findAll('tr')

        values = []
        firstRow = True

        if (not rows):
            continue
            # No results found

        # Go through each round and extract the opponents details and the scores.
        for row in rows:

            completed = 'Completed'
            playerWins = True
            matchStats = False
            # Check for a url to the match stats page exists.
            if (row.contents[9].find('a', href=True) is not None):
                matchStatsUrl = row.contents[9].find('a', href=True)['href']
                matchStats = True

            # Append the round results 
            for index in range(1,len(row.contents),2):
                try:
                    values.append(row.contents[index].text.split())
                except AttributeError:
                    continue
                 

            # Only need to use the last 5 values 
            roundResult = values[-5:]
            
            if ('(INV)' in roundResult[4] or    # Ref: Grigor Dimitrov [Knoxville 2009] ?= invitation.
                '(ABN)' in roundResult[4] or    # Ref: Ryan Harrison [Col vs USA WG playoff 2010] ?= Abandoned (presumably because the result is already conclusive)
                '(UNP)' in roundResult[4] or    # Ref: Feliciano Lopez [USA vs ESP WG QF 2011] ?
                '(WEA)' in roundResult[4] or    # Ref: Dusan Lajovic [Tukey F2 2011] ?
                'Bye' in roundResult[2][0]):
                break
                # If the round is ruled invalid, ignore the whole round (No results are published)

            if (firstRow):
                if (firstTournament):
                    # First row of the first tournament
                    tournamentDic['Tournament'] = [tournamentTitle]
                    tournamentDic['Location'] = [location]
                    tournamentDic['Series'] = [tournementSeries(Series)]
                    tournamentDic['Best of #'] = [bestOf]
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
                    tournamentDic[headings[2]] = [' '.join(roundResult[2])]
                    if (len(roundResult[2]) > 0 and 'Bye' in roundResult[2][0]):
                        # Player has a bye for this round
                        tournamentDic[headings[1]] = ['']         # Opponent rank
                        tournamentDic[headings[3]] = ['']         # Result (W or L)
                    else:
                        tournamentDic[headings[1]] = [intTryParse(roundResult[1][0])]   # Opponent rank
                        tournamentDic[headings[3]] = [roundResult[3][0]]                # Result (W or L)

                        if (roundResult[3][0] == 'L'):
                            playerWins = False

                    # Get the results for each set
                    for index in range(5):
                        pKey = 'Player Set '+str(index+1)
                        oKey = 'Opponent Set '+str(index+1)
                                                
                        # Deal with unexpected values in the results.
                        if ('(RET)' in roundResult[4]):
                            completed = 'Retired'
                            roundResult[4].remove('(RET)')

                        if ('(W/O)' in roundResult[4]):
                            completed = 'Walkover'
                            roundResult[4].remove('(W/O)')

                        if ('(DEF)' in roundResult[4]):
                            completed = 'Incomplete'
                            roundResult[4].remove('(DEF)')
                            
                        # Seperate the player score from the opponent score
                        playerScore = '-'
                        opponentScore = '-'

                        if (index < len(roundResult[4])):
                            score = roundResult[4][index]

                            if (len(roundResult[4][index]) <= 3 ):
                                if (len(score) >= 2):
                                    playerScore = int(score[0])
                                    opponentScore = int(score[1])
                                    # This ignores the tie break result
                            else: # > 3
                                if ('-' in score):
                                    playerScore = int(score.split('-')[0])
                                    opponentScore = int(score.split('-')[1])
                                elif (score.isnumeric()):
                                    playerScore = int(score[0])
                                    opponentScore = int(score[1])
                                    # This ignores the tie break result 
                                else:
                                    playerScore = -9
                                    opponentScore = -9

                        tournamentDic[pKey] = [playerScore]
                        tournamentDic[oKey] = [opponentScore]
                    
                    tournamentDic['Match Completed'] = [completed]
                    # Get match statistics
                    if (matchStats):
                        get_matchStats(matchStatsUrl, playerWins, tournamentDic, firstRow & firstTournament)
                    
                        # Check the keys are all the same length
                        size = len(tournamentDic['Tournament'])
                        for key in tournamentDic.keys():
                            if (len(tournamentDic[key]) < size):
                                tournamentDic[key].append('-')

    
                    else:
                        # Update the match statistics keys.
                        size = len(tournamentDic['Tournament'])
                        for key in tournamentDic.keys():
                            if (len(tournamentDic[key]) < size):
                                tournamentDic[key].append('-')


                    
                    firstTournament = False
                else:
                    # Append additional tournament details to the list.
                    tournamentDic['Tournament'].append(tournamentTitle)
                    tournamentDic['Location'].append(location)
                    tournamentDic['Series'].append(tournementSeries(Series))
                    tournamentDic['Best of #'].append(bestOf)
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
                    if (len(roundResult[2]) > 0 and 'Bye' in roundResult[2][0]):
                        # Player has a bye for this round
                        tournamentDic[headings[1]].append('')         # Opponent rank
                        tournamentDic[headings[3]].append('')         # Result (W or L)
                    else:
                        tournamentDic[headings[1]].append(intTryParse(roundResult[1][0]))   # Opponent rank
                        tournamentDic[headings[3]].append(roundResult[3][0])                # Result (W or L)
                        
                        if (roundResult[3][0] == 'L'):
                            playerWins = False

                    
                    # Get the results for each set
                    for index in range(5):
                        pKey = 'Player Set '+str(index+1)
                        oKey = 'Opponent Set '+str(index+1)
                                                
                        # Deal with unexpected values in the results.
                        if ('(RET)' in roundResult[4]):
                            completed = 'Retired'
                            roundResult[4].remove('(RET)')

                        if ('(W/O)' in roundResult[4]):
                            completed = 'Walkover'
                            roundResult[4].remove('(W/O)')

                        if ('(DEF)' in roundResult[4]):
                            completed = 'Incomplete'
                            roundResult[4].remove('(DEF)')

                        # Seperate the player score from the opponent score
                        playerScore = '-'
                        opponentScore = '-'

                        if (index < len(roundResult[4])):
                            score = roundResult[4][index]

                            if (len(roundResult[4][index]) <= 3 ):
                                if (len(score) >= 2):
                                    playerScore = int(score[0])
                                    opponentScore = int(score[1])
                                    # This ignores the tie break result
                            else: # > 3
                                if ('-' in score):
                                    playerScore = int(score.split('-')[0])
                                    opponentScore = int(score.split('-')[1])
                                elif (score.isnumeric()):
                                    playerScore = int(score[0])
                                    opponentScore = int(score[1])
                                    # This ignores the tie break result 
                                else:
                                    playerScore = -9
                                    opponentScore = -9
                      

                        tournamentDic[pKey].append(playerScore)
                        tournamentDic[oKey].append(opponentScore)
 
                    tournamentDic['Match Completed'].append(completed)

                    # Get match statistics
                    if (matchStats):
                        get_matchStats(matchStatsUrl, playerWins, tournamentDic, firstRow & firstTournament)
                        
                        # Check the keys are all the same length
                        size = len(tournamentDic['Tournament'])
                        for key in tournamentDic.keys():
                            if (len(tournamentDic[key]) < size):
                                tournamentDic[key].append('-')

                    else:
                        # Update the match statistics keys.
                        size = len(tournamentDic['Tournament'])
                        for key in tournamentDic.keys():
                            if (len(tournamentDic[key]) < size):
                                tournamentDic[key].append('-')


            else:
                # Remaining round details
                tournamentDic['Tournament'].append(tournamentTitle)
                tournamentDic['Location'].append(location)
                tournamentDic['Series'].append(tournementSeries(Series))
                tournamentDic['Best of #'].append(bestOf)
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
                if (len(roundResult[2]) > 0 and 'Bye' in roundResult[2][0]):
                    # Player has a bye for this round
                    tournamentDic[headings[1]].append('')         # Opponent rank
                    tournamentDic[headings[3]].append('')         # Result (W or L)
                else:
                    tournamentDic[headings[1]].append(intTryParse(roundResult[1][0]))    # Opponent rank
                    tournamentDic[headings[3]].append(roundResult[3][0])         # Result (W or L)

                    if (roundResult[3][0] == 'L'):
                        playerWins = False

                # Get the results for each set
                for index in range(5):
                    pKey = 'Player Set '+str(index+1)
                    oKey = 'Opponent Set '+str(index+1)
                                                
                    # Deal with unexpected values in the results.
                    if ('(RET)' in roundResult[4]):
                        completed = 'Retired'
                        roundResult[4].remove('(RET)')

                    if ('(W/O)' in roundResult[4]):
                        completed = 'Walkover'
                        roundResult[4].remove('(W/O)')

                    if ('(DEF)' in roundResult[4]):
                        completed = 'Incomplete'
                        roundResult[4].remove('(DEF)')

                    # Seperate the player score from the opponent score
                    playerScore = '-'
                    opponentScore = '-'

                    if (index < len(roundResult[4])):
                        score = roundResult[4][index]

                        if (len(roundResult[4][index]) <= 3 ):
                            if (len(score) >= 2):
                                playerScore = int(score[0])
                                opponentScore = int(score[1])
                                # This ignores the tie break result
                        else: # > 3
                            if ('-' in score):
                                playerScore = int(score.split('-')[0])
                                opponentScore = int(score.split('-')[1])
                            elif (score.isnumeric()):
                                playerScore = int(score[0])
                                opponentScore = int(score[1])
                                # This ignores the tie break result 
                            else:
                                playerScore = -9
                                opponentScore = -9

                    tournamentDic[pKey].append(playerScore)
                    tournamentDic[oKey].append(opponentScore)

                tournamentDic['Match Completed'].append(completed)

                # Get match statistics
                if (matchStats):
                    get_matchStats(matchStatsUrl, playerWins, tournamentDic)
                    
                    # Check the keys are all the same length
                    size = len(tournamentDic['Tournament'])
                    for key in tournamentDic.keys():
                        if (len(tournamentDic[key]) < size):
                            tournamentDic[key].append('-')

                else:
                    # Update the match statistics keys.
                    size = len(tournamentDic['Tournament'])
                    for key in tournamentDic.keys():
                        if (len(tournamentDic[key]) < size):
                            tournamentDic[key].append('-')

            firstRow = False

    # Convert the dictionary to a pandas dataframe
    profileDateFrame = pd.DataFrame.from_dict(tournamentDic, orient='columns')

    # Write the dataframe to a csv file.
    playerName = player.text.strip().replace(' ','_')
    filename = filePath+'\Players Activity\\'+playerName+'.xlsx'
                
    sheet = 'Activity'
    profileDateFrame.to_excel(filename, sheet_name=sheet, index=False)
    
def get_Win_Loss_Stats(player, playerProfile, url, firstPass=False):
    """
    Get the year to date and career match record stats. This includes 
    the wins and losses for various statistics.
    """

    # Add the win loss extension
    url+= '/fedex-atp-win-loss'

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')

    # Find the match record table
    matchRecord = html.find('div', {'id': 'matchRecordTableContainer'})
        
    # Find all the elements of the table
    elements = matchRecord.findAll('td')
    # Each row has 12 elements
    tableWidth = 12
    recordElements = len(elements)//tableWidth

    # Cycle through the elements in the table
    for idx in range(recordElements):
   
        key = elements[idx*tableWidth].text.strip()
        titles = elements[idx*tableWidth+11].text.split()

        if (firstPass): 
            # Create the first item in the list
            playerProfile['YTD '+key+' Wins'] = [elements[idx*tableWidth+2].text]
            playerProfile['YTD '+key+' Loss'] = [elements[idx*tableWidth+4].text]
            playerProfile['Career '+key+' Wins'] = [elements[idx*tableWidth+7].text]
            playerProfile['Career '+key+' Loss'] = [elements[idx*tableWidth+9].text]
            if (not (not titles)):
                # if titles not empty
                playerProfile[key+' Titles'] = [elements[idx*tableWidth+11].text.split()[0]]


        else:
            playerProfile['YTD '+key+' Wins'].append(elements[idx*tableWidth+2].text)
            playerProfile['YTD '+key+' Loss'].append(elements[idx*tableWidth+4].text)
            playerProfile['Career '+key+' Wins'].append(elements[idx*tableWidth+7].text)
            playerProfile['Career '+key+' Loss'].append(elements[idx*tableWidth+9].text)
            if (not (not titles)):
                playerProfile[key+' Titles'].append(elements[idx*tableWidth+11].text.split()[0])

    # Return the updated dictionary
    return playerProfile

def get_Player_Stats(url,playerProfile, firstPass=False):
    """
    Get the player career service and return record statistics.
    """
    # Add the player statistics extension
    url+= '/player-stats'

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')

    # Find all the records in the table
    matchRecord = html.find('div', {'id': 'playerMatchFactsContainer'}).findAll('tr')
        
    # Cylce throught each record
    for record in matchRecord:

        if (len(record.contents) > 3):
            if (firstPass):
                playerProfile[record.contents[1].text.strip()] = [record.contents[3].text.strip()]

            else:
                playerProfile[record.contents[1].text.strip()].append(record.contents[3].text.strip())

    # Return the updated dictionary
    return playerProfile




def get_Ranking_History(player, url, firstDate, rankHistory, firstYear=2000, firstPass=False):
    """
    Get the historical rankings form 2000 onwards.
    """

    # Add the ranking history extension
    url+= '/rankings-history'

    content = get_html_content(url)
    html = BeautifulSoup(content, 'html.parser')

    # Find the table of rankings 
    rankings = html.find('table', {'class': 'mega-table'}).findAll('tr')
   
    # Add the player name
    if (firstPass):
        rankHistory['Player'] = [player.text.strip()]
    else:
        rankHistory['Player'].append(player.text.strip())

    # Convert the date string into a date time object
    date = datetime.strptime(firstDate,'%Y.%m.%d')
    previous = date + timedelta(days=7)

    # Relevant data starts at index 1
    index = 1

    # Define the stopping point of interest
    while (date.year >= firstYear):
        dateCheck = previous - timedelta(days=7)

        if (index < len(rankings)):
            date = datetime.strptime(rankings[index].contents[1].text.strip(),'%Y.%m.%d')
            
            # Check the date is the next expected date
            if (dateCheck != date):
                date = dateCheck
                value = ''  # previous
                index -= 1
            else:
                value = rankings[index].contents[3].text.strip()
                if ('T' in value):
                    value = value[:-1]

            previousRank = value
            previous = date

        else:
            date -= timedelta(days=7)
            value = ''

        # Update the dictionary
        if (firstPass):
            rankHistory[date.strftime('%Y-%m-%d')] = [value]
        else:
            rankHistory[date.strftime('%Y-%m-%d')].append(value)

        index += 1

    # Return the updated dictionary
    return rankHistory



if __name__ == '__main__':
    """
    Extract the history of each player.
    """
  
    # Set the base url to start searching for each player profile.
    mens_ATP_Ranking = home+'/en/rankings/singles'
    
    content = get_html_content(mens_ATP_Ranking)
    html = BeautifulSoup(content, 'html.parser')    
    rankDate = html.find('div', {'class': 'dropdown-label'}).text.strip()
    
    # Set up the profile dictionary
    playerProfile = {}
    rankHistory = {}
    firstPass = True

    # Set the ranking boundaries of interest
    startRank = 1
    endRank = 500

    # Get a list of all players in the rank range
    players = get_Players(mens_ATP_Ranking,rankDate,startRank,endRank)

    # Loop through each player profiles
    for player in players:
        urlExtension = player.contents[1].attrs['href']
            
        # Remove the last element from the url string
        extension = urlExtension.split('/')
        del extension[-1]
               
        # Add the extension to the url
        urlExtension = '/'.join(extension)
        url = home+urlExtension

        # Get the details from the player activity tab
        write_Player_Activity(player, url)

        # Get the information from the remaining tabs
        if (firstPass):
            playerProfile = get_Player_Details(player, playerProfile, url, firstPass=firstPass)                
            playerProfile = get_Win_Loss_Stats(player, playerProfile, url,firstPass=firstPass)
            playerProfile = get_Player_Stats(url, playerProfile,firstPass=firstPass)

            rankHistory = get_Ranking_History(player, url, rankDate, rankHistory,firstPass=firstPass)

            firstPass = False
        else:
            playerProfile = get_Player_Details(player, playerProfile, url)
            playerProfile = get_Win_Loss_Stats(player, playerProfile, url)
            playerProfile = get_Player_Stats(url, playerProfile)

            rankHistory = get_Ranking_History(player, url, rankDate, rankHistory)
                
        # Check the dictionary keys have the same size, 
        # this adds a value where there is missing data
        size = len(playerProfile['Player'])
        for key, _ in playerProfile.items():
            if (len(playerProfile[key]) < size):
                playerProfile[key].append('-')

    # Convert dictionaries to data frames
    playerStats = pd.DataFrame.from_dict(playerProfile, orient='columns')
    rank = pd.DataFrame.from_dict(rankHistory, orient='columns')
    
    # Create the statistics file
    playerFile = filePath+'Player_Profile.xlsx'
    # Write the player statistics and ranking history to the file.
    writer = pd.ExcelWriter(playerFile)
    playerStats.to_excel(writer,sheet_name='Player Statistics', index=False)
    rank.to_excel(writer,sheet_name='Rank History', index=False)
    writer.save()

    