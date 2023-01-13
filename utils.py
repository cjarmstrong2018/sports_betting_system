"""
Contains all functions necessary for the Odds Portal Scrapers
"""
import pandas as pd
import numpy as np
import pytz
import time
from datetime import datetime, timezone
import re
import requests
from retrying import retry
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np
from utils import *
import sys
import json
import http.client
import re
import requests
from datetime import datetime, timezone
import time
from bs4 import BeautifulSoup
import pytz
import sqlite3
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from multiprocessing.pool import Pool
import multiprocessing as mp

TERRIBLE_TIMESTAMP_PAT = re.compile(r'^t(\d+)-')


def unhash(xhash):
    """
    Unhashes the hash provided by odds_portal

    Args:
        xhash (str): xhash found in oddsportal

    Returns:
        str: unhashed string
    """
    decoded = ''
    for i in xhash.split('%')[1:]:
        decoded += chr(int(i, 16))
    return decoded


def decode_terrible_timestamp(tag) -> datetime:
    """
    Decodes a timestamp from the odds_portal.com website
    Args:
        tag (BeautifulSoup): tag containing the timestamp
    Raises:
        ValueError: no valid timestamp

    Returns:
        datetime: timestamp in datetime format
    """
    for cls in tag.attrs['class']:
        match = TERRIBLE_TIMESTAMP_PAT.search(cls)
        if match:
            break
    else:
        raise ValueError(f'{tag} does not seem to contain a valid timestamp')

    stamp = int(match[1])
    return datetime.fromtimestamp(stamp, tz=timezone.utc)


def retry_if_connection_error(exception):
    """
    Raises an error if there is a connection error in safe_request
    """
    return isinstance(exception, requests.ConnectionError)


@retry(retry_on_exception=retry_if_connection_error, wait_fixed=2000)
def safe_request(url, **kwargs):
    """
    Get request with retry functionality. 
    Good for webscraping

    Args:
        url (str): url of get_request

    Returns:
        response: response object/ result of get_request
    """
    return requests.get(url, **kwargs)


def get_postmatch_data(gid, xhash):
    """
    Returns a JSON object that has information about the results of a game
    Args:
        gid (str): game id speficied by oddsportal.com
        xhash (str): a hash of the game id specified

    Returns:
        dict: JSON object with the results of the match
    """
    post_match_url = f"https://fb.oddsportal.com/feed/postmatchscore/3-{gid}-{xhash}.dat"
    current_time = int(round(time.time() * 1000))
    querystring = {"_": f"{current_time}"}

    payload = ""
    headers = {
        "authority": "fb.oddsportal.com",
        "referer": "https://www.oddsportal.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    }
    postmatch_response = safe_request(
        post_match_url, data=payload, headers=headers, params=querystring)
    postmatch_data = json.loads(re.findall(
        r"\.dat\',\s({.*})", postmatch_response.text)[0])['d']
    return postmatch_data


def get_winner(gid, xhash):
    """Finds the winner of a given game
    Args:
        gid (str): game id
        xhash (str):hash of the game id

    Returns:
        1 if the home team won, 0 if away won, np.nan if the game has not 
        finished
    """
    results = get_postmatch_data(gid, xhash)
    if not results['isFinished']:
        return np.nan
    home_score, away_score = [int(x.strip)
                              for x in results['result'].split("-")]
    return 1 if home_score > away_score else 0


def get_results(event_id, xhash):
    """
    Gets the results of a given match when provided an event_id and xhash
    Args:
        event_id (str):odds_portal event id
        xhash (str): odds_portal event xhash

    Returns:
        bool, int, int: bool: whether the game has finished, home_score, away_score
    """
    results = get_postmatch_data(event_id, xhash)
    if results.get("E", False):
        return False, np.nan, np.nan
    if not results.get('isFinished', False):
        return False, np.nan, np.nan
    score = re.findall("(?<=<strong>)(.*?)(?=</strong>)", results['result'])[0]
    home_score, away_score = [int(x.strip())
                              for x in score.split(" ")[0].split(":")]

    return True, home_score, away_score


def normalize_teams(series, league):
    """
    Given a series of team acronyms or listed names and normalize them to using 
    the text files in the directory
    Inputs:
        series (Series): of team names
        league (str): league abbreviation
    Returns:
        Series
    """
    if league == "NBA":
        teams = pd.read_json("nba_teams.json")
    elif league == "NFL":
        teams = pd.read_csv("nfl_teams.csv")
        teams = teams[['Abbreviation', "Name"]]
        teams.columns = ['abbreviation', 'teamName']
    elif league == "MLB":
        teams = pd.read_csv("mlb_teams.csv")
    elif league == "NHL":
        teams = pd.read_csv("NHL_teams.csv")
    teams = teams['teamName'].to_list()
    cleaned_series = series.apply(lambda x: process.extractOne(x, teams)[0])
    return cleaned_series


def format_odds(odds_data, event_id, xhash, xhashf, bookies):
    """Formats an odds_data json object into a DataFrame.
    This is a helper function for get_odds_data()

    Args:
        odds_data (JSON): parsed response from ajax request
        event_id (str): oddsportal event_id
        xhash (str): one of two xhashes found in oddsportal for odds request
        xhashf (str): other xhash found on oddsportal for results request
        bookies (DataFrame): result of get_bookies() function call

    Returns:
        _type_: _description_
    """
    odds_df = pd.DataFrame(odds_data, ).T
    odds_df.columns = ['away_odds', "home_odds"]
    odds_df.index.name = "idProvider"
    odds_df = odds_df.reset_index()
    odds_df = odds_df.merge(bookies, how="left")
    odds_df['event_id'] = event_id
    odds_df['xhash'] = xhash
    odds_df['xhashf'] = xhashf
    return odds_df


def get_odds_data(game_id, xhash, xhashf, bookies):
    """
    Gets odds for a specific game

    Args:
        game_id (str): oddsportal xeid for the game of interest
        xhash (str): oddsportal xhash for the game
        xhashf (str): oddsportal xhashf for the game of interest
        bookies (DataFrame): return from get_bookies()

    Returns:
        DataFrame: DataFrame containing the odds from each bookmaker
    """
    current_time = int(round(time.time() * 1000))
    querystring = {"_": f"{current_time}"}
    payload = ""

    headers = {
        'sec-ch-ua': "^\^Google",
        'Referer': "https://www.oddsportal.com/basketball/usa/nba/",
        'DNT': "1",
        'sec-ch-ua-mobile': "?0",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        'sec-ch-ua-platform': "^\^Windows^^"
    }
    url = f"https://fb.oddsportal.com/feed/match/1-3-{game_id}-3-1-{xhash}.dat"
    response = safe_request(
        url, data=payload, headers=headers, params=querystring)
    odds_data = json.loads(re.findall(r"\.dat',\s({.*})", response.text)[0])

    odds_data = odds_data.get('d', {})
    odds_data = odds_data.get("oddsdata", {})
    odds_data = odds_data.get("back", {})
    odds_data = odds_data.get('E-3-1-0-0-0', {})
    odds_data = odds_data.get("odds", {})
    if not odds_data:
        current_time = int(round(time.time() * 1000))
        url = f"https://fb.oddsportal.com/feed/match/1-3-{game_id}-3-1-{xhashf}.dat"
        querystring = {"_": f"{current_time}"}
        response = safe_request(
            url, data=payload, headers=headers, params=querystring)
        odds_data = json.loads(re.findall(
            r"\.dat',\s({.*})", response.text)[0])
        odds_data = odds_data.get('d', {})
        odds_data = odds_data.get("oddsdata", {})
        odds_data = odds_data.get("back", {})
        odds_data = odds_data.get('E-3-1-0-0-0', {})
        odds_data = odds_data.get("odds", {})
    df = format_odds(odds_data, game_id, xhash, xhashf, get_bookies())
    return df


def get_page_source(url, web):
    """
    Helper function to get the html of a given url when
    data is loaded in using JavaScript
    Waits until the data has loaded before the source is returned

    Args:
        url (str): url of the webpage of interest MUST BE AN ODDS PORTAL ODDS PAGE

    Returns:
        BeautifulSoup: rendered HTML of the webpage in a BeautifulSoup object
    """
    web.get(url)
    timeout = 10
    try:
        element_present = EC.presence_of_element_located(
            (By.ID, "tournamentTable"))
        WebDriverWait(web, timeout).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")
        return

    soup = BeautifulSoup(web.page_source, "html.parser")
    return soup


# Get all Bookie IDs
def get_bookies():
    """
    Helper function to return a DataFrame with bookie id and information
    for the oddsportal API

    Returns:
        DataFrame: df with bookmaker information
    """
    conn = http.client.HTTPSConnection("www.oddsportal.com")

    payload = ""

    headers = {
        'sec-ch-ua': "^\^Google",
        'Referer': "https://www.oddsportal.com/basketball/usa/nba/",
        'DNT': "1",
        'sec-ch-ua-mobile': "?0",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        'sec-ch-ua-platform': "^\^Windows^^"
    }

    conn.request(
        "GET", "/res/x/bookies-221117123626-1669130184.js", payload, headers)

    res = conn.getresponse()
    data = res.read()

    data = data.decode("utf-8")
    bookies = json.loads(re.findall(r"bookmakersData=({.*});var", data)[0])
    bookies = pd.DataFrame(bookies).T
    bookies = bookies.reset_index(drop=True)
    cols = ['idProvider', 'WebName', 'WebUrl',
            'IsBookmaker', 'IsBettingExchange']
    bookies = bookies[cols]
    return bookies


def tag_mapping_func(tag):
    """
    helper function to parallelize the processing of game ids when scraping 
    historical odds

    Args:
        tag (str): string containing an HTML "tr" tag of an event from oddsportal

    Returns:
        Tuple: event_id, DataFrame entry / (False, False if error)
    """
    try:
        tag = BeautifulSoup(tag, "html.parser").find("tr")
        gid = tag['xeid']
        entry = {}
        all_a = tag.find_all('a')
        if len(all_a) > 1:
            teams = all_a[0].text
            home, away = teams.split('-')
            entry['away_team'] = away
            entry['home_team'] = home
            # games[gid] = entry
        url = f"http://www.oddsportal.com/basketball/usa/nba/{gid}/"
        current_time = int(round(time.time() * 1000))
        querystring = {"_": f"{current_time}"}
        payload = ""

        headers = {
            'sec-ch-ua': "^\^Google",
            'Referer': "https://www.oddsportal.com/basketball/usa/nba/",
            'DNT': "1",
            'sec-ch-ua-mobile': "?0",
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            'sec-ch-ua-platform': "^\^Windows^^"
        }

        response = safe_request(
            url, data=payload, headers=headers, params=querystring)

        next_soup = BeautifulSoup(response.text, 'html.parser')

        tag = next_soup.find('p', {"class": re.compile("^date")})
        tz = pytz.timezone('US/Central')
        start = decode_terrible_timestamp(tag).astimezone(tz=tz)
        entry['start_time'] = start
        entry["xhashf"] = unhash(re.findall(
            r'xhashf":"([^"]+)"', response.text)[0])
        entry["xhash"] = unhash(re.findall(
            r'xhash":"([^"]+)"', response.text)[0])
        odds_data = get_odds_data(
            gid, entry["xhash"], entry['xhashf'], get_bookies())
        if odds_data.empty:
            print(f"No data for {away}@{home} {start}")
            raise
        entry['mean_home_odds_all'] = odds_data['home_odds'].mean()
        entry['mean_home_odds_bookmakers'] = odds_data[odds_data['IsBookmaker']
                                                       == "y"]['home_odds'].mean()
        entry['mean_home_odds_exchange'] = odds_data[odds_data['IsBettingExchange']
                                                     == "y"]['home_odds'].mean()

        entry['mean_away_odds_all'] = odds_data['away_odds'].mean()
        entry['mean_away_odds_bookmakers'] = odds_data[odds_data['IsBookmaker']
                                                       == "y"]['away_odds'].mean()
        entry['mean_away_odds_exchange'] = odds_data[odds_data['IsBettingExchange']
                                                     == "y"]['away_odds'].mean()

        post_match_data = get_postmatch_data(gid, entry['xhash'])
        finished, home_score, away_score = get_results(gid, entry['xhash'])
        entry['home_score'] = home_score
        entry['away_score'] = away_score
        return gid, entry
    except Exception as e:
        print(e)
        return False, e


def parse_game_tag(game):
    date_tag = game.find('td')
    tz = pytz.timezone('US/Central')
    date = decode_terrible_timestamp(date_tag).astimezone(tz=tz)
    home, away = game.find(
        "td", attrs={"class": "name table-participant"}).text.strip().split(" - ")
    score = game.find("td", attrs={'class': re.compile(r"table-score$")}).text
    home_score, away_score = [int(x.strip())
                              for x in re.split('\xa0|\ ', score)[0].split(":")]
    extra_time = True if len(re.split('\xa0|\ ', score)) > 1 else False
    odds = game.find_all("td", attrs={"xodd": True})
    if len(odds) == 2:
        home_odds, away_odds = [float(x.text) for x in odds]
        draw_odds = np.nan
    else:
        home_odds, draw_odds, away_odds = [float(x.text) for x in odds]
    num_bookies = int(
        game.find("td", attrs={'class': "center info-value"}).text)
    entry = {
        "date": date,
        "home_team": home,
        "away_team": away,
        "home_score": home_score,
        "away_score": away_score,
        "extra_time": extra_time,
        "home_odds": home_odds,
        "draw_odds": draw_odds,
        "away_odds": away_odds,
        "num_bookies": num_bookies
    }
    return entry


def get_odds_portal_driver(odds_type="AVERAGE"):
    op = webdriver.ChromeOptions()
    op.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
    op.add_argument('headless')
    op.add_argument("--disable-web-security")
    op.add_argument("--disable-blink-features=AutomationControlled")
    op.add_argument("--log-level=3")
    if sys.platform == "linux":
        # path to where you saved chromedriver binary
        # webdriver_service = Service('/usr/lib/chromium-browser/chromedriver')
        # web = webdriver.Chrome(service=webdriver_service,
        #                        options=op)
        web = webdriver.Chrome(service='/usr/bin/chromedriver',
                               options=op)

    else:
        web = webdriver.Chrome(
            "C:\\Users\\chris\\OneDrive\\Projects\\odds_portal_scraper\\chromedriver", options=op)

    web.get("https://www.oddsportal.com/login/")

    login_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[5]/div/div/form/div[4]/span/input'
    WebDriverWait(web, 10).until(
        EC.element_to_be_clickable((By.XPATH, login_xpath)))
    user_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[5]/div/div/form/div[1]/div[2]/input'
    user = web.find_element(By.XPATH, user_xpath)
    user.send_keys("cjarmstrong2018")
    pswd_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[5]/div/div/form/div[2]/div[2]/input'
    pswd = web.find_element(By.XPATH, pswd_xpath)
    pswd.send_keys("Cps!43950649")

    login = web.find_element(By.XPATH, login_xpath)
    web.execute_script("arguments[0].scrollIntoView();", login)
    web.execute_script("arguments[0].click();", login)

    web.get("https://www.oddsportal.com/settings/")
    if odds_type == "HIGHEST":
        button_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[5]/div[3]/form/div[2]/div[2]/div[3]/div/div[2]/input'
    else:
        button_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[5]/div[3]/form/div[2]/div[2]/div[3]/div/div[1]/input'
    WebDriverWait(web, 10).until(
        EC.presence_of_element_located((By.XPATH, button_xpath)))

    button = web.find_element(By.XPATH, button_xpath)
    try:
        button.click()
    except ElementClickInterceptedException:
        print("There may be an error with the odds type!")
    # button.send_keys('\n')

    save = web.find_element(By.NAME, "settings-submit")
    save.send_keys("\n")
    return web


def basic_kelly_criterion(prob, odds, kelly_size=1):
    """
    Taken directly from 
    https://github.com/sedemmler/WagerBrain/blob/master/WagerBrain/bankroll.py
    :param prob: Float. Estimated probability of winning the wager
    :param odds: Integer (American), Float(Decimal), String or Fraction Class (Fractional). Stated odds from bookmaker
    :param kelly_size: Integer. Risk management. (e.g., 1 is Kelly Criterion, .5 is Half Kelly, 2+ is Levered Kelly)
    :return: Float. % of bankroll one should commit to wager
    """
    b = odds - 1
    q = 1 - prob
    return ((b * prob - q) / b) * kelly_size
