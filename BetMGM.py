import asyncio
from requests_html import HTMLSession, AsyncHTMLSession
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from pybettor import convert_odds
from utils import normalize_teams

"""
To Do:
 - Fix NCAAB parsing to get upcoming games

"""


class BetMGM(object):
    def __init__(self):
        self.name = "BetMGM"
        self.sports = {
            "NBA": "https://sports.il.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004",
            "MLB": "https://sports.il.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75",
            "NFL": "https://sports.il.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35",
            "NCAAF": "https://sports.il.betmgm.com/en/sports/football-11/betting/usa-9/college-football-211",
            "NCAAB": "https://sports.il.betmgm.com/en/sports/basketball-7/betting/usa-9/ncaa-264",
            # "EPL": "https://sports.il.betmgm.com/en/sports/soccer-4/betting/england-14/premier-league-102841"
        }

        op = webdriver.ChromeOptions()
        op.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
        op.add_argument('headless')
        op.add_argument("--disable-web-security")
        op.add_argument("--disable-blink-features=AutomationControlled")

        self.web = webdriver.Chrome(
            "C:\\Users\\chris\\OneDrive\\Projects\\odds_portal_scraper\\chromedriver", options=op)

    def get_odds(self, league):
        """
        Returns a dataframe of the current odds posted on BetMGM
        for a provided league

        Args:
            league (str): the league of interest

        Returns:
            DataFrame: df of the current upcoming odds
        """
        url = self.sports.get(league, False)
        if not url:
            raise ValueError(
                f"League is invalid, it must be one of {self.sports.keys()}")

        self.web.get(url)
        timeout = 10
        try:
            element_present = EC.presence_of_element_located(
                (By.CLASS_NAME, "participants-pair-game"))
            WebDriverWait(self.web, timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load")

        if league == "NCAAB":
            upcoming = self.web.find_element(
                By.XPATH, "//*[@id='main-view']/ms-widget-layout/ms-widget-slot/ms-composable-widget/ms-widget-tab-bar/ms-tab-bar/ms-scroll-adapter/div/div/ul/li[2]/a/span")
            upcoming.click()
            try:
                element_present = EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='main-view']/ms-widget-layout/ms-widget-slot/ms-composable-widget/ms-widget-slot"))
                WebDriverWait(self.web, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load")
        soup = BeautifulSoup(self.web.page_source, "html.parser")
        games = soup.find_all('ms-six-pack-event')
        data = []
        for game in games:
            is_live = game.find('i', class_=re.compile("^live"))
            if is_live:
                continue
            # get dates
            start = game.find("ms-event-timer", class_='grid-event-timer')
            if start is None:
                continue
            else:
                start = start.text
            if "Starting" in start:
                mins_to_start = int(start.split(' ')[-2])
                date = datetime.now() + timedelta(minutes=mins_to_start)
            elif "Today" in start:
                start = start.split()[-2:]
                start = " ".join(start)
                start = datetime.strptime(start, "%H:%M %p")
                date = datetime.now()
                date = date.replace(hour=start.hour, minute=start.minute)
            elif "Tomorrow" in start:
                start = start.split()[-2:]
                start = " ".join(start)
                start = datetime.strptime(start, "%I:%M %p")
                date = datetime.now() + timedelta(days=1)
                date = date.replace(hour=start.hour, minute=start.minute)
            else:
                start = re.sub("•", "", start)
                date = datetime.strptime(start, "%m/%d/%y  %I:%M %p")
            date = date.replace(second=0, microsecond=0)
            # get teams
            participants = game.find_all("div", class_="participant")
            # addresses any special matches or lines listed
            if not participants:
                break
            away_team, home_team = [x.text.strip() for x in participants]
            lines = game.find_all("ms-option-group")
            moneyline_tag = lines[-1]
            # moneyline odds
            moneyline_odds = moneyline_tag.find_all(
                "div", class_="option option-value")
            if not moneyline_odds:
                away_odds = np.nan
                home_odds = np.nan
            else:
                if len(moneyline_odds) == 2:
                    away_odds, home_odds = [int(x.text)
                                            for x in moneyline_odds]
            # create data dict
            home_entry = {
                "date": date,
                'home_team': home_team,
                'away_team': away_team,
                "odds_team": home_team,
                'odds': home_odds,
            }
            away_entry = {
                "date": date,
                'home_team': home_team,
                'away_team': away_team,
                "odds_team": away_team,
                'odds': away_odds,
            }
            data.append(home_entry)
            data.append(away_entry)
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df = df.dropna()
        df = df.set_index("date")
        df.index = pd.to_datetime(df.index)
        df['odds'] = df['odds'].apply(
            lambda x: convert_odds(int(x), cat_out="dec")[0])
        return df

    def get_all_odds(self):
        """
        Parses each of the urls and returns a DataFrame of all the
        current average odds

        Returns:
            pd.DataFrame: DataFrame of odds
        """
        dfs = []
        for league in self.sports.keys():
            df = self.get_odds(league)
            if df.empty:
                continue
            df['league'] = league
            if league in ['NFL', "NBA", "MLB"]:
                df['home_team'] = normalize_teams(df['home_team'], league)
                df['away_team'] = normalize_teams(df['away_team'], league)
            dfs.append(df)
        df = pd.concat(dfs)
        return df


async def async_render():
    session = AsyncHTMLSession()
    res = await session.get("https://sports.il.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004")
    await res.html.arender(timeout=10000)
    return res.html.raw_html


def render():
    session = HTMLSession()
    res = session.get(
        "https://sports.il.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004")
    res.html.render(timeout=10000)
    return res.html.content
