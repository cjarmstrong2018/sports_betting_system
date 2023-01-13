from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pandas as pd
from utils import normalize_teams
from pybettor import convert_odds
import sys


class OddsTrader(object):
    """
    Class that wraps collection of best_odds from www.oddstrader.com
    """

    def __init__(self) -> None:
        self.leagues = {
            "NFL": "nfl",
            "NBA": 'nba',
            "MLB": "mlb",
            "NHL": "nhl",
            "NCAAF": "ncaa-college-football",
            "NCAAB": "ncaa-college-basketball",
            "EPL": "english-premier-league",
            "LaLiga": "spanish-la-liga",
            "Champions_League": "uefa-champions-league",
            "SerieA": "italian-serie-a"

        }
        op = webdriver.ChromeOptions()
        op.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
        op.add_argument('headless')
        op.add_argument("--disable-web-security")
        op.add_argument("--disable-blink-features=AutomationControlled")
        op.add_argument("--log-level=3")
        if sys.platform == "linux":
            self.web = webdriver.Chrome(
                '/usr/bin/chromedriver', options=op)
        else:
            self.web = webdriver.Chrome(
                "C:\\Users\\chris\\OneDrive\\Projects\\odds_portal_scraper\\chromedriver", options=op)

    def get_best_lines(self, league):
        assert league in self.leagues.keys()

        insert = self.leagues.get(league)
        url = f"https://www.oddstrader.com/{insert}/?g=game&m=money"
        self.web.get(url)
        xpath = '/html/body/div[2]/div/div/div/div[1]/div/section/div[1]/div[4]/div/table/tbody'
        try:
            WebDriverWait(self.web, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath)))
        except:
            print(f"Error")
            return pd.DataFrame()

        soup = BeautifulSoup(self.web.page_source, "html.parser")
        table = soup.find('tbody', attrs={'data-cy': 'odds-grid-table-body'})
        data = []
        games = table.find_all(
            "tr", attrs={'data-cy': "participant-row-event-wrapper"})
        for game in games:
            tag_1 = game.find_next("tr")
            tag_2 = tag_1.find_next('tr')
            date_tag = tag_1.find("span", class_='generalDay')
            if not date_tag:
                print("Live Game!")
                continue
            else:
                time_tag = date_tag.find_next('span')
                date = (date_tag.text + " " + time_tag.text).lower()
                date = datetime.strptime(date, "%a %m/%d %H:%M %p")
                date = date.replace(year=datetime.today().year)
                team_1 = tag_1.find('div', class_="nameAndRanking")
                team_2 = tag_2.find('div', class_="nameAndRanking")
                away_team = team_1.find_next('span').text
                home_team = team_2.find_next('span').text
                participant_1 = tag_1.find('div', class_="participant")
                participant_2 = tag_2.find('div', class_="participant")
                away_line_tag = participant_1.find('span', class_="best-line")
                home_line_tag = participant_2.find('span', class_="best-line")
                away_odds = away_line_tag.text
                home_odds = home_line_tag.text
                away_best_bookie = away_line_tag.find_next('span').text
                home_best_bookie = home_line_tag.find_next('span').text
                home_entry = {
                    "date": date,
                    'home_team': home_team,
                    "away_team": away_team,
                    "odds_team": home_team,
                    'bookmaker': home_best_bookie,
                    'odds': home_odds

                }
                away_entry = {
                    "date": date,
                    'home_team': home_team,
                    "away_team": away_team,
                    "odds_team": away_team,
                    'bookmaker': away_best_bookie,
                    'odds': away_odds
                }
                data.append(home_entry)
                data.append(away_entry)
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df = df[df['odds'] != '-']
        if league in ['NFL', "MLB", "NBA", "NHL"]:
            df['home_team'] = normalize_teams(df['home_team'], league)
            df['away_team'] = normalize_teams(df['away_team'], league)
            df['odds_team'] = normalize_teams(df['odds_team'], league)
        df['odds'] = df['odds'].astype(int)
        df['odds'] = df['odds'].apply(
            lambda x: convert_odds(x, cat_in="us")['Decimal'])
        df = df[df['date'] < datetime.now()]
        return df

    def exit(self):
        self.web.close()
