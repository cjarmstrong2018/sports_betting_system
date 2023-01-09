import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
from pybettor import convert_odds
import sys

class OddsJam(object):
    """
    Object to gather average odds from oddsjam.com for all upcoming matches
    """

    def __init__(self) -> None:
        self.urls = {
            "NCAAF": "https://oddsjam.com/ncaaf/odds",
            "NCAAB": "https://oddsjam.com/ncaab/odds",
            "NBA": "https://oddsjam.com/nba/odds",
            "MLB": "https://oddsjam.com/mlb/odds",
            "NFL": "https://oddsjam.com/nfl/odds",
            "EPL": "https://oddsjam.com/soccer/league/england-premier-league",
            "UEFA": 'https://oddsjam.com/soccer/league/uefa-champions-league',
            "SerieA": "https://oddsjam.com/soccer/league/italy-serie-a",
            "LaLiga": "https://oddsjam.com/soccer/league/spain-la-liga",
            "NHL": "https://oddsjam.com/nhl/odds",
        }

        op = webdriver.ChromeOptions()
        op.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
        op.add_argument('headless')
        op.add_argument("--disable-web-security")
        op.add_argument("--disable-blink-features=AutomationControlled")
        op.add_argument("--log-level=3")
        if sys.platform == "linux":
            chromedriver_path = '/usr/lib/chromium-browser/chromedriver' 
        else:
            chromedriver_path = "C:\\Users\\chris\\OneDrive\\Projects\\odds_portal_scraper\\chromedriver"
        self.web = webdriver.Chrome(chromedriver_path, options=op)

    def get_lines(self, league) -> pd.DataFrame:
        """
        Collects the odds posted on www.oddjam.com for a given league
        and returns a formatted dataframe of the odds

        Args:
            league (str): the acronym of the league of interest

        Returns:
            pd.DataFrame: dataframe of odds
        """
        assert league in self.urls.keys()

        url = self.urls.get(league)
        self.web.get(url)
        xpath = '//*[@id="__next"]/div/main/div[1]/div[1]/div'
        try:
            WebDriverWait(self.web, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath)))
        except:
            print(f"Error with {league}")
            return pd.DataFrame()
        soup = BeautifulSoup(self.web.page_source, "html.parser")
        extensions = []
        for table in soup.find_all("div", attrs={"class": "grid gap-6 grid-cols-1 2xl:grid-cols-2"}):
            ext = [game['href'] for game in table.find_all(
                "a", attrs={'href': True}) if "moneyline" in game['href']]
            extensions.extend(ext)
        dfs = []
        for ext in extensions:
            url2 = "https://oddsjam.com" + ext
            self.web.get(url2)
            xpath = "//*[@id='__next']/div/main/div[2]/div[4]/div/div[1]/div[1]/div"
            try:
                WebDriverWait(self.web, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
            except:
                print(f"Error with game {url2}")
                continue
            soup2 = BeautifulSoup(self.web.page_source, "html.parser")
            date_tag = soup2.find(
                "span", attrs={"class": "relative w-fit h-fit"})
            if date_tag is None:
                continue
            date_tag = date_tag.text + f" {datetime.now().year}"
            date = datetime.strptime(date_tag, "%a, %b %d at %I:%M %p %Y")
            if "soccer" in url2:
                away_team, home_team, _ = [x['id'] for x in soup2.find_all(
                    "div", attrs={'class': "lg:pl-2 w-[85%]"})]
            else:
                away_team, home_team = [x['id'] for x in soup2.find_all(
                    "div", attrs={'class': "lg:pl-2 w-[85%]"})]
            away_team = (" ").join(away_team.split("_"))
            home_team = (" ").join(home_team.split("_"))
            lines = soup2.find_all("p", attrs={'data-testid': True})
            odds = [x.text for x in lines]
            ids = [x['data-testid'] for x in lines]
            teams = [x.split("-")[-2] for x in ids]
            bookies = [x.split("-")[-1] for x in ids]
            data = list(zip(teams, bookies, odds))
            df = pd.DataFrame(data, columns=["odds_team", "bookmaker", "odds"])
            df['home_team'] = home_team
            df['away_team'] = away_team
            df['date'] = date
            dfs.append(df)
        all_lines = pd.concat(dfs)
        all_lines['sport'] = league
        column_order = ['date', 'home_team',
                        'away_team', 'odds_team', 'bookmaker', 'sport', 'odds']
        all_lines = all_lines[column_order]
        all_lines['odds'] = all_lines['odds'].astype(int)
        all_lines['odds'] = all_lines['odds'].apply(
            lambda x: convert_odds(x, cat_in="us")['Decimal'])
        return all_lines
