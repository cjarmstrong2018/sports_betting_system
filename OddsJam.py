import pandas as pd
from dateutil import parser
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
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
            "Champions_League": 'https://oddsjam.com/soccer/league/uefa-champions-league',
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
            chromedriver_path = '/home/christian_armstrong25/sports_betting_system/chromedriver'
        else:
            chromedriver_path = "C:\\Users\\chris\\OneDrive\\Projects\\odds_portal_scraper\\chromedriver"
        self.web = webdriver.Chrome(chromedriver_path, options=op)

    def get_lines(self, league, abridged=False) -> pd.DataFrame:
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
            WebDriverWait(self.web, 4).until(
                EC.presence_of_element_located((By.XPATH, xpath)))
        except:
            return pd.DataFrame()
        soup = BeautifulSoup(self.web.page_source, "html.parser")
        extensions = []
        for table in soup.find_all("div", attrs={"class": "grid gap-6 grid-cols-1 2xl:grid-cols-2"}):
            ext = [game['href'] for game in table.find_all(
                "a", attrs={'href': True}) if "moneyline" in game['href']]
            extensions.extend(ext)
        extensions = [x for x in extensions if "sportOrLeague" not in x]
        dfs = []
        for ext in extensions:
            url2 = "https://oddsjam.com" + ext
            try:
                self.web.get(url2)
                xpath = "//*[@id='__next']/div/main/div[2]/div[4]/div/div[1]/div[1]/div"
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
            if abridged and date > datetime.now() + timedelta(days=1):
                break
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
        if not dfs:
            return pd.DataFrame()
        all_lines = pd.concat(dfs)
        all_lines['sport'] = league
        column_order = ['date', 'home_team',
                        'away_team', 'odds_team', 'bookmaker', 'sport', 'odds']
        all_lines = all_lines[column_order]
        all_lines['odds'] = all_lines['odds'].astype(int)
        all_lines['odds'] = all_lines['odds'].apply(
            lambda x: convert_odds(x, cat_in="us")['Decimal'])
        return all_lines

    def get_best_lines(self, league):
        """
        Parses OddsJam for the best lines available for a given event

        Args:
            league (str): the league of interest
        """
        assert league in self.urls.keys()

        url = self.urls.get(league)

        data = []
        self.web.get(url)
        xpath = '//*[@id="__next"]/div/main/div[1]/div[1]/div'
        try:
            WebDriverWait(self.web, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath)))
        except:
            print(f"Error with {league}")
            return pd.DataFrame()
        soup = BeautifulSoup(self.web.page_source, "html.parser")
        for table in soup.find_all("div", attrs={"class": "grid gap-6 grid-cols-1 2xl:grid-cols-2"}):
            # Handle dates
            date = table.previous_sibling()[0]
            if date is None:
                break
            date = date.text
            if "Today" in date:
                date = datetime.today()
            elif "Tomorrow" in date:
                date = date.today + timedelta(days=1)
            elif date in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
                # Path this later
                continue
            else:
                date += f" {datetime.today().year}"
                date = parser.parse(date)
            game_tags = table.find_all(
                'div', class_="bg-white rounded-xl flex justify-between h-[120px] shadow")
            for game in game_tags:
                entry = {}
                time = game.find(
                    'span', class_="text-brand-gray-1 text-xs font-prompt uppercase flex items-center").text.strip()
                date_time = datetime.strptime(time, "%H:%M%p")
                date_time = date_time.replace(
                    day=date.day, month=date.month, year=date.year)
                teams = game.find_all(
                    'p', class_="text-base font-inter text-brand-gray-1 font-semibold overflow-hidden truncate")
                home_team, away_team = [x.text for x in teams]

                moneyline_tag = game.find(
                    "a", class_="px-3 flex flex-col justify-between py-2 w-[105px] border-brand-gray-10 border-l-2")
                try:
                    home_odds, away_odds = moneyline_tag.find_all(
                        "p", class_="font-inter text-brand-gray-1 font-bold text-sm")
                    home_odds = int(home_odds.text)
                    away_odds = int(away_odds.text)
                except ValueError:
                    continue
                best_books = moneyline_tag.find_all('img', attrs={'alt': True})
                home_bookie, away_bookie = [x['alt'] for x in best_books]
                home_entry = {
                    "date": date_time,
                    "home_team": home_team,
                    'away_team': away_team,
                    "odds_team": home_team,
                    'odds_book': home_bookie,
                    'odds': home_odds,
                }
                away_entry = {
                    "date": date_time,
                    "home_team": home_team,
                    'away_team': away_team,
                    "odds_team": away_team,
                    'odds_book': away_bookie,
                    'odds': away_odds,
                }

                data.append(home_entry)
                data.append(away_entry)
        return pd.DataFrame(data)

    def exit(self):
        """
        Closes the chromedriver instance associated with the OddsJam object
        """
        self.web.quit()
