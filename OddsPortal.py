from utils import *
import pandas as pd
import json
import requests
import pytz
from datetime import datetime, timedelta


class OddsPortal(object):
    """
    Object to gather average odds from oddsportal.com for all upcoming matches
    """

    def __init__(self) -> None:
        self.web = get_odds_portal_driver()

        self.paths = {
            "NFL": "https://www.oddsportal.com/american-football/usa/nfl/",
            "MLB": "https://www.oddsportal.com/baseball/usa/mlb/",
            "NBA": "https://www.oddsportal.com/basketball/usa/nba/",
            "NCAAB": "https://www.oddsportal.com/basketball/usa/ncaa/",
            "NCAAF": "https://www.oddsportal.com/american-football/usa/ncaa/",
            "NHL": "https://www.oddsportal.com/hockey/usa/nhl/",
            "EPL": "https://www.oddsportal.com/soccer/england/premier-league/"
        }

    def get_odds(self, league) -> pd.DataFrame:
        """
        Collects the average odds posted on www.oddportal.com for a given league
        and returns a formatted dataframe of the odds

        Args:
            league (str): the acronym of the league of interest

        Returns:
            pd.DataFrame: dataframe of odds
        """
        assert league in self.paths.keys()

        url = self.paths.get(league)
        self.web.get(url)
        table_xpath = '/html/body/div[1]/div/div[1]/div/main/div[2]/div[7]'
        WebDriverWait(self.web, 10).until(
            EC.element_to_be_clickable((By.XPATH, table_xpath)))

        soup = BeautifulSoup(self.web.page_source, 'html.parser')
        table = soup.find(
            "div", attrs={"class": "flex flex-col px-3 text-sm max-mm:px-0"})
        games = []
        current_date = None
        for child in table.children:
            try:
                date_tag = child.find("div", attrs={
                                      "class": "w-full text-xs font-normal leading-5 text-black-main font-main"})
            except TypeError:
                break
            if date_tag is not None:
                text = date_tag.text
                text = text.split('-')[0].strip()
                if "Today" in text:
                    text += (" " + str(datetime.today().year))
                    current_date = datetime.strptime(text, "Today, %d %b %Y")
                elif "Tomorrow" in text:
                    text += (" " + str(datetime.today().year))
                    current_date = datetime.strptime(
                        text, "Tomorrow, %d %b %Y")
                else:
                    current_date = datetime.strptime(text.strip(), "%d %b %Y")
            game = child.find(
                "div", class_="flex hover:bg-[#f9e9cc] group border-l border-r border-black-borders")
            if game is None:
                break
            teams = game.find_all('img')
            home_team, away_team = [x['alt'] for x in teams]
            time = game.find('p', class_="whitespace-nowrap").text
            if ":" not in time:
                continue
            hours, mins = time.split(":")
            date = current_date + \
                timedelta(hours=int(hours), minutes=int(mins))
            if date < datetime.now():
                print("game already started!")
                continue
            odds = game.find_all('p', class_="height-content")
            if len(odds) == 2:
                home_odds, away_odds = [x.text for x in odds]
                draw_odds = np.nan
            else:
                home_odds, draw_odds, away_odds = [x.text for x in odds]
            if home_odds == '-' or away_odds == '-':
                continue
            home_odds = float(home_odds)
            away_odds = float(away_odds)
            if draw_odds:
                draw_odds = float(draw_odds)
            num_bookies = game.find(
                'div', class_="height-content text-[10px] leading-5 text-black-main").text
            num_bookies = int(num_bookies)
            game_entry = {
                "date": date,
                "home_team": home_team,
                'away_team': away_team,
                "home_odds": home_odds,
                "draw_odds": draw_odds,
                "away_odds": away_odds,
                "num_bookies": num_bookies
            }
            games.append(game_entry)
        return pd.DataFrame(games)

    def get_all_odds(self):
        """
        Parses each of the urls and returns a DataFrame of all the
        current average odds

        Returns:
            pd.DataFrame: DataFrame of odds
        """
        dfs = []
        for league in self.paths.keys():
            df = self.get_odds(league)
            if df.empty:
                continue
            df['league'] = league
            if league in ['NFL', "NBA", "MLB"]:
                df['home_team'] = normalize_teams(df['home_team'], league)
                df['away_team'] = normalize_teams(df['away_team'], league)
            dfs.append(df)
        df = pd.concat(dfs)
        df = df.set_index("date")
        return df

    def parse_upcoming_game_tag(self, game) -> dict:
        """
        Helper function to extract data from a tag in the OddsPortal table

        Args:
            game (Soup): a BeautifulSoup object
        returns: dict of game information
        """
        date_tag = game.find('td')
        tz = pytz.timezone('US/Central')
        date = decode_terrible_timestamp(date_tag).astimezone(tz=tz)
        date = date.replace(tzinfo=None)
        home, away = game.find(
            "td", attrs={"class": "name table-participant"}).text.strip().split(" - ")
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
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "num_bookies": num_bookies
        }
        return entry

    def exit(self):
        """
        Cleanup function to close anything running in the object
        """
        self.web.quit()
