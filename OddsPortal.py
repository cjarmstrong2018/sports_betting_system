from utils import *
import pandas as pd
import json
import requests
import pytz
from datetime import datetime, timezone


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
            "NCAAF": "https://www.oddsportal.com/american-football/usa/ncaa/"
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
        soup = get_page_source(url, self.web)
        table = soup.find(
            "table", attrs={"id": "tournamentTable"}).find("tbody")
        games = table.find_all("tr", attrs={"xeid": True})
        game_entries = []
        for game in games:
            if game.find("span", attrs={'class': "live-odds-ico-prev"}) is not None:
                continue
            try:
                entry = self.parse_upcoming_game_tag(game)
                game_entries.append(entry)
            except Exception as e:
                print("Error parsing entry!")
        df = pd.DataFrame(game_entries)
        return df

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
