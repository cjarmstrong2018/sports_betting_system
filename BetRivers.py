import pandas as pd
import numpy as np
import http.client
import pytz
import json
from datetime import datetime, timezone
from pybettor import convert_odds
from utils import normalize_teams


class BetRivers(object):
    def __init__(self):
        self.name = "BetRivers"
        self._group_ids = {
            'NFL': 1000093656,
            "MLB": 1000093616,
            "NBA": 1000093652,
            "NCAAB": 1000093654,
            "NCAAF": 1000093655,
            "NHL": 1000093657,
            "EPL": 1000094985,
            "LaLiga": 1000095049,
            "SerieA": 1000095001,
            "Champions_League": 1000093381,
            "Ligue1": 1000094991,
            "MLS": 1000095063,
            "Bundesliga": 1000094994
        }

    def get_odds(self, league):
        group_id = self._group_ids.get(league, False)
        if not group_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self._group_ids.keys())}")
        conn = http.client.HTTPSConnection("il.betrivers.com")
        conn.request(
            "GET", f"/api/service/sportsbook/offering/listview/events?pageNr=1&cageCode=847&groupId={group_id}&=&type=prematch")

        res = conn.getresponse()
        data = res.read()
        data.decode("utf-8")
        data = json.loads(data)

        # Get odds
        if not data['items']:
            return pd.DataFrame()

        odds = pd.json_normalize(data['items'], record_path=['betOffers', 'outcomes'], meta=[
            'id', ['betOffers', "betDescription"]], errors='ignore', meta_prefix='Meta.')
        odds = odds[['type', 'odds', 'Meta.id',
                     'Meta.betOffers.betDescription']]
        odds = odds.rename(columns={'Meta.betOffers.betDescription': 'Bet Type',
                                    'Meta.id': "Game ID", })
        odds = odds[odds['Bet Type'].isin(['Moneyline', "Full Time"])]
        odds = odds.set_index(['Game ID'])

        teams = pd.json_normalize(data['items'], 'participants', [
            'id'], errors="ignore", record_prefix='T1').set_index(['id', 'T1home'])
        teams = teams.unstack(level=1)

        teams = teams.droplevel(0, axis=1)
        teams.columns = ['away_team', 'home_team', "Away ID", "Home ID"]
        teams = teams[['away_team', 'home_team']]

        teams.index.name = "Game ID"

        games = pd.json_normalize(data, 'items', errors="ignore")
        games = games[['id', 'start']]
        games = games.rename(columns={'id': "Game ID"})
        games = games.set_index('Game ID')

        df = teams.merge(games, how='inner', left_index=True, right_index=True)
        df = df.merge(odds, how='inner', left_index=True, right_index=True)
        df['start'] = pd.to_datetime(df['start'], utc=True)
        df = df.set_index("start")
        df.index = df.index.tz_convert('US/Central')
        df.index = df.index.tz_localize(None)
        df.index.name = 'date'
        columns = ["home_team", "away_team",
                   "type", "odds"]
        df = df[columns]
        df = df.rename(columns={"type": "odds_team"})
        df = df.reset_index()
        df.columns = ['date', 'home_team', 'away_team', 'odds_team', 'odds']
        if league in ['NFL', "NBA", "MLB", "NHL"]:
            df['home_team'] = normalize_teams(df['home_team'], league)
            df['away_team'] = normalize_teams(df['away_team'], league)
        df['odds_team'] = np.where(
            df['odds_team'] == 'HOME', df['home_team'], df['odds_team'])
        df['odds_team'] = np.where(
            df['odds_team'] == 'AWAY', df['away_team'], df['odds_team'])
        df['odds_team'] = np.where(
            df['odds_team'] == 'DRAW', "Draw", df['odds_team'])

        return df

    def get_all_odds(self):
        """
        Parses each of the urls and returns a DataFrame of all the
        current average odds

        Returns:
            pd.DataFrame: DataFrame of odds
        """
        dfs = []
        for league in self._group_ids.keys():
            try:
                df = self.get_odds(league)
                if df.empty:
                    continue
                df['league'] = league
                if league in ['NFL', "NBA", "MLB"]:
                    df['home_team'] = normalize_teams(df['home_team'], league)
                    df['away_team'] = normalize_teams(df['away_team'], league)
                dfs.append(df)
            except:
                print(f"Error with {league}")
                continue
        df = pd.concat(dfs)
        return df
