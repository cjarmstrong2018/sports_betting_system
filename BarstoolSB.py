import pandas as pd
import requests
import json
from dateutil import parser
import pytz
from pybettor import convert_odds


class Barstool(object):
    def __init__(self) -> None:
        self.name = "Barstool"
        self.league_ids = {
            "NBA": "basketball/nba",
            "MLB": "baseball/mlb",
            "NFL": "american_football/nfl",
            "NCAAF": "american_football/ncaaf",
            "NCAAB": "basketball/ncaab",
            "NHL": "ice_hockey/nhl",
            "EPL": "football/england/premier_league",
            "LaLiga": "football/spain/la_liga",
            "SerieA": "football/italy/serie_a",
            "Champions_League": "football/champions_league",
            "Ligue1": "football/france/ligue_1",
            "MLS": "football/usa/mls",
            "Bundesliga": "football/germany/bundesliga"
        }

    def get_odds(self, league):
        league_id = self.league_ids.get(league, False)
        if not league_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self.league_ids.keys())}")
        url = f"https://eu-offering-api.kambicdn.com/offering/v2018/pivusil/listView/{league_id}/all/all/matches.json"

        querystring = {"market": "US", "useCombined": "true", "lang": "en_US"}

        payload = ""
        response = requests.request(
            "GET", url, data=payload, params=querystring)
        if response.status_code != 200:
            return pd.DataFrame()
        data = json.loads(response.text)
        entries = []
        for event in data.get('events'):
            event_info = event.get("event")
            event_name = event_info.get("englishName")
            try:
                home_team, away_team = event_name.split(' - ')
            except:
                continue
            away_team = away_team.strip()
            home_team = home_team.strip()
            date = event_info.get('start')
            date = parser.parse(date)
            tz = pytz.timezone('US/Central')
            date = date.astimezone(tz)
            date = date.replace(tzinfo=None)
            lines = event.get("betOffers")
            try:
                lines = [x for x in lines if x['criterion'].get(
                    'label') in ["Moneyline", "Full Time", 'Moneyline - Inc. OT and Shootout']][0]
            except IndexError:
                continue
            lines = lines.get("outcomes")
            for line in lines:
                odds_team = line.get("englishLabel")
                if odds_team == "1" or odds_team == "2":
                    odds_team = line.get("participant")
                elif odds_team == "X":
                    odds_team = "Draw"

                odds = convert_odds(
                    int(line.get("oddsAmerican")), cat_out="dec")[0]
                entry = {
                    "date": date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "odds_team": odds_team,
                    "odds": odds
                }
                entries.append(entry)
        return pd.DataFrame(entries)
