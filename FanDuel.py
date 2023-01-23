import pandas as pd
import requests
import json
from dateutil import parser
import pytz
from datetime import datetime, timedelta
from utils import central_time_now


class FanDuel(object):
    def __init__(self):
        self.name = "FanDuel"
        self._group_ids = {
            "NBA": 55978,
            "NFL": 50037,
            "NCAAB": 53474,
            "MLB": 50084,
            "NCAAF": None,
            "NHL": 50530,
            "EPL": 51335,
            "LaLiga": 50652,
            "SerieA": 52643,
            "Champions_League": 50204,
            "Ligue1": 52642,
            "MLS": None,
            "Bundesliga": 52646,
        }

    def get_odds(self, league):
        group_id = self._group_ids.get(league, False)
        if not group_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self._group_ids.keys())}")

        url = f"https://sportsbook.fanduel.com/cache/psmg/UK/{group_id}.3.json"

        payload = ""
        headers = {
            "cookie": "X-Mapping-hfimiklm=37433E545497E228B83DFF211FEF00D8"}

        response = requests.request("GET", url, data=payload, headers=headers)

        data = json.loads(response.text)
        entries = []
        for event in data['events']:
            date = event.get("tsstart")
            date = parser.parse(date)
            date = date.replace(tzinfo=None)
            date = date - timedelta(hours=1)
            if date < central_time_now():
                continue
            home_team = event.get("participantname_home")
            away_team = event.get("participantname_away")
            markets = event.get("markets")
            for market in markets:
                if market.get("name") != "Moneyline":
                    continue
                market = market.get("selections")
                for mkt in market:
                    odds_team = mkt.get("name")
                    odds = mkt.get("price")
                    entry = {
                        "date": date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "odds_team": odds_team,
                        "odds": odds,
                    }
                    entries.append(entry)
        return pd.DataFrame(entries)
