import pandas as pd
import requests
import json
from dateutil import parser
import pytz
from pybettor import convert_odds


class PointsBet(object):
    def __init__(self) -> None:
        self.name = "PointsBet"
        self.league_ids = {
            "NBA": 5,
            "MLB": 3,
            "NFL": 2,
            "NCAAF": "american_football/ncaaf",
            "NCAAB": 4,
            "NHL": 1,
            "EPL": 15290,
            "LaLiga": 294836,
            "SerieA": 14665,
            "Champions_League": 14704,
            "Ligue1": 14664,
            "MLS": 797,
            "Bundesliga": 14616
        }

    def get_odds(self, league):
        league_id = self.league_ids.get(league, False)
        if not league_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self.league_ids.keys())}")
        url = f"https://api.il.pointsbet.com/api/v2/competitions/{league_id}/events/featured"
        headers = {
            "referer": "https://il.pointsbet.com/",
            "request-context": "appId=cid-v1:ce0ccdbc-efee-48c2-b03c-a261da7f6c2c",
            "request-id": "^|35dee362dceb48f99ddda5675fd23a47.9edc676e103b4993",
            "sec-ch-ua": "^\^Not?A_Brand^^;v=^\^8^^, ^\^Chromium^^;v=^\^108^^, ^\^Google",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "^\^Windows^^",
            "traceparent": "00-35dee362dceb48f99ddda5675fd23a47-9edc676e103b4993-01",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }

        querystring = {"includeLive": "false", "page": "1"}

        payload = ""
        response = requests.request(
            "GET", url, data=payload, headers=headers, params=querystring)

        data = json.loads(response.text)
        entries = []
        events = data.get('events', False)
        if not events:
            print(data)
            return pd.DataFrame()
        for event in events:
            home_team = event.get('homeTeam')
            away_team = event.get('awayTeam')
            odds_markets = event.get("specialFixedOddsMarkets", False)
            if not odds_markets:
                odds_markets = event.get("fixedOddsMarkets")
            for line in odds_markets:
                if line.get("eventClass") not in ["Moneyline", 'Match Result']:
                    continue
                date = line.get('advertisedStartTime')
                date = parser.parse(date)
                tz = pytz.timezone('US/Central')
                date = date.astimezone(tz)
                date = date.replace(tzinfo=None)
                outcomes = line.get("outcomes")
                for mkt in outcomes:
                    odds_team = mkt.get("name")
                    odds = mkt.get("price")
                    entry = {
                        "date": date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "odds_team": odds_team,
                        "odds": odds
                    }
                    entries.append(entry)
        return pd.DataFrame(entries)
