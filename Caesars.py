import pandas as pd
import requests
import json
from dateutil import parser
import pytz
from pybettor import convert_odds

"""
To Do
- Add different extensions
- insert competition_ids
"""


class Caesars(object):
    def __init__(self) -> None:
        self.name = "Ceasars"
        self.league_exts = {
            "NBA": ("basketball", "5806c896-4eec-4de1-874f-afed93114b8c"),
            "NCAAB": ("basketball", "d246a1dd-72bf-45d1-bc86-efc519fa8e90"),
            "NHL": ("icehockey", "b7b715a9-c7e8-4c47-af0a-77385b525e09"),
            "MLB": ("baseball", "04f90892-3afa-4e84-acce-5b89f151063d"),
            "EPL": ("football", "00200de2-37f3-4339-9e3d-9bf8cb5eb34b"),
            "NCAAF": ('americanfootball', "b7eda1b3-0170-4510-9616-1bce561d7aa7"),
            "NFL": ("americanfootball", "007d7c61-07a7-4e18-bb40-15104b6eac92"),
            "Champions_League": ('football', "9749ba20-1aa4-4f10-8014-af726d2c6fc5"),
            "SerieA": ('football', "e36a8c54-4451-418e-9842-118d8523a421"),
            "LaLiga": ("football", "3b0cbb27-7923-47a9-8117-bcec252162d2"),
            "Ligue1": ("football", "c06e1f2d-c704-47f7-9949-63c536db2b7d"),
            "MLS": ("football", "51ad2d73-982e-4dd0-ae99-f786cd5bd7d5"),
            "Bundesliga": ("football", "5cda9342-5f65-4483-af53-4a7b786832e2")
        }

    def get_odds(self, league):
        ext, comp_id = self.league_exts.get(league)
        if not comp_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self.league_exts.keys())}")

        url = f"https://www.williamhill.com/us/il/bet/api/v3/sports/{ext}/events/schedule"

        querystring = {"competitionIds": comp_id}

        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}

        response = requests.request(
            "GET", url, headers=headers, params=querystring)
        if response.status_code != 200:
            return pd.DataFrame()
        data = json.loads(response.text)
        events = data.get('competitions', False)
        if not events:
            return pd.DataFrame()
        events = events[0]
        events = events.get('events')
        entries = []
        for event in events:
            event_name = event.get("name").split("| |")
            away_team = event_name[0].strip("|")
            home_team = event_name[-1].strip("|")
            date = event.get("startTime")
            date = parser.parse(date)
            tz = pytz.timezone('US/Central')
            date = date.astimezone(tz)
            date = date.replace(tzinfo=None)
            markets = event.get('markets')
            for mkt in markets:
                if mkt.get("name") not in ["|Money Line|", "|90 Minutes|"]:
                    continue
                for line in mkt.get("selections"):
                    odds = line.get("price").get('d')
                    try:
                        odds_team = line.get("teamData").get("teamName")
                    except AttributeError:
                        odds_team = line.get("name").strip("|")
                    entry = {
                        "date": date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "odds_team": odds_team,
                        "odds": odds
                    }
                    entries.append(entry)
        return pd.DataFrame(entries)
