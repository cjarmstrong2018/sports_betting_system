import pandas as pd
import requests
import numpy as np
import json
from dateutil import parser
import pytz
from utils import normalize_teams


class DraftKings(object):
    def __init__(self) -> None:
        self.name = "DraftKings"
        self.league_ids = {
            "NBA": 42648,
            "MLB": 84240,
            "NFL": 88808,
            "NCAAF": 87637,
            "NCAAB": 92483,
            "NHL": 42133,
            "EPL": 40253,
            "LaLiga": 40031,
            "SerieA": 40030,
            "Champions_League": 40685,
            "Ligue1": 40032,
            "MLS": 89345
        }

    def get_odds(self, league):
        league_id = self.league_ids.get(league, False)
        if not league_id:
            raise ValueError(
                f"League is invalid, it must be one of {' '.join(self.league_ids.keys())}")
        url = f"https://sportsbook-us-nh.draftkings.com//sites/US-NH-SB/api/v5/eventgroups/{league_id}"

        querystring = {"format": "json"}

        payload = ""
        headers = {"cookie": "hgg=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2aWQiOiIyNzEyMTgwMjgyMSIsImRrZS02MCI6IjI4NSIsImRraC0xMjYiOiI4MzNUZF9ZSiIsImRrZS0xMjYiOiIwIiwiZGtlLTE0NCI6IjQzMSIsImRrZS0xNDkiOiIzMzczIiwiZGtlLTE1MCI6IjU2NyIsImRrZS0xNTEiOiI0NTciLCJka2UtMTUyIjoiNDU4IiwiZGtlLTE1MyI6IjQ1OSIsImRrZS0xNTQiOiI0NjAiLCJka2UtMTU1IjoiNDYxIiwiZGtlLTE1NiI6IjQ2MiIsImRrZS0xNzkiOiI1NjkiLCJka2UtMjA0IjoiNzEwIiwiZGtlLTIxOSI6IjIyNDYiLCJka2gtMjI5IjoiSWxOaEMwNlMiLCJka2UtMjI5IjoiMCIsImRrZS0yMzAiOiI4NTciLCJka2UtMjg4IjoiMTEyOCIsImRrZS0zMDAiOiIxMTg4IiwiZGtlLTMxOCI6IjEyNjAiLCJka2UtMzQ1IjoiMTM1MyIsImRrZS0zNDYiOiIxMzU2IiwiZGtoLTM5NCI6IkNmWEFoemRPIiwiZGtlLTM5NCI6IjAiLCJka2gtNDA4IjoiWWRhVlJtRFoiLCJka2UtNDA4IjoiMCIsImRrZS00MTYiOiIxNjQ5IiwiZGtlLTQxOCI6IjE2NTEiLCJka2UtNDE5IjoiMTY1MiIsImRrZS00MjAiOiIxNjUzIiwiZGtlLTQyMSI6IjE2NTQiLCJka2UtNDIyIjoiMTY1NSIsImRrZS00MjkiOiIxNzA1IiwiZGtlLTcwMCI6IjI5OTIiLCJka2UtNzM5IjoiMzE0MCIsImRrZS03NTciOiIzMjEyIiwiZGtoLTc2OCI6IlVaR2Mwckh4IiwiZGtlLTc2OCI6IjAiLCJka2UtNzkwIjoiMzM0OCIsImRrZS03OTQiOiIzMzY0IiwiZGtlLTgwNCI6IjM0MTEiLCJka2UtODA2IjoiMzQyNiIsImRrZS04MDciOiIzNDM3IiwiZGtlLTgyNCI6IjM1MTEiLCJka2UtODI1IjoiMzUxNCIsImRrZS04MzQiOiIzNTU3IiwiZGtlLTgzNiI6IjM1NzAiLCJka2UtODY1IjoiMzY5NSIsImRraC04OTUiOiIyejBEWVNDMiIsImRrZS04OTUiOiIwIiwiZGtlLTkwMyI6IjM4NDgiLCJka2UtOTE3IjoiMzkxMyIsImRrZS05MzgiOiI0MDA0IiwiZGtlLTk0NyI6IjQwNDIiLCJka2UtOTc2IjoiNDE3MSIsImRrZS0xMDgxIjoiNDU4NyIsImRrZS0xMTA0IjoiNDY3NiIsImRrZS0xMTI0IjoiNDc2NCIsImRrZS0xMTcyIjoiNDk2NCIsImRrZS0xMTczIjoiNDk2NyIsImRrZS0xMTc0IjoiNDk3MCIsImRrZS0xMTg3IjoiNTAxNCIsImRrZS0xMjEwIjoiNTEyNyIsImRrZS0xMjEzIjoiNTE0MiIsImRrZS0xMjMxIjoiNTIxMyIsImRrZS0xMjQ0IjoiNTI2NyIsImRrZS0xMjU1IjoiNTMyNiIsImRrZS0xMjU5IjoiNTMzOSIsImRrZS0xMjc3IjoiNTQxMSIsImRrZS0xMjk1IjoiNTQ5NSIsImRrZS0xMjk5IjoiNTUwOSIsImRraC0xMzAzIjoiLUxrekFyZUsiLCJka2UtMTMwMyI6IjAiLCJka2gtMTMwNCI6IkFCSDhqM1hUIiwiZGtlLTEzMDQiOiIwIiwiZGtoLTEzMDciOiJ4UTBTSHNZOCIsImRrZS0xMzA3IjoiMCIsImRrZS0xMzIzIjoiNTYyNiIsImRrZS0xMzI3IjoiNTY1MCIsImRrZS0xMzI4IjoiNTY1MyIsImRraC0xMzMyIjoiTmlzTl9NVTciLCJka2UtMTMzMiI6IjAiLCJka2UtMTMzNiI6IjU2OTgiLCJka2UtMTMzOCI6IjU3MDkiLCJka2UtMTMzOSI6IjU3MTUiLCJka2UtMTM0MSI6IjU3MjQiLCJka2UtMTM0OCI6IjU3NjIiLCJka2UtMTM1MCI6IjU3NjciLCJka2UtMTM1MSI6IjU3NzIiLCJka2UtMTM1MiI6IjU3NzciLCJka2UtMTM1OCI6IjU4MTAiLCJka2gtMTM1OSI6IkFpZktzVWJKIiwiZGtlLTEzNTkiOiIwIiwiZGtlLTEzNjAiOiI1ODE5IiwiZGtlLTEzNjEiOiI1ODIzIiwibmJmIjoxNjczOTI1NDE3LCJleHAiOjE2NzM5MjU3MTcsImlhdCI6MTY3MzkyNTQxNywiaXNzIjoiZGsifQ.o_AoX8vvpJEhDqMeW-WEdi20f72oFAq8Sse96gxMN-w; STE=%222023-01-17T03%3A47%3A08.4605305Z%22; _abck=FB5C13E24E2C21857969D001A34357D1~-1~YAAQkXFAF74V%2BySFAQAATu26vQnOPHO9ChYZbGpFvrbInxkGkYC0dvPNrpeW7W3Xf8YNIzcONEtegOC%2BpZ8SYl3U0cSgAfVs%2FM2kDz%2Ff1Vg5rytiya5NMdLYEUmgPblV26K2gCSttuMvN3ls3C%2F6IynChCZzMui0xMUSzPUHOv4R%2BsmRU%2BDXrjNhtW7JWLRuOj4s78PRF1KKf7hjXaR3%2FpEFtOgt7qyTfg9h1zTlXsVmEUFrRIUfqC9%2Bue%2BHLK00qum9uFWR2Wd7WpVo0N959%2Ft%2BlfYnjDlQVf7ea2HBFnZbAXK%2BGOq1Z0p%2FoYxjw1sRy6bnKIUY%2BJZwTDJZPRd9LsLz9Vd5XgYodgmcG5sc5ERsYA2k%2FgMNibomEliaoznMe6sTONaFpCgZyOcqz54on2rfJzxRxXflVss%3D~-1~-1~-1; _csrf=adaf5aef-05f7-4ec8-9938-95b158daac38; STIDN=eyJDIjoxMjIzNTQ4NTIzLCJTIjo0MzkyOTQ3MTEzMiwiU1MiOjQ2MjQ2NDQwNDg5LCJWIjoyNzEyMTgwMjgyMSwiTCI6MSwiRSI6IjIwMjMtMDEtMTdUMDM6NDY6NTcuMTgxMjc2N1oiLCJTRSI6IlVTLURLIiwiVUEiOiJDK3hhOGt1Q1grRFMwdnd0SXJSbUlnQ2Y2MW1mY1lqT1JIM2d6Zko3ZUVnPSIsIkRLIjoiNjYxMTI2ZTMtNTliMS00ZTM0LWJlMTQtNzYyZmEwNGRkYjExIiwiREkiOiIwYWQwN2E1My0zMDBhLTRkYjctYjBkZi02MzA1ZjIwZGMwYmEiLCJERCI6MjQyMjk5NTMxNDR9; STH=143046cd29c443b538dc533c2a78ccebe6993469a934318b58be98289867ef03"}

        response = requests.request(
            "GET", url, data=payload, headers=headers, params=querystring)

        data = json.loads(response.text)
        odds_data = data['eventGroup']['offerCategories']
        odds_data = [x for x in odds_data if x['name'] == "Game Lines"][0]
        odds_data = odds_data["offerSubcategoryDescriptors"][0]
        odds_data = odds_data['offerSubcategory']['offers']
        line_entries = []
        for offer in odds_data:
            odds = [x for x in offer if x.get('label', "") == "Moneyline"][0]
            event_id = odds['eventId']
            outcomes = odds['outcomes']
            for line in outcomes:
                odds_team = line['label']
                odds = line['oddsDecimal']
                entry = {
                    "event_id": event_id,
                    "odds_team": odds_team,
                    'odds': odds
                }
                line_entries.append(entry)
        lines = pd.DataFrame(line_entries)
        event_data = data['eventGroup']['events']
        event_entries = []
        for event in event_data:
            if event.get("eventStatus").get('state') != "NOT_STARTED":
                continue
            event_id = event.get("eventId")
            home_team = event.get("teamName2")
            away_team = event.get("teamName1")
            date = event.get("startDate")
            date = parser.parse(date)
            tz = pytz.timezone('US/Central')
            date = date.astimezone(tz)
            date = date.replace(tzinfo=None)
            entry = {
                "event_id": event_id,
                "date": date,
                "home_team": home_team,
                "away_team": away_team,
            }
            event_entries.append(entry)
        events = pd.DataFrame(event_entries)

        df = events.merge(lines, how="inner", on="event_id")
        df = df.drop("event_id", axis=1)
        if league in ['NFL', "NBA", "MLB", "NHL"]:
            df['home_team'] = normalize_teams(df['home_team'], league)
            df['away_team'] = normalize_teams(df['away_team'], league)
            df['odds_team'] = normalize_teams(df['odds_team'], league)
        return df
