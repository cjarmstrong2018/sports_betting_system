import pandas as pd
import numpy as np
from OddsPortal import OddsPortal
from DiscordAlerts import DiscordAlert
import fuzzy_pandas as fpd
import os
from dotenv import load_dotenv
from pybettor import convert_odds
from utils import *
import requests
import pickle
from pyvirtualdisplay import Display
if sys.platform == "linux":
    display = Display(visible=0, size=(800, 600))
    display.start()

REGIONS = 'us'  # uk | us | eu | au. Multiple can be specified if comma delimited
MARKETS = 'h2h'  # h2h | spreads | totals. Multiple can be specified if comma delimited
ODDS_FORMAT = 'decimal'  # decimal | american
DATE_FORMAT = 'iso'

SUPPORTED_BOOKS = ['DraftKings', 'FanDuel', 'Barstool Sportsbook',
                   'BetRivers', 'BetMGM', 'PointsBet (US)', 'William Hill (US)']

SPORTS = {
    "NBA": "basketball_nba",
    "NFL": "americanfootball_nfl",
    # "MLB": "TBD",
    "NHL": "icehockey_nhl",
    "NCAAB": "basketball_ncaab",
    "NCAAF": "americanfootball_ncaaf",
}

"""
To-do:
    - include Soccer
    - finish Kelly Testing
    - add processing of in-season sports
    - only return game within 1 hour of starting
    - Update errors
    - save trades in a df
    - don't send duplicate texts for games
"""


class BettingEngine(object):
    """
    The object that pulls all odds, performs calculations, and sends notifications
    on discord regarding + EV bets. The strategy was heavily inspired by the paper linked
    below:

    https://arxiv.org/ftp/arxiv/papers/1710/1710.02824.pdf
    """

    def __init__(self):
        """
        Constructor for the BettingEngine object
        """
        self.discord = DiscordAlert()
        try:
            self.odds_portal = OddsPortal()
        except Exception as e:
            self.discord.send_error(
                "Error instantiating OddsPortal instance: " + str(e))
        self.odds_api_key = os.getenv("API_KEY")
        self._alpha = 0.05
        try:
            self.model = pickle.load(open('model.pkl', 'rb'))
        except Exception as e:
            self.discord.send_error("Error Loading Model: " + str(e))

    def get_current_best_odds(self, sport):
        """
        Loads current odds for individual books and returns a DataFrame
        of the decimal odds for each event

        sport (str): the odds_api name for the sport of interest

        Return: DataFrame of the best odds and the respective bookie
        """
        odds_response = requests.get(
            f'https://api.the-odds-api.com/v4/sports/{sport}/odds',
            params={
                'api_key': self.odds_api_key,
                'regions': REGIONS,
                'markets': MARKETS,
                'oddsFormat': ODDS_FORMAT,
                'dateFormat': DATE_FORMAT,
            }
        )
        if odds_response.status_code != 200:
            error_msg = self.discord.construct_error_msg(odds_response.status_code,
                                                         "CRITICAL")
            self.discord.send_error(error_msg)
            return None
        odds_response = json.loads(odds_response.text)
        df = pd.json_normalize(odds_response,
                               record_path=['bookmakers',
                                            'markets', 'outcomes'],
                               meta=['sport_title', 'commence_time', 'home_team',
                                     'away_team', ['bookmaker', 'title']], errors='ignore')
        df = df[['commence_time', 'sport_title', 'home_team',
                 'away_team', 'bookmaker.title', 'name', 'price']]
        df = df.rename(columns={'commence_time': "date",
                                "sport_title": "sport",
                                "bookmaker.title": "bookmaker",
                                "name": "odds_team",
                                "price": "odds"})
        df = df[df['bookmaker'].isin(SUPPORTED_BOOKS)]
        df = df.reset_index(drop=True)
        highest_odds_idx = df.groupby(['sport', 'home_team', 'away_team', "odds_team"])[
            'odds'].idxmax()
        df = df.iloc[highest_odds_idx]
        return df

    def get_current_mean_odds(self, sport):
        """
        Scrapes OddsPortal for upcoming games for a given league and returns a DataFrame
        ready for merging with the current best odds

        Args:
            sport (str): league of interest

        Returns: DataFrame ready to merge with the current best odds
        """
        op_df = self.odds_portal.get_odds(sport)
        op_df = op_df.reset_index()
        pivoted = op_df.set_index(['date', "home_team", "away_team"])[
            ['home_odds', 'away_odds']].stack()
        pivoted = pd.DataFrame(pivoted, columns=['highest_odds'])
        pivoted = pivoted.reset_index()
        pivoted.columns = ['date', 'home_team',
                           'away_team', 'odds_type', 'mean_odds']
        pivoted['odds_team'] = np.where(
            pivoted['odds_type'] == 'home_odds', pivoted['home_team'], np.nan)
        pivoted['odds_team'] = np.where(
            pivoted['odds_type'] == 'away_odds', pivoted['away_team'], pivoted['odds_team'])

        return pivoted

    def create_league_df(self, league) -> pd.DataFrame:
        """
        Driver function to create a merged df of average and best odds for each
        team as well as the book on which it is hosted

        Args:
            league (str): the league of interest

        Returns:
            pd.DataFrame:merged DataFrame ready for analysis
        """
        sport_title = SPORTS.get(league, False)
        if not sport_title:
            error_msg = "Invalid LEAGUE! Cannot get DataFrame from OddsAPI"
            error_msg = self.discord.construct_error_msg(
                error_msg, "Low Priority")
            self.discord.send_error(error_msg)
        best_odds = self.get_current_best_odds(sport_title)
        if best_odds is None:
            return None
        mean_odds = self.get_current_mean_odds(league)
        df = fpd.fuzzy_merge(mean_odds, best_odds,
                             on=['home_team', "away_team", "odds_team"],
                             ignore_case=True,
                             keep_left=['date', 'home_team',
                                        'away_team', 'odds_team', 'mean_odds'],
                             keep_right=['sport', 'bookmaker', 'odds'],
                             method="levenshtein",
                             join="inner",
                             threshold=0.9)
        return df

    def find_trades(self, df) -> pd.DataFrame:
        """
        Runs the necessary calculations on a merged df of mean and highest
        odds and returns a DataFrame of all trades spotted

        Args:
            df (pd.DataFrame): DataFrame from self.create_league_format

        Returns:
            pd.DataFrame: a DataFrame ready to be iterated over to send alerts to
            the channel
        """
        df['mean_implied_probability'] = 1 / df['mean_odds']
        df['highest_implied_probability'] = 1 / df['odds']
        df['thresh'] = 1 / (df['mean_implied_probability'] - self.alpha)
        df = df[df['odds'] >= df['thresh']]
        if df.empty():
            self.discord.send_error(
                "Not an error, everything is working as planned, no trades though")
            return df
        return df

    def necessary_calculations(self, df) -> pd.DataFrame:
        """
        Performs necessary calculations before iterating through df to send 
        notifications

        Args:
            df (pd.DataFrame): DataFrame of identified trades

        Returns:
            pd.DataFrame: original df but with updated columns for notifications
        """
        df['american_thresh'] = df['thresh'].apply(
            lambda x: convert_odds(x, cat_in="dec")['American'])
        df['american_odds_best'] = df['odds'].apply(
            lambda x: convert_odds(x, cat_in="dec")['American'])

        # compute df['predicted_prob'] here using self.prediction_model
        implied = df['mean_implied_probability']
        implied.name = 'implied_probability'
        df['predicted_prob'] = self.model.predict(implied)
        df['kelly'] = df.apply(lambda x: basic_kelly_criterion(
            x['predicted_prob'], x['odds']), axis=1)
        df['half_kelly'] = df.apply(lambda x: basic_kelly_criterion(
            x['predicted_prob'], x['odds'], kelly_size=0.5), axis=1)

        return df

    def create_and_send_notification(self, df) -> str:
        """
        Generates the notification that will be sent to the discord server to
        notify of treades

        Args:
            df (pd.DataFrame): DataFrame of trades that have all the necessary
            calculations completed already

        Returns:
            str: The output string to send to the discord
        """
        if df.empty:
            return
        intro_msg = ":rotating_light::rotating_light: Potential Bets Found! :rotating_light::rotating_light:\n"
        self.discord.send_msg(intro_msg)
        for i, row in df.head().iloc[:2, :].iterrows():
            date = row['date'].strftime("%m/%d %I:%M")
            msg = f"{date} {row['away_team']} @ {row['home_team']}\n"
            msg += f"Bet on {row['odds_team']} Moneyline with {row['bookmaker']}\n"
            msg += f"Current Odds: {row['odds']}.\n"
            msg += f"We Reccommend this wager as long as odds are greater than {round(row['thresh'], 2)}\n"
            msg += "Using Kelly Criterion, we reccommend betting the following percentages of your betting bankroll: \n"
            msg += f"Full Kelly: {round(row['kelly'] * 100)}%\n"
            msg += f"Half Kelly: {round(row['half_kelly'] * 100)}%\n"
            msg += "\n\n"
            self.discord.send_msg(msg)

        self.discord.send_msg("Good Luck!!")

    def run_engine(self) -> None:
        """
        Driving method for the scraping and notification engine
        Collects all necessary data from The OddsApi and OddsPortal,
        cleans the data, finds any relevant trades, and sends a notification on 
        Discord notifying users of the opportunities
        """
        all_trades = []
        for sport, name in SPORTS.items():
            try:
                league_df = self.create_league_df(sport)
            except Exception as e:
                e = str(e)
                self.discord.construct_error_msg(
                    f"Error creating {sport} df \n" + e, "CRITICAL")
                continue
            try:
                trades_df = self.find_trades(league_df)
            except Exception as e:
                e = str(e)
                self.discord.construct_error_msg(
                    f"Error finding trades for {sport} \n" + e, "CRITICAL")
                continue
            if not trades_df.empty:
                all_trades.append(trades_df)

        if all_trades:
            df = pd.concat(all_trades)
            df = self.necessary_calculations(df)
            self.create_and_send_notification(df)
        else:
            self.discord.send_error("No Errors: engine ran smoothly")
