import pandas as pd
import numpy as np
from OddsPortal import OddsPortal
from DiscordAlerts import DiscordAlert
from DraftKings import DraftKings
from BetRivers import BetRivers
from BarstoolSB import Barstool
from PointsBet import PointsBet
from Caesars import Caesars
from FanDuel import FanDuel
from BetMGM import BetMGM
import fuzzy_pandas as fpd
import os
from dotenv import load_dotenv
from pybettor import convert_odds
from utils import *
import requests
import pickle
from pyvirtualdisplay import Display
import traceback
from datetime import datetime, timedelta


if sys.platform == "linux":
    display = Display(visible=0, size=(800, 600))
    display.start()

SPORTS = {
    "NBA": True,
    "NFL": True,
    "MLB": False,
    "NHL": True,
    "NCAAB": True,
    "NCAAF": False,
    "EPL": True,
    "LaLiga": True,
    "SerieA": True,
    "Champions_League": False,
    "Ligue1":   True,
    "Bundesliga": True,
    "MLS": False
}


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
        self.trades_path = "trades.csv"
        self.initial_bankroll = 500
        self.discord = DiscordAlert()
        self.books = [DraftKings(), Barstool(), PointsBet(),
                      Caesars(), BetRivers(), FanDuel(), BetMGM()]
        self.valid_lines = 0
        try:
            self.odds_portal = OddsPortal()
        except Exception as e:
            self.discord.send_error(
                "Error instantiating OddsPortal instance: " + str(e))
            raise e
        self._alpha = 0.05
        try:
            self.model = pickle.load(open('model.pkl', 'rb'))
        except Exception as e:
            self.discord.send_error(
                "Error Loading Model: " + str(traceback.format_exc()))
            raise e

    def get_current_mean_odds(self, sport) -> pd.DataFrame:
        """
        Scrapes OddsPortal for upcoming games for a given league and returns a DataFrame
        ready for merging with the current best odds

        Args:
            sport (str): league of interest

        Returns: DataFrame ready to merge with the current best odds
        """
        op_df = self.odds_portal.get_odds(sport, abridged=True)
        if op_df.empty:
            return pd.DataFrame()
        op_df = op_df.reset_index()
        pivoted = op_df.set_index(['date', "home_team", "away_team"])[
            ['home_odds', 'draw_odds', 'away_odds']].stack()
        pivoted = pd.DataFrame(pivoted, columns=['highest_odds'])
        pivoted = pivoted.reset_index()
        pivoted.columns = ['date', 'home_team',
                           'away_team', 'odds_type', 'mean_odds']
        pivoted['odds_team'] = np.where(
            pivoted['odds_type'] == 'home_odds', pivoted['home_team'], np.nan)
        pivoted['odds_team'] = np.where(
            pivoted['odds_type'] == 'away_odds', pivoted['away_team'], pivoted['odds_team'])
        pivoted['odds_team'] = np.where(
            pivoted['odds_type'] == 'draw_odds', "draw", pivoted['odds_team'])

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
        all_lines = self.get_sportsbook_lines(league)
        if all_lines.empty:
            print("Empty Odds df")
            return pd.DataFrame()
        mean_odds = self.get_current_mean_odds(league)
        if mean_odds.empty:
            print("Empty mean Odds df")
            return pd.DataFrame()
        all_lines = all_lines.dropna()
        mean_odds = mean_odds.dropna()
        df = fpd.fuzzy_merge(mean_odds, all_lines,
                             on=['home_team', "away_team", "odds_team"],
                             ignore_case=True,
                             keep_left=['date', 'home_team',
                                        'away_team', 'odds_team', 'mean_odds'],
                             keep_right=['bookmaker', 'odds'],
                             method="levenshtein",
                             join="inner",
                             threshold=0.85)
        return df

    def get_sportsbook_lines(self, league) -> pd.DataFrame:
        """
        Uses custom built scrapers to gather live odds for each sportsbook directly

        Args:
            league (str): the name of the league want odds for

        Returns:
            pd.DataFrame: dataframe with the following columns
            ['date', 'home_team', 'away_team', "odds_team", 'odds', "bookmaker"]
        """
        dfs = []
        for book in self.books:
            try:
                df = book.get_odds(league)
            except:
                print(f"Error with {book.name}")
                continue
            df['bookmaker'] = book.name
            if not df.empty:
                dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        books_odds = pd.concat(dfs)
        # books_odds = books_odds[books_odds['date'] <=
        #                         (central_time_now() + timedelta(days=1))]
        return books_odds

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
        df = df[df['date'] < central_time_now() + pd.Timedelta(3, 'h')]
        print(f"Checking {len(df)} lines within window")
        self.valid_lines += len(df)
        df['mean_implied_probability'] = 1 / df['mean_odds']
        df['highest_implied_probability'] = 1 / df['odds']
        df['thresh'] = 1 / (df['mean_implied_probability'] - self._alpha)
        df = df[df['odds'] >= df['thresh']]
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
        df['american_thresh'] = df['american_thresh'].round()
        df['american_odds_best'] = df['odds'].apply(
            lambda x: convert_odds(x, cat_in="dec")['American'])
        df['american_odds_best'] = df['american_odds_best'].round()

        implied = df['mean_implied_probability']
        implied.name = 'implied_probability'
        df['predicted_prob'] = self.model.predict(implied)
        df['kelly'] = df.apply(lambda x: basic_kelly_criterion(
            x['predicted_prob'], x['odds']), axis=1)
        df['half_kelly'] = df.apply(lambda x: basic_kelly_criterion(
            x['predicted_prob'], x['odds'], kelly_size=0.5), axis=1)

        df['id'] = df.apply(lambda x: self.generate_game_id(x), axis=1)
        current_bankroll = self.current_bankroll()
        df['cja_wager'] = df['kelly'] * current_bankroll
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
        for i, row in df.iterrows():
            date = row['date'].strftime("%m/%d %I:%M %p")
            american_thresh = "+" + \
                str(row['american_thresh']) if row['american_thresh'] > 0 else str(
                    row['american_thresh'])
            american_best = "+" + \
                str(row['american_odds_best']) if row['american_odds_best'] > 0 else str(
                    row['american_odds_best'])
            msg = f"{date} {row['away_team']} @ {row['home_team']}\n"
            msg += f"Bet on {row['odds_team']} Moneyline with {row['bookmaker']}\n"
            msg += f"Current Odds: {row['odds']} ({american_best}).\n"
            msg += f"The bet is good if the odds are at least {round(row['thresh'], 2)} ({american_thresh})\n"
            msg += "Using Kelly Criterion, we reccommend betting the following percentages of your betting bankroll: \n"
            msg += f"Full Kelly: {round(row['kelly'] * 100)}%\n"
            msg += f"Half Kelly: {round(row['half_kelly'] * 100)}%\n"
            msg += "\n\n"
            self.discord.send_msg(msg)

        self.discord.send_msg("Good Luck!!")

    def create_and_send_notification_cja(self, df) -> str:
        """
        sends message specific to CJA for bet placement

        Args:
            df (pd.DataFrame): DataFrame of trades that have all the necessary
            calculations completed already

        Returns:
            str: The output string to send to the discord
        """
        if df.empty:
            return
        current_bankroll = self.initial_bankroll
        intro_msg = ":rotating_light::rotating_light: Potential Bets Found! :rotating_light::rotating_light:\n"
        intro_msg += f"You current bankroll is {current_bankroll}.\n"
        self.discord.send_msg_cja(intro_msg)
        for i, row in df.iterrows():
            date = row['date'].strftime("%m/%d %I:%M %p")
            american_thresh = "+" + \
                str(row['american_thresh']) if row['american_thresh'] > 0 else str(
                    row['american_thresh'])
            american_best = "+" + \
                str(row['american_odds_best']) if row['american_odds_best'] > 0 else str(
                    row['american_odds_best'])
            msg = f"{date} {row['away_team']}@{row['home_team']}\n"
            msg += f"ID: {row['id']} \n"
            msg += f"Bet on {row['odds_team']} Moneyline with {row['bookmaker']}\n"
            msg += f"Current Odds: {row['odds']} {american_best}.\n"
            msg += f"Lowest Profitable Odds: {round(row['thresh'], 2)} {american_thresh}\n"
            msg += f"Kelly wager size: ${round(row['cja_wager'], 2)} \n"
            msg += "\n\n"
            self.discord.send_msg_cja(msg)

        self.discord.send_msg_cja("Good Luck!!")

    def current_bankroll(self) -> float:
        if not os.path.exists(self.trades_path):
            return self.initial_bankroll
        else:
            trades = pd.read_csv(self.trades_path)
            trades = trades[trades['cja_placed_bet'] == 1]
            trades = trades.dropna()
            bankroll = self.initial_bankroll
            for i, row in trades.iterrows():
                bankroll += (row['cja_wager'] * row['bet_wins']
                             * row['odds']) - row['cja_wager']
            return bankroll

    def generate_game_id(self, row) -> str:
        """
        When provided with a row in the df pertaining to a given game, returns a 
        game code unique to that game

        Args:
            row (pd.Series) a row pertaining to a certain game
        Return: a string identifier unique to the game 
        """
        home_first_initial = row['home_team'][0].lower()
        away_first_initial = row['away_team'][0].lower()
        line_first_initial = row['odds_team'][0].lower()
        month = str(row['date'].month)
        day = str(row['date'].day)
        hour = str(row['date'].hour)
        minute = str(row['date'].minute)
        return home_first_initial + month + day + hour + minute + away_first_initial + line_first_initial

    def save_spotted_trades(self, df) -> None:
        """
        Saves trades that have been spotted by the Engine into a .csv file

        Args:
            df (pd.DataFrame): DataFrame of observed trades
        """

        df = df.set_index("id")
        df['bet_wins'] = np.nan
        df['cja_placed_bet'] = np.nan
        if not os.path.exists(self.trades_path):
            df.to_csv(self.trades_path)
        else:
            trades = pd.read_csv(self.trades_path, index_col="id")
            df = pd.concat([trades, df])
            df.to_csv(self.trades_path)

    def remove_already_spotted_trades(self, df):
        """
        Temporary! Removes trades that have already been notified on the discord

        Args:
            df (pd.DataFrame): DataFrame of all trades that have been spotted

        Returns: filtered dataframe
        """
        if not os.path.exists(self.trades_path):
            return df
        else:
            trades = pd.read_csv(self.trades_path, index_col='id')
            already_sent = df[~df['id'].isin(trades.index)]
            for row in already_sent.itterrows():
                self.discord.send_msg_cja(
                    f"{row['id']} is still a good bet on {row['bookmaker']} as long as odds are > {round(row['american_thresh'])}")
            return df[~df['id'].isin(trades.index)]

    def run_engine(self) -> None:
        """
        Driving method for the scraping and notification engine
        Collects all necessary data from The OddsApi and OddsPortal,
        cleans the data, finds any relevant trades, and sends a notification on 
        Discord notifying users of the opportunities
        """
        # self.discord.send_error("Searching for Odds...")
        time = central_time_now()
        print(time.strftime("%m/%d/%Y %H:%M"))
        all_trades = []
        num_lines_scraped = 0
        error_occured = False
        for sport, in_season in SPORTS.items():
            if not in_season:
                continue
            print(f"Scraping odds for {sport}")
            try:
                league_df = self.create_league_df(sport)
                if league_df.empty:
                    print(f"No lines available for {sport}")
                    continue
                num_lines_scraped += len(league_df)

            except Exception as e:
                error = f"Error creating {sport} df\n" + \
                    str(traceback.format_exc())
                self.discord.send_error(error)
                continue
            try:
                print(f"Gathered lines for {sport}!")
                trades_df = self.find_trades(league_df)
            except Exception as e:
                e = str(e)
                error_msg = self.discord.construct_error_msg(
                    f"Error finding trades for {sport} \n" + e, "CRITICAL")
                self.discord.send_error(error_msg)

                continue
            if not trades_df.empty:
                print(f"Trades found in {sport}")
                all_trades.append(trades_df)
        if all_trades:
            print("Trades spotted! Sending all info")
            df = pd.concat(all_trades)
            df = self.necessary_calculations(df)
            df = self.remove_already_spotted_trades(df)
            self.create_and_send_notification(df)
            self.create_and_send_notification_cja(df)
            self.save_spotted_trades(df)
        # self.discord.send_error(
        #     f"Engine completed, analyzed {self.valid_lines} lines")
        self.odds_portal.exit()
        for scraper in self.books:
            if scraper.name == "BetMGM":
                scraper.web.quit()
