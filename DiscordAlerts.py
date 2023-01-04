import os
import requests
from dotenv import load_dotenv

load_dotenv()


class DiscordAlert(object):

    def __init__(self) -> None:
        self.errors_url = os.getenv('SYSTEM_ERRORS')
        self.notification_url = os.getenv("TRADES_WEBHOOK")

    def send_msg(self, msg):
        """
        Method to send a message to my discord bot
        Args:
            msg (str):message to send to discord channel 
        Returns:
            nothing, posts message to channel
        """
        data = {'content': msg}
        r = requests.post(url=self.notification_url, json=data)

    def send_error(self, error_msg):
        data = {'content': error_msg}
        r = requests.post(url=self.errors_url, json=data)

    def construct_error_msg(self, msg, level):
        """
        Method to format error messages for my notifications

        Args:
            msg (string): the data you want to send
            level (str): level indicating the danger of the Error
        """
        return f"ERROR LEVEL: {level} \n\n {msg} \n Please check systems as soon" + \
            "as possible!"
