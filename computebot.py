# imports 
import polars as pl
import numpy as np

# super class for all compute bots
class Compute: 
    def __init__(self): 
        pass

    def calc_bid_ask_spread(): 
        pass

    def calc_fair_value(): 
        pass 
    
    async def process_update(self, index):
        """
        Process an update for this bot.
        This is the main method called when new data is received.
        Each bot subclass should override this to implement specialized behavior.
        """
        pass