from computebot import Compute
import polars as pl
import numpy as np
import math 
import time

class APTBot(Compute):
    """Specialized bot for APT symbol"""
    
    def __init__(self, parent_client=None):
        
        super().__init__(parent_client)
        print(parent_client)
        self.symbol = "APT"
        self.earnings = None
        self.fair_value = None
        self.spread = None 
        self.pe_ratio = 10 # for practice rounds
        self.S0 = None 
        self.deltaBid = None 
        self.deltaAsk = None
        self.lambda_bid = None
        self.lambda_ask = None
        
    def calc_bid_ask_spread(self):
        
        book = self.parent_client.order_books["APT"]
        best_bid = max(book.bids.keys()) if book.bids else None 
        best_ask = min(book.asks.keys()) if book.asks else None 

        if best_bid is None or best_ask is None: 
            return None 
        
        self.spread = best_ask - best_bid 

    def calc_bid_ask_price(self, symbol=None, df=None):
        """Override with APT-specific spread calculation"""
        bid_price = self.calc_reservation_price() - (self.spread / 2)
        ask_price = self.calc_reservation_price() + (self.spread / 2)
        return bid_price, ask_price
        
    def calc_reservation_price(self, symbol=None, df=None):
        """We use the avellinda stoikov model to calculate the reservation price for 
        interday trading p_{\text{mm}} = s - q \cdot \gamma \sigma^2 (T - t)"""
        self.A = 0.05 
        self.sigma = self.S0 * 0.02 / math.sqrt(1)
        self.k = math.log(2) / 0.01
        self.q_tilde = 100 
        self.gamma = 0.01 / self.q_tilde
        self.n_steps = int(self.T)
        self.n_paths = 500 
        self.h = 15 * 60 

        book = super().client.order_books["APT"]
        positions = super().client.positions["APT"]

        self.deltaBid = max(0.5 * self.gamma * self.sigma ** 2 * self.T + 1 / self.gamma * math.log(
            1 + self.gamma / self.k) + self.gamma * self.sigma ** 2 * self.T * positions, 0)
        self.deltaAsk = max(0.5 * self.gamma * self.sigma ** 2 * self.T + 1 / self.gamma * math.log(
            1 + self.gamma / self.k) - self.gamma * self.sigma ** 2 * self.T * positions, 0)
        
        self.lambda_bid = self.A * math.exp(-self.k * self.delta_bid)
        self.lambda_ask = self.A * math.exp(-self.k * self.delta_ask)
    
    def calc_fair_value(self):
        if self.earnings is None: 
            return 
        self.fair_value = self.earnings / self.pe_ratio 

    def handle_earnings_update(self, earnings):  
        self.earnings = earnings 
        self.calc_fair_value()
        

    def get_fair_value(self): 
        return self.fair_value 