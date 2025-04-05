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

        # Avellaneda–Stoikov parameters 
        self.T = 15 * 60 # 15 minute horizon 
        self.S0 = 100
        self.deltaBid = None 
        self.deltaAsk = None 
        self.A = 0.05 
        self.sigma = self.S0 * 0.02 
        self.k = math.log(2) / 0.01
        self.q_tilde = 100 
        self.gamma = 0.01 / self.q_tilde
        self.n_steps = int(self.T)
        self.n_paths = 500 
        
    def calc_bid_ask_spread(self):
        
        book = self.parent_client.order_books["APT"]
        best_bid = max(book.bids.keys()) if book.bids else None 
        best_ask = min(book.asks.keys()) if book.asks else None 

        if best_bid is None or best_ask is None: 
            return None 
        
        self.spread = best_ask - best_bid 
        return self.spread

    def calc_bid_ask_price(self, t):
        """Override with APT-specific spread calculation"""

        positions = super().client.positions["APT"]

        constant_term = (1 / self.gamma) * math.log(1 + self.gamma / self.k) 

        self.deltaBid = max(self.gamma * (self.sigma ** 2) * self.T * (positions + 0.5) + constant_term, 0)
        self.deltaAsk = max(-self.gamma * (self.sigma ** 2) * self.T * (positions - 0.5) + constant_term, 0)
    
        bid_price = self.calc_reservation_price(t) - self.deltaBid
        ask_price = self.calc_reservation_price(t) + self.deltaAsk
    
        return bid_price, ask_price
        
    def calc_reservation_price(self, t):
        """We use the avellinda stoikov model to calculate the reservation price for 
        interday trading p_{\ text{mm}} = s - q \cdot \gamma \sigma^2 (T - t)"""

        if self.S0 is None or self.S0 == 0: 
            self.S0 = self.get_fair_value() or 100.0 # fall back value
        dt = self.T - t # t is the current time stamp 

        positions = super().client.positions()

        reservation_price = self.S0 - positions * self.gamma * self.sigma**2 * dt

        return reservation_price
    
    def calc_fair_value(self):
        if self.earnings is None: 
            return 
        self.fair_value = self.earnings / self.pe_ratio 

    def handle_earnings_update(self, earnings):  
        self.earnings = earnings 
        self.calc_fair_value()
        
    def get_fair_value(self): 
        return self.fair_value 