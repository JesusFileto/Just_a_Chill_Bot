from computebot import Compute
import polars as pl
import numpy as np
import math 
import time
import asyncio
from utcxchangelib import xchange_client

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
        self.trade_count = 0
        self.trading_frequency = 20
        # Avellaneda–Stoikov parameters 
        self.T = 15 * 60 # 15 minute horizon 
        self.S0 = None
        self.deltaBid = None 
        self.deltaAsk = None 
        self.A = 0.05 
        self.sigma = None
        self.k = math.log(2) / 0.01
        self.q_tilde = 10 
        self.gamma = 0.01 / self.q_tilde
        self.n_steps = int(self.T)
        self.n_paths = 500 
        
    def calc_bid_ask_spread(self):
        
        book = self.parent_client.order_books["APT"]

        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        print("Sorted Bids:", sorted_bids)
        print("Sorted Asks:", sorted_asks)
    
        best_bid,_ = max(sorted_bids) if sorted_bids else None 
        print("best bid: ", best_bid)
        best_ask,_ = min(sorted_asks) if sorted_asks else None 
        print("best ask: ", best_ask)

        if best_bid is None or best_ask is None: 
            return None 
        
        self.spread = (best_ask - best_bid) / 100
        return self.spread

    def calc_bid_ask_price(self, t):
        """Override with APT-specific spread calculation"""

        positions = self.parent_client.positions.get("APT", 0)

        # print all variables 
        #print("gamma: ", self.gamma)
        #print("k: ", self.k)
        #print("T: ", self.T)
        #print("t:", t)
        #print("S0: ", self.S0)
        #print("A: ", self.A)
        #print("sigma: ", self.sigma)
        #print("q_tilde: ", self.q_tilde)
        #print("n_steps: ", self.n_steps)
        #print("n_paths: ", self.n_paths)
        #print("positions: ", self.parent_client.positions.get("APT", 0))

        constant_term = (1 / self.gamma) * math.log(1 + self.gamma / self.k) 

        print("constant term: ", constant_term)

        reservation_price = self.calc_reservation_price(t)

        self.deltaBid = max(self.gamma * (self.sigma ** 2) * self.T * (positions + 0.5) + constant_term, 0)
        self.deltaAsk = max(-self.gamma * (self.sigma ** 2) * self.T * (positions - 0.5) + constant_term, 0)

        delta_bid_price = int(reservation_price - self.deltaBid ) * 100 
        #print("delta ver. bid price: ", delta_bid_price )
        delta_ask_price = int(reservation_price + self.deltaAsk) * 100
        #print("delta ver. ask price: ", delta_ask_price )
        
        # using spread instead 
        spread = self.calc_bid_ask_spread()
        #print("spread: ", spread)
        bid_price = int((reservation_price - spread / 2) * 100)
        #print("spead ver. bid price: ", bid_price )
        ask_price = int((reservation_price + spread / 2) * 100)
        #print("spead ver. ask price: ", ask_price )

        return delta_bid_price, delta_ask_price
        
    def calc_reservation_price(self, t):
        """We use the avellinda stoikov model to calculate the reservation price for 
        interday trading p_{\ text{mm}} = s - q \cdot \gamma \sigma^2 (T - t)"""
        self.S0 = self.get_fair_value()
        self.sigma = self.S0 * 0.02 / math.sqrt(self.T)
        dt = self.T - t # t is the current time stamp 

        positions = self.parent_client.positions.get("APT", 0)

        reservation_price = self.S0 - positions * self.gamma * self.sigma**2 * dt
        print("reservation price: ", reservation_price) 
        return reservation_price
    
    def calc_fair_value(self):
        if self.earnings is None: 
            return 
        self.fair_value = self.earnings / self.pe_ratio

    def handle_earnings_update(self, earnings):  
        self.earnings = earnings 
        self.calc_fair_value()
        print("handling earnings: ", self.fair_value)
        
    def get_fair_value(self): 
        if self.earnings is None: 
            self.fair_value = 10 

        self.S0 = self.fair_value
        return self.fair_value 
    
    async def handle_trade(self):
        with self.parent_client._lock: 
            latest_timestamp = int(time.time()) - self.parent_client.start_time
            if latest_timestamp is None:
                return 
            print("type of latest_timestamp: ", type(latest_timestamp))
            bid_price, ask_price = self.calc_bid_ask_price(latest_timestamp)
        print("========================================")
        print("Adjusted Bid Price:", bid_price)
        await self.parent_client.place_order("APT",self.q_tilde, xchange_client.Side.BUY, bid_price)
        print("Adjusted Ask Price:", ask_price)
        await self.parent_client.place_order("APT",self.q_tilde, xchange_client.Side.SELL, ask_price)
        print("my positions:", self.parent_client.positions)
        
    
    def increment_trade(self):
        self.trade_count += 1
        if self.trade_count % self.trading_frequency == 0:
            asyncio.create_task(self.handle_trade())
            
            
    def unstructured_update(self, news_data):
        with self.parent_client._lock:
            self.parent_client.pnl_timeseries = self.parent_client.pnl_timeseries.with_columns(
                pl.when(pl.col("timestamp") == pl.col("timestamp").max())
                .then(1)
                .otherwise(pl.col("is_unstructured_news_event"))
                .alias("is_unstructured_news_event")
            )
    