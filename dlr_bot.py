from computebot import Compute
import polars as pl
import numpy as np
from scipy.stats import norm
import asyncio
import time
import math
from utcxchangelib import xchange_client

class DLRBot(Compute):
    """
    DLR Bot for calculating fair value based on signature process
    """
    
    def __init__(self, parent_client=None):
        super().__init__(parent_client)
        
        # Model parameters
        self.S0_sig = 5000  # Initial signature count
        self.alpha = 1.0630449594499  # Growth factor
        self.log_alpha = np.log(self.alpha)  # Log growth factor
        self.sigma_sig = 0.006  # Volatility
        self.S_star = 100000  # Success threshold
        self.log_S_star = np.log(self.S_star)
        self.T_sig = 50  # Total time periods (deadline)
        self.update_counter = 0
        self.symbol = "DLR"
        
        
        # Current signature count
        self.current_signatures = self.S0_sig
        self.fair_value = 50
        
        self.spread = None 
        self.trade_count = 0
        self.trading_frequency = 20
        # Avellaneda–Stoikov parameters 
        self.T = 15 * 60 # 15 minute horizon 
        self.S0 = 50
        self.deltaBid = None 
        self.deltaAsk = None 
        self.sigma = None
        self.A = 0.05 
        self.k = math.log(2) / 0.01
        self.q_tilde = 10 
        self.gamma = 0.1 / self.q_tilde
        self.n_steps = int(self.T)
        self.n_paths = 500 
        
        # Track signature updates
        self.signature_history = []
        
    def monte_carlo_vectorized(self, current_signatures, rounds_remaining, num_simulations):
        # Initialize an array for the signature counts in each simulation.
        signatures = np.full(num_simulations, current_signatures, dtype=float)
        
        for _ in range(rounds_remaining):
            # Compute parameters for the lognormal distribution for each simulation
            mu_vals = np.log(signatures) + self.log_alpha
            #print("mu_vals: ", mu_vals)
            sigma_vals = np.full(num_simulations, self.sigma_sig)
            
            # Draw new signatures for each simulation (vectorized)
            signatures = np.random.lognormal(mean=mu_vals, sigma=sigma_vals)
        # Calculate payoffs: $100 if signature count reaches 100,000, else $0.
        payoffs = np.where(signatures >= 100000, 100, 0)
        fair_value = np.mean(payoffs)
        
        return fair_value
    
    def get_avellaneda_stoikov_params(self):
        return self.gamma, self.k, self.q_tilde, self.T, self.sigma
        
    def calc_fair_value(self):
        """
        Calculate the fair value of DLR based on the lognormal signature process
        
        Returns:
            float: Fair value of DLR
        """
        
        #current_time = int(time.time()) - self.parent_client.start_time
        # Calculate time remaining
        
        #print("current time: ", current_time)
    
        fair_value = self.monte_carlo_vectorized(self.current_signatures, self.T_sig - self.update_counter, 100000)
        print("monte_carlo_value: ", fair_value)
        print("update_counter: ", self.update_counter)
        print("T: ", self.T_sig)
        print("current_signatures: ", self.current_signatures)
        
        
        current_time = int(time.time()) - self.parent_client.start_time
        self._update_fair_value_timeseries(current_time, fair_value)
        return fair_value
        
    
    def _update_fair_value_timeseries(self, timestamp, fair_value):
        """
        Update the fair value timeseries with a new snapshot
        """
        new_row = pl.DataFrame([{
            "timestamp": timestamp,
            "fair_value": int(fair_value*100)
        }])
        self.parent_client.fair_value_timeseries["DLR"] = pl.concat([self.parent_client.fair_value_timeseries["DLR"], new_row])
    
    def calc_bid_ask_spread(self):
        return super().calc_bid_ask_spread(self.symbol)
        
    def calc_bid_ask_price(self, t=None):
        return super().calc_bid_ask_price(self.symbol, t)
    
    def calc_reservation_price(self, t, sigma, gamma):
        return super().calc_reservation_price(self.symbol, t, sigma, gamma)
    
    async def process_update(self, update):
        """
        Process updates from the parent client
        
        Args:
            update: The update to process
        """
        # This method would be called by the parent client's compute thread
        # It would process updates and calculate new fair values
        pass
    
    async def bot_handle_trade_msg(self, symbol, price, qty):
        """
        Handle trade messages
        
        Args:
            symbol: The trading symbol
            price: The trade price
            qty: The trade quantity
        """
    
    
    async def handle_trade(self):
        with self.parent_client._lock: 
            latest_timestamp = int(time.time()) - self.parent_client.start_time
            if latest_timestamp is None:
                return 
            print("type of latest_timestamp: ", type(latest_timestamp))
            bid_price, ask_price = self.calc_bid_ask_price(latest_timestamp)
        print("========================================")
        print("Adjusted Bid Price:", bid_price)
        await self.parent_client.place_order(self.symbol, self.q_tilde, xchange_client.Side.BUY, bid_price)
        print("Adjusted Ask Price:", ask_price)
        await self.parent_client.place_order(self.symbol, self.q_tilde, xchange_client.Side.SELL, ask_price)
        print("my positions:", self.parent_client.positions)
        
    
    def increment_trade(self):
        self.trade_count += 1
        if self.trade_count % self.trading_frequency == 0:
            asyncio.create_task(self.handle_trade())
            
    def get_fair_value(self):
        return self.fair_value
    
    def signature_update(self, new_signatures, cumulative):
        """
        Update the signature count
        
        Args:
            new_signatures: New signatures since last update
            cumulative: Cumulative signature count
        """
        # Update the current signature count
        self.current_signatures = cumulative
        current_time = int(time.time()) - self.parent_client.start_time
        # Add to history
        self.signature_history.append({
            "time": current_time,
            "signatures": cumulative
        })
        
        self.update_counter += 1
        
        # Calculate new fair value
        self.fair_value = self.calc_fair_value()
        print("DLR fair value: ", self.fair_value)
        
    
    def unstructured_update(self, news_data):
        """
        Handle unstructured news updates
        
        Args:
            news_data: The news data
        """
        # For DLR, we might want to update our model based on news
        pass
