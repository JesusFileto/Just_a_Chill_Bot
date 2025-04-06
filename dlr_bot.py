from computebot import Compute
import polars as pl
import numpy as np
from scipy.stats import norm
import asyncio
import time

class DLRBot(Compute):
    """
    DLR Bot for calculating fair value based on signature process
    """
    
    def __init__(self, parent_client=None):
        super().__init__(parent_client)
        
        # Model parameters
        self.S0 = 5000  # Initial signature count
        self.alpha = 1.0630449594499  # Growth factor
        self.log_alpha = np.log(self.alpha)  # Log growth factor
        self.sigma = 0.006  # Volatility
        self.S_star = 100000  # Success threshold
        self.log_S_star = np.log(self.S_star)
        self.T = 50  # Total time periods (deadline)
        self.update_counter = 0
        
        # Current signature count
        self.current_signatures = self.S0
        self.fair_value = None
        
        # Track signature updates
        self.signature_history = []
        
    def calc_fair_value(self):
        """
        Calculate the fair value of DLR based on the lognormal signature process
        
        Returns:
            float: Fair value of DLR
        """
        
        #current_time = int(time.time()) - self.parent_client.start_time
        # Calculate time remaining
        
        #print("current time: ", current_time)
        time_remaining = self.T - self.update_counter
        
        if time_remaining <= 0:
            # If we're past the deadline, the value is either 100 or 0
            return 100 if self.current_signatures >= self.S_star else 0
        
        # Calculate the probability of success
        # Using the formula: p_t = 1 - Φ((ln(S*) - ln(S_t) - ln(α)(T-t)) / (σ√(T-t)))
        
        # Calculate the numerator of the z-score
        numerator = self.log_S_star - np.log(self.current_signatures) - time_remaining * self.log_alpha
        
        print("numerator: ", numerator)
        print("self.S_star: ", self.log_S_star)
        print("self.current_signatures: ", np.log(self.current_signatures))
        print("self.log_alpha: ", time_remaining * self.log_alpha)
        print("time_remaining: ", time_remaining)
        print("self.sigma: ", self.sigma)
        print("log ", np.log(self.current_signatures) - self.log_alpha * time_remaining)
        
        # Calculate the denominator of the z-score
        denominator = self.sigma * np.sqrt(time_remaining)
        
        print("denominator: ", denominator)
        
        # Calculate the z-score
        z_score = numerator / denominator
        
        print("z_score: ", z_score)
        
        # Calculate the probability of success
        
        prob_success = 1 - norm.cdf(z_score)
        
        print("prob_success: ", prob_success)
        
        # Calculate the fair value (binary option pays $100 if successful)
        #
        fair_value = 100 * prob_success
        
        return fair_value
    
    def calc_bid_ask_spread(self):
        """
        Calculate the bid-ask spread for DLR
        
        Returns:
            tuple: (bid_price, ask_price)
        """
        fair_value = self.calc_fair_value()
        
        # Set a reasonable spread around the fair value
        # For a binary option, we might want a tighter spread
        spread = 0.5  # 50 cents spread
        
        bid_price = max(0, fair_value - spread/2)
        ask_price = min(100, fair_value + spread/2)
        
        return bid_price, ask_price
    
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
    
    
    def increment_trade(self):
        """
        Increment the trade counter
        """
        # For DLR, we might want to update our model based on trades
        pass
    
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