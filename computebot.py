# imports 
import polars as pl
import numpy as np
import asyncio
from scipy.optimize import minimize
# super class for all compute bots
class Compute: 

    def __init__(self, parent_client=None, symbol=None): 
        self.parent_client = parent_client
        self.symbol = symbol
        self.omega = None
        self.alpha = None
        self.beta = None

    def calc_bid_ask_spread(): 
        pass

    def calc_fair_value(): 
        pass 
    
    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass
    
        
    def increment_trade(self):
        pass
        
    def unstructured_update(self, news_data):
        pass
    
    async def handle_trade(self):
        pass
    
    def calc_volatility(self):
        garch = self.garch_neg_loglik(self.omega, self.alpha, self.beta)
        self.fit_garch(garch)
        
    # GARCH for volatility 
    def garch_neg_loglik(self, params):
        # Unpack parameters
        omega, alpha, beta = params
        
        sigma2 = None
        with self.parent_client._lock:
            T = len(self.parent_client.stock_LOB_timeseries[self.symbol]["spread"])
            sigma2 = np.zeros(T)
            sigma2[0] = np.var(self.parent_client.stock_LOB_timeseries[self.symbol]["mid_price"])
        
        
        # Initialize negative log-likelihood
        neg_loglik = 0
        
        # Recursively compute the conditional variance and accumulate the log-likelihood
        for t in range(1, T):
            # GARCH-X recursion: add the exogenous variable impact at time t
            
            returns = self.parent_client.stock_LOB_timeseries[self.symbol]["mid_price"]
            
            sigma2[t] = omega + alpha * (returns[t-1]**2) + beta * sigma2[t-1]
            
            # To avoid numerical issues, ensure sigma2[t] is positive
            if sigma2[t] <= 0:
                return 1e6
            
            # Contribution to log-likelihood from observation t, assuming normality and zero mean
            neg_loglik += 0.5 * (np.log(2 * np.pi) + np.log(sigma2[t]) + (returns[t]**2 / sigma2[t]))
        self.sigma = sigma2[T]
        return neg_loglik

    def fit_garch(self, garch_neg_loglik):
        # Initial guess for parameters
        initial_guess = [0.01, 0.05, 0.94]
        bounds = [(1e-8, None), (0, 1), (0, 1), (None, None)]
        
        # Perform optimization
        result = minimize(garch_neg_loglik, initial_guess, bounds=bounds, method='Nelder-Mead')
        
        # Extract optimized parameters
        self.omega = result.x[0]
        self.alpha = result.x[1]
        self.beta = result.x[2]
        
    
    async def send_to_parent(self, message_type, data):
        """
        Send data back to the parent client
        """
        if self.parent_client:
            # Call appropriate method on parent client
            if message_type == "trade":
                asyncio.create_task(self.parent_client.bot_handle_trade_msg(
                    data.get("symbol"), 
                    data.get("price"), 
                    data.get("qty")
                ))
            elif message_type == "book_update":
                asyncio.create_task(self.parent_client.bot_handle_book_update(
                    data.get("symbol")
                ))
            # Add more message types as needed