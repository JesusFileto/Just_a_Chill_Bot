# imports 
import polars as pl
import numpy as np
import asyncio
from scipy.optimize import minimize
import math
# super class for all compute bots
class Compute: 

    def __init__(self, parent_client=None, symbol=None): 
        self.parent_client = parent_client
        self.symbol = symbol
        self.omega = None
        self.alpha = None
        self.beta = None
        self.sigma = None
        self.gamma = None
        self.k = None
        self.q_tilde = None
        self.T = None
        self.S0 = None
        self.deltaBid = None
        self.deltaAsk = None
        self.A = None
        
    

    def calc_fair_value(): 
        pass 
    
    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass
    
    def calc_bid_ask_spread(self, symbol=None):
        
        book = self.parent_client.order_books[symbol]

        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        print("Sorted Bids:", sorted_bids)
        print("Sorted Asks:", sorted_asks)
    
        best_bid,_ = max(sorted_bids) if len(sorted_bids) > 0 else None 
        print("best bid: ", best_bid)
        best_ask,_ = min(sorted_asks) if len(sorted_asks) > 0 else None 
        print("best ask: ", best_ask)

        if best_bid is None or best_ask is None: 
            return None 
        
        self.spread = (best_ask - best_bid) / 100
        return self.spread

    def calc_bid_ask_price(self, symbol=None, t=None):
        """Override with APT-specific spread calculation"""

        positions = self.parent_client.positions.get(symbol, 0)

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
        gamma, k, q_tilde, T, sigma = self.get_avellaneda_stoikov_params()
        
        S0 = self.get_fair_value()

        constant_term = (1 / self.gamma) * math.log(1 + self.gamma / self.k) 

        print("constant term: ", constant_term)

        reservation_price, sigma = self.calc_reservation_price(t, sigma, gamma)

        deltaBid = max(gamma * (sigma ** 2) * T * (positions + 0.5) + constant_term, 0)
        deltaAsk = max(-gamma * (sigma ** 2) * T * (positions - 0.5) + constant_term, 0)

        delta_bid_price = int(reservation_price - deltaBid ) * 100 
        #print("delta ver. bid price: ", delta_bid_price )
        delta_ask_price = int(reservation_price + deltaAsk) * 100
        #print("delta ver. ask price: ", delta_ask_price )
        
        # using spread instead 
        #spread = self.calc_bid_ask_spread()
        #print("spread: ", spread)
        #bid_price = int((reservation_price - spread / 2) * 100)
        #print("spead ver. bid price: ", bid_price )
        #ask_price = int((reservation_price + spread / 2) * 100)
        #print("spead ver. ask price: ", ask_price )

        return delta_bid_price, delta_ask_price
    
    def calc_reservation_price(self, symbol, t, sigma, gamma):
        """We use the avellinda stoikov model to calculate the reservation price for 
        interday trading p_{\ text{mm}} = s - q \cdot \gamma \sigma^2 (T - t)"""
        S0 = self.get_fair_value()
        sigma = S0 * 0.02 / math.sqrt(self.T)
        dt = self.T - t # t is the current time stamp 

        positions = self.parent_client.positions.get(symbol, 0)

        reservation_price = S0 - positions * gamma * sigma**2 * dt
        print("reservation price: ", reservation_price) 
        return reservation_price, sigma
        
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