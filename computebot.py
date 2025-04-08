# imports 
import polars as pl
import numpy as np
import asyncio
from scipy.optimize import minimize
import math
import time
from utcxchangelib import xchange_client
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
        self.T = 15 * 60
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
        #print("Sorted Bids:", sorted_bids)
        #print("Sorted Asks:", sorted_asks)
    
        best_bid,_ = max(sorted_bids) if len(sorted_bids) > 0 else None 
        #print("best bid: ", best_bid)
        best_ask,_ = min(sorted_asks) if len(sorted_asks) > 0 else None 
        #print("best ask: ", best_ask)

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
        gamma, k, fair_value, T, sigma = self.get_avellaneda_stoikov_params()
        
        S0 = fair_value

        constant_term = (1 / gamma) * math.log(1 + gamma / k) 

        #print("constant term: ", constant_term)

        reservation_price, sigma = self.calc_reservation_price(symbol, t, sigma, gamma, fair_value)
        if reservation_price is None:
            return None, None

        deltaBid = max(gamma * (sigma ** 2) * (T-t) * (positions + 0.5) + constant_term, 0)
        deltaAsk = max(-gamma * (sigma ** 2) * (T-t) * (positions - 0.5) + constant_term, 0)

        delta_bid_price = int(reservation_price - deltaBid )
        #print("delta ver. bid price: ", delta_bid_price )
        delta_ask_price = int(reservation_price + deltaAsk)
        
        #print("deltaBid: ", deltaBid)
        #print("deltaAsk: ", deltaAsk)
        #print("delta ver. ask price: ", delta_ask_price )
        
        # using spread instead 
        #spread = self.calc_bid_ask_spread()
        #print("spread: ", spread)
        #bid_price = int((reservation_price - spread / 2) * 100)
        #print("spead ver. bid price: ", bid_price )
        #ask_price = int((reservation_price + spread / 2) * 100)
        #print("spead ver. ask price: ", ask_price )

        return delta_bid_price, delta_ask_price
    
    def calc_reservation_price(self, symbol, t, sigma, gamma, fair_value):
        """We use the avellinda stoikov model to calculate the reservation price for 
        interday trading p_{\ text{mm}} = s - q \cdot \gamma \sigma^2 (T - t)"""
        S0 = fair_value
        if S0 is None:
            return None, None
        dt = self.T - t # t is the current time stamp 
        
        if sigma is None:
            sigma = S0 * 0.02 / math.sqrt(self.T) * 0.1

        positions = self.parent_client.positions.get(symbol, 0)

        reservation_price = S0 - positions * gamma * sigma**2 * dt
        #print("reservation price: ", reservation_price) 
        #print("sigma: ", sigma)
        #print("gamma: ", gamma)
        #print("dt: ", dt)
        #print("S0: ", S0)
        #print("positions: ", positions)

        return reservation_price, sigma
    
    def handle_trade(self, symbol):
        latest_timestamp = None
        latest_timestamp = int(time.time()) - self.parent_client.start_time
        if latest_timestamp is None:
            return 
        #print("type of latest_timestamp: ", type(latest_timestamp))
        bid_price, ask_price = self.calc_bid_ask_price(symbol, latest_timestamp)
        if bid_price is None or ask_price is None:
            return 
        #print("========================================")
        #print("handle_trade for ", symbol)
        #print("Adjusted Bid Price:", bid_price)
        #print("Adjusted Ask Price:", ask_price)
       # Put buy order in queue
        self.parent_client.trade_queue.put({
            "symbol": symbol,
            "side": xchange_client.Side.BUY,
            "qty": self.get_q_tilde(),
            "price": bid_price
        })
        print(f"Added BUY order to queue: {symbol} {self.get_q_tilde()} @ {bid_price}")
        
        # Put sell order in queue
        self.parent_client.trade_queue.put({
            "symbol": symbol,
            "side": xchange_client.Side.SELL,
            "qty": self.get_q_tilde(),
            "price": ask_price
        })
        print(f"Added SELL order to queue: {symbol} {self.get_q_tilde()} @ {ask_price}")
        #print("========================================")
        #print("my positions:", self.parent_client.positions)
        
    def increment_trade(self):
        pass
    
    def get_q_tilde(self):
        pass
        
    def unstructured_update(self, news_data):
        pass
    
    
    def calc_volatility(self, omega, alpha, beta, index=None):
        omega, alpha, beta, sigma = self.garch_neg_loglik(omega, alpha, beta, index)
        return omega, alpha, beta, sigma
        
    # GARCH for volatility 
    def garch_neg_loglik(self, omega, alpha, beta, index=None):
        # This method is now only used for the initial calculation
        # The actual optimization happens in fit_garch
        returns = None
        sigma2 = None
        with self.parent_client._lock:
            # Get full series length
            T = len(self.parent_client.stock_LOB_timeseries[self.symbol]["spread"])
            
            # If index specified, only look at last index items
            start_idx = max(0, T - index) if index else 0
            window_size = min(T, index) if index else T
            
            sigma2 = np.zeros(window_size)
            prices = self.parent_client.stock_LOB_timeseries[self.symbol]["mid_price"].to_numpy()[start_idx:]
            returns = np.log(prices[:-1] / prices[1:])
            
        # initial variance 
        sigma2[0] = np.var(returns)
        
        # Get optimized parameters
        # print("sigma2[0]: ", sigma2[0])
        # print("returns[-1]: ", returns[-1])
        # print("omega: ", sigma2[0] - alpha - beta)
        # print("alpha: ", alpha)
        # print("beta: ", beta)
        
        omega, alpha, beta = self.fit_garch(sigma2[0] - alpha - beta, alpha, beta, returns)
        
        # Calculate final sigma
        sigma = np.sqrt(omega + alpha * (returns[-1]**2) + beta * sigma2[-1])
        
        if sigma is None or alpha is None or beta is None:
            return None, None, None, None
        
        return omega, alpha, beta, sigma

    def fit_garch(self, omega, alpha, beta, returns):
        # Initial guess for parameters
        initial_guess = [omega, alpha, beta]
        bounds = [(1e-8, 100), (0, 1), (0, 1)]
        
        # Define the objective function for optimization
        def objective(returns, params):
            omega, alpha, beta = params
            #print("params: ", params)
            # Calculate the negative log-likelihood with these parameters
            T = len(returns)
            sigma2 = np.zeros(T)
            sigma2[0] = np.var(returns)
            
            # Initialize negative log-likelihood
            neg_loglik = 0
            
            # Recursively compute the conditional variance and accumulate the log-likelihood
            for t in range(1, T):
                
                sigma2[t] = omega + alpha * (returns[t-1]**2) + beta * sigma2[t-1]
                
                # To avoid numerical issues, ensure sigma2[t] is positive
                if sigma2[t] <= 0:
                    return 1e5
                
                # Contribution to log-likelihood from observation t
                neg_loglik += 0.5 * (np.log(2 * np.pi) + np.log(sigma2[t]) + (returns[t]**2 / sigma2[t]))
            #print("neg_loglik: ", neg_loglik)
            return neg_loglik
        
        # Perform optimization
        result = minimize(lambda params: objective(returns, params), initial_guess, bounds=bounds, method='L-BFGS-B')
        
        # Extract optimized parameters
        omega = result.x[0]
        alpha = result.x[1]
        beta = result.x[2]
        
        return omega, alpha, beta
    
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