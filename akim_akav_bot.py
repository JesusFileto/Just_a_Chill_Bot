from computebot import Compute
import polars as pl
import numpy as np
import math
import time
from utcxchangelib import xchange_client
import asyncio

class AKIMAKAVBot(Compute):
    """Specialized bot for AKIM and AKAV symbols that implements market making, 
    momentum trend trading, and arbitrage using theoretical pricing"""
    
    def __init__(self, parent_client=None, APT_bot=None, MKJ_bot=None, DLR_bot=None):
        super().__init__(parent_client)
        # This bot handles both AKIM and AKAV
        self.symbols = ["AKIM", "AKAV"]
        # Track correlation between the two symbols
        self.correlation = None
        # Track spread between the two symbols
        self.pair_spread = None
        # Historical fair values for each symbol
        self.historical_values = {
            "AKIM": [],
            "AKAV": []
        }
        # Reference to underlying stock bots
        self.APT_bot = APT_bot
        self.MKJ_bot = MKJ_bot
        self.DLR_bot = DLR_bot
        
        # Strategy parameters
        self.price_threshold = 0.02  # Threshold for arbitrage (2% deviation)
        self.momentum_window = 5     # Window for momentum calculation
        self.bullish_threshold = 0.01  # Threshold for bullish momentum
        self.bearish_threshold = -0.01  # Threshold for bearish momentum
        self.arb_trade_size = 10     # Size for arbitrage trades
        self.momentum_trade_size = 5  # Size for momentum trades
        self.market_making_size = 3  # Size for market making quotes
        
        # ETF swap fee
        self.swap_fee = 5
        
        # Track historical prices for momentum calculation
        self.price_history = {
            "AKIM": [],
            "AKAV": []
        }
        
        # Track theoretical prices
        self.theoretical_prices = {
            "AKIM": None,
            "AKAV": None
        }
        
        # Track momentum signals
        self.momentum_signals = {
            "AKIM": 0,
            "AKAV": 0
        }
        
        # Track arbitrage signals
        self.arbitrage_signals = {
            "AKIM": None,
            "AKAV": None
        }
        
        # Track market making quotes
        self.market_making_quotes = {
            "AKIM": {"bid": None, "ask": None},
            "AKAV": {"bid": None, "ask": None}
        }
        
        # Track inventory
        self.inventory = {
            "AKIM": 0,
            "AKAV": 0
        }
        
        # Track last update time
        self.last_update_time = time.time()
        
        # Track daily reset times (each day is 90 seconds)
        self.day_length = 90  # seconds
        self.last_day_reset = time.time()
        self.current_day = 0
        
        # Track initial prices for daily reset
        self.initial_prices = {
            "AKIM": None,
            "AKAV": None
        }
        
        # Track time remaining in the day
        self.time_remaining_in_day = self.day_length
        
        # Maximum allowed AKIM position (to ensure we can return to neutral)
        self.max_akim_position = 5
        
        # Flag to track if we're in the final minutes of the day
        self.is_final_minutes = False
        self.final_minutes_threshold = 15  # seconds
        
    def get_fair_value(self):
        """Calculate the theoretical fair value for AKAV based on underlying stocks"""
        # Sum of underlying stock fair values
        apt_fair = self.APT_bot.get_fair_value() if self.APT_bot else 0
        mkj_fair = self.MKJ_bot.get_fair_value() if self.MKJ_bot else 0
        dlr_fair = self.DLR_bot.get_fair_value() if self.DLR_bot else 0
        
        return apt_fair + mkj_fair + dlr_fair
    
    def calculate_theoretical_price(self, symbol):
        """Calculate the theoretical price for a symbol"""
        if symbol == "AKAV":
            # For AKAV, use the sum of underlying stock fair values
            base_theo = self.get_fair_value()
            if base_theo is not None:
                # Add half the swap fee to the theoretical price
                # This creates a "fair value band" of ±swap_fee/2 around the theoretical price
                # Prices within this band don't present arbitrage opportunities
                return base_theo + (self.swap_fee / 2)
            return None
        elif symbol == "AKIM":
            # For AKIM, use the inverse of AKAV's theoretical price
            akav_theo = self.calculate_theoretical_price("AKAV")
            if akav_theo is not None and self.initial_prices["AKAV"] is not None:
                # Calculate the percentage change in AKAV
                # Subtract the swap fee component before calculating the change
                akav_base = akav_theo - (self.swap_fee / 2)
                akav_change_percent = (akav_base - self.initial_prices["AKAV"]) / self.initial_prices["AKAV"]
                
                # AKIM should move in the opposite direction by the same percentage
                akim_change_percent = -akav_change_percent
                
                # Apply the percentage change to AKIM's initial price
                akim_theo = self.initial_prices["AKIM"] * (1 + akim_change_percent)
                return akim_theo
            return None
        return None
    
    def check_arbitrage_signal(self, symbol, current_price, theo_price):
        """Determine if an arbitrage opportunity exists based on thresholds"""
        if theo_price is None or current_price is None:
            return None
            
        # For AKAV, include the swap fee in the arbitrage calculation
        if symbol == "AKAV":
            # When price is higher than theoretical, we can:
            # 1. Short AKAV
            # 2. Buy underlying stocks
            # 3. Create new AKAV shares and cover short
            # Need price to be higher than theoretical + swap fee for profitable arbitrage
            if current_price > theo_price + self.swap_fee:
                return 'overpriced'
            # When price is lower than theoretical, we can:
            # 1. Buy AKAV
            # 2. Redeem for underlying stocks
            # 3. Sell underlying stocks
            # Need price to be lower than theoretical - swap fee for profitable arbitrage
            elif current_price < theo_price - self.swap_fee:
                return 'underpriced'
        else:  # For AKIM, use regular threshold since no creation/redemption
            deviation = (current_price - theo_price) / theo_price
            if deviation > self.price_threshold:
                return 'overpriced'
            elif deviation < -self.price_threshold:
                return 'underpriced'
        
        return None
    
    def compute_momentum(self, symbol):
        """Compute momentum signals for a symbol based on price history"""
        if symbol not in self.price_history or len(self.price_history[symbol]) < self.momentum_window:
            return 0
            
        # Get price history
        prices = self.price_history[symbol]
        
        # Calculate multiple momentum indicators
        
        # 1. Simple price change (original method)
        current_price = prices[-1]
        past_price = prices[-self.momentum_window]
        simple_momentum = (current_price - past_price) / past_price
        
        # 2. Exponential weighted momentum (more recent prices have higher weight)
        if len(prices) >= 5:
            # Calculate returns
            returns = []
            for i in range(1, len(prices)):
                returns.append((prices[i] - prices[i-1]) / prices[i-1])
            
            # Apply exponential weights (more recent returns have higher weight)
            weights = np.exp(np.linspace(-1, 0, len(returns)))
            weights = weights / np.sum(weights)  # Normalize weights
            
            # Calculate weighted momentum
            weighted_momentum = np.sum(np.array(returns) * weights)
        else:
            weighted_momentum = simple_momentum
        
        # 3. Rate of change (ROC) - percentage change over time
        roc_momentum = simple_momentum
        
        # 4. Moving average crossover
        if len(prices) >= 10:
            # Short-term MA (3 periods)
            short_ma = np.mean(prices[-3:])
            # Long-term MA (7 periods)
            long_ma = np.mean(prices[-7:])
            # MA crossover signal
            ma_crossover = (short_ma - long_ma) / long_ma
        else:
            ma_crossover = 0
        
        # Combine all momentum indicators with weights
        # Simple momentum: 20%, Weighted momentum: 40%, ROC: 20%, MA crossover: 20%
        combined_momentum = (
            0.2 * simple_momentum +
            0.4 * weighted_momentum +
            0.2 * roc_momentum +
            0.2 * ma_crossover
        )
        
        # Apply smoothing to reduce noise
        if hasattr(self, 'last_momentum') and symbol in self.last_momentum:
            # Exponential smoothing with alpha=0.3 (higher alpha = more weight to new value)
            alpha = 0.3
            combined_momentum = alpha * combined_momentum + (1 - alpha) * self.last_momentum[symbol]
        
        # Store the current momentum for next calculation
        if not hasattr(self, 'last_momentum'):
            self.last_momentum = {}
        self.last_momentum[symbol] = combined_momentum
        
        return combined_momentum
    
    def adjust_market_making_quotes(self, symbol, current_price, theo_price, momentum_signal):
        """Adjust market making quotes dynamically based on theoretical price and momentum"""
        if current_price is None:
            return None, None
            
        # Base spread calculation
        base_spread = 0.01  # 1% base spread
        
        # For AKAV, incorporate the swap fee into the spread
        if symbol == "AKAV":
            # Add the swap fee to the base spread
            # This ensures our quotes account for the cost of creation/redemption
            base_spread += self.swap_fee / current_price
        
        # Adjust spread based on deviation from theoretical price
        if theo_price is not None:
            deviation = abs((current_price - theo_price) / theo_price)
            # Widen spread as deviation increases
            base_spread += deviation * 0.5
        
        # Adjust spread based on momentum
        # Widen spread when momentum is strong (market is moving)
        momentum_factor = abs(momentum_signal) * 2
        adjusted_spread = base_spread * (1 + momentum_factor)
        
        # Calculate bid and ask prices
        mid_price = current_price
        bid_price = int(mid_price * (1 - adjusted_spread/2))
        ask_price = int(mid_price * (1 + adjusted_spread/2))
        
        # Adjust quotes based on momentum direction
        if momentum_signal > 0:  # Bullish momentum
            # More aggressive on the ask side
            ask_price = int(ask_price * 0.99)  # Lower ask price
        elif momentum_signal < 0:  # Bearish momentum
            # More aggressive on the bid side
            bid_price = int(bid_price * 1.01)  # Higher bid price
        
        return bid_price, ask_price
    
    def update_dynamic_hedges(self):
        """Update hedging positions to maintain inventory neutrality"""
        # Calculate net position across both symbols
        net_position = self.inventory["AKIM"] + self.inventory["AKAV"]
        
        # If net position is significant, hedge
        if abs(net_position) > 5:  # Threshold for hedging
            # Determine which symbol to hedge
            if net_position > 0:
                # Net long position, need to short
                if self.inventory["AKIM"] > 0:
                    # Short AKIM
                    self.place_hedge_order("AKIM", xchange_client.Side.SELL, abs(net_position))
                else:
                    # Short AKAV
                    self.place_hedge_order("AKAV", xchange_client.Side.SELL, abs(net_position))
            else:
                # Net short position, need to go long
                if self.inventory["AKIM"] < 0:
                    # Buy AKIM
                    self.place_hedge_order("AKIM", xchange_client.Side.BUY, abs(net_position))
                else:
                    # Buy AKAV
                    self.place_hedge_order("AKAV", xchange_client.Side.BUY, abs(net_position))
    
    def place_hedge_order(self, symbol, side, qty):
        """Place a hedging order"""
        # Get current market price
        book = self.parent_client.order_books[symbol]
        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        
        if side == xchange_client.Side.BUY and sorted_asks:
            price = sorted_asks[0][0]  # Best ask price
        elif side == xchange_client.Side.SELL and sorted_bids:
            price = sorted_bids[0][0]  # Best bid price
        else:
            return  # Can't place order
            
        # Place the order
        self.parent_client.trade_queue.put({
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price
        })
        print(f"Added HEDGE order to queue: {symbol} {side} {qty} @ {price}")
    
    def execute_arbitrage(self, symbol, signal):
        """Execute arbitrage trades based on signal"""
        if symbol == "AKAV":  # Only AKAV can be created/redeemed
            # Calculate the net profit after swap fee
            theo_price = self.calculate_theoretical_price("AKAV") - (self.swap_fee / 2)  # Remove the spread component
            current_price = (self.market_making_quotes["AKAV"]["bid"] + self.market_making_quotes["AKAV"]["ask"]) / 2
            
            if signal == 'underpriced':
                # Calculate potential profit
                profit_per_share = (theo_price - current_price - self.swap_fee)
                if profit_per_share <= 0:
                    return  # No profitable opportunity
                
                # ETF is trading below its theoretical value: buy AKAV shares and redeem
                # Place buy order for AKAV
                self.parent_client.trade_queue.put({
                    "symbol": "AKAV",
                    "side": xchange_client.Side.BUY,
                    "qty": self.arb_trade_size,
                    "price": self.market_making_quotes["AKAV"]["ask"]
                })
                print(f"Added ARBITRAGE BUY order to queue: AKAV {self.arb_trade_size} @ {self.market_making_quotes['AKAV']['ask']}")
                print(f"Expected profit per share after swap fee: {profit_per_share}")
                
            elif signal == 'overpriced':
                # Calculate potential profit
                profit_per_share = (current_price - theo_price - self.swap_fee)
                if profit_per_share <= 0:
                    return  # No profitable opportunity
                
                # ETF is trading above its theoretical value: short AKAV shares and create
                # Place sell order for AKAV
                self.parent_client.trade_queue.put({
                    "symbol": "AKAV",
                    "side": xchange_client.Side.SELL,
                    "qty": self.arb_trade_size,
                    "price": self.market_making_quotes["AKAV"]["bid"]
                })
                print(f"Added ARBITRAGE SELL order to queue: AKAV {self.arb_trade_size} @ {self.market_making_quotes['AKAV']['bid']}")
                print(f"Expected profit per share after swap fee: {profit_per_share}")
                
        elif symbol == "AKIM":  # AKIM cannot be created/redeemed, only traded
            # Check if we're in the final minutes of the day
            if self.is_final_minutes:
                print("Skipping AKIM arbitrage in final minutes of the day")
                return
                
            # Check if we're approaching the maximum allowed position
            if abs(self.inventory["AKIM"]) >= self.max_akim_position:
                print(f"AKIM position ({self.inventory['AKIM']}) already at maximum allowed ({self.max_akim_position})")
                return
                
            # Calculate remaining capacity
            remaining_capacity = self.max_akim_position - abs(self.inventory["AKIM"])
            trade_size = min(self.arb_trade_size, remaining_capacity)
            
            if trade_size <= 0:
                return
                
            if signal == 'underpriced':
                # AKIM is trading below its theoretical value: buy AKIM
                self.parent_client.trade_queue.put({
                    "symbol": "AKIM",
                    "side": xchange_client.Side.BUY,
                    "qty": trade_size,
                    "price": self.market_making_quotes["AKIM"]["ask"]
                })
                print(f"Added ARBITRAGE BUY order to queue: AKIM {trade_size} @ {self.market_making_quotes['AKIM']['ask']}")
                
            elif signal == 'overpriced':
                # AKIM is trading above its theoretical value: short AKIM
                self.parent_client.trade_queue.put({
                    "symbol": "AKIM",
                    "side": xchange_client.Side.SELL,
                    "qty": trade_size,
                    "price": self.market_making_quotes["AKIM"]["bid"]
                })
                print(f"Added ARBITRAGE SELL order to queue: AKIM {trade_size} @ {self.market_making_quotes['AKIM']['bid']}")
    
    def execute_momentum_trade(self, symbol, momentum_signal):
        """Execute momentum-based trades"""
        # For AKIM, be more conservative with momentum trading
        if symbol == "AKIM":
            # Check if we're in the final minutes of the day
            if self.is_final_minutes:
                print("Skipping AKIM momentum trade in final minutes of the day")
                return
                
            # Check if we're approaching the maximum allowed position
            if abs(self.inventory["AKIM"]) >= self.max_akim_position:
                print(f"AKIM position ({self.inventory['AKIM']}) already at maximum allowed ({self.max_akim_position})")
                return
                
            # Calculate remaining capacity
            remaining_capacity = self.max_akim_position - abs(self.inventory["AKIM"])
            trade_size = min(self.momentum_trade_size, remaining_capacity)
            
            if trade_size <= 0:
                return
                
            # Use a higher threshold for AKIM momentum trades
            if momentum_signal > self.bullish_threshold * 1.5:  # 50% higher threshold
                # Signal bullish on AKIM: buy AKIM
                self.parent_client.trade_queue.put({
                    "symbol": "AKIM",
                    "side": xchange_client.Side.BUY,
                    "qty": trade_size,
                    "price": self.market_making_quotes["AKIM"]["ask"]
                })
                print(f"Added MOMENTUM BUY order to queue: AKIM {trade_size} @ {self.market_making_quotes['AKIM']['ask']}")
                
            elif momentum_signal < self.bearish_threshold * 1.5:  # 50% higher threshold
                # Signal bearish on AKIM: short AKIM
                self.parent_client.trade_queue.put({
                    "symbol": "AKIM",
                    "side": xchange_client.Side.SELL,
                    "qty": trade_size,
                    "price": self.market_making_quotes["AKIM"]["bid"]
                })
                print(f"Added MOMENTUM SELL order to queue: AKIM {trade_size} @ {self.market_making_quotes['AKIM']['bid']}")
        
        # For AKAV, proceed with normal momentum trading
        elif symbol == "AKAV":
            if momentum_signal > self.bullish_threshold:
                # Signal bullish on AKAV: long AKAV
                self.parent_client.trade_queue.put({
                    "symbol": "AKAV",
                    "side": xchange_client.Side.BUY,
                    "qty": self.momentum_trade_size,
                    "price": self.market_making_quotes["AKAV"]["ask"]
                })
                print(f"Added MOMENTUM BUY order to queue: AKAV {self.momentum_trade_size} @ {self.market_making_quotes['AKAV']['ask']}")
                
            elif momentum_signal < self.bearish_threshold:
                # Signal bearish on AKAV: short AKAV
                self.parent_client.trade_queue.put({
                    "symbol": "AKAV",
                    "side": xchange_client.Side.SELL,
                    "qty": self.momentum_trade_size,
                    "price": self.market_making_quotes["AKAV"]["bid"]
                })
                print(f"Added MOMENTUM SELL order to queue: AKAV {self.momentum_trade_size} @ {self.market_making_quotes['AKAV']['bid']}")
    
    def update_market_making_quotes(self, symbol):
        """Update market making quotes for a symbol"""
        # Get current market price
        book = self.parent_client.order_books[symbol]
        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        
        if not sorted_bids or not sorted_asks:
            return
            
        current_price = (sorted_bids[0][0] + sorted_asks[0][0]) / 2
        
        # Get theoretical price
        theo_price = self.calculate_theoretical_price(symbol)
        
        # Get momentum signal
        momentum_signal = self.compute_momentum(symbol)
        self.momentum_signals[symbol] = momentum_signal
        
        # Check for arbitrage signal
        arbitrage_signal = self.check_arbitrage_signal(symbol, current_price, theo_price)
        self.arbitrage_signals[symbol] = arbitrage_signal
        
        # Adjust quotes
        bid_price, ask_price = self.adjust_market_making_quotes(symbol, current_price, theo_price, momentum_signal)
        
        if bid_price and ask_price:
            self.market_making_quotes[symbol] = {"bid": bid_price, "ask": ask_price}
            
            # Place market making orders
            self.parent_client.trade_queue.put({
                "symbol": symbol,
                "side": xchange_client.Side.BUY,
                "qty": self.market_making_size,
                "price": bid_price
            })
            print(f"Added MARKET MAKING BUY order to queue: {symbol} {self.market_making_size} @ {bid_price}")
            
            self.parent_client.trade_queue.put({
                "symbol": symbol,
                "side": xchange_client.Side.SELL,
                "qty": self.market_making_size,
                "price": ask_price
            })
            print(f"Added MARKET MAKING SELL order to queue: {symbol} {self.market_making_size} @ {ask_price}")
    
    def calc_bid_ask_spread(self, symbol=None, df=None):
        """Calculate bid-ask spread for AKIM or AKAV"""
        if symbol is None:
            return None
            
        book = self.parent_client.order_books[symbol]
        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        
        if not sorted_bids or not sorted_asks:
            return None
            
        best_bid = sorted_bids[0][0]
        best_ask = sorted_asks[0][0]
        
        spread = (best_ask - best_bid) / 100
        return spread
        
    def calc_fair_value(self, symbol=None, df=None):
        """Calculate fair value for AKIM or AKAV"""
        if symbol == "AKAV":
            return self.get_fair_value()
        elif symbol == "AKIM":
            akav_fair = self.get_fair_value()
            if akav_fair is not None and self.initial_prices["AKAV"] is not None:
                # Calculate the percentage change in AKAV
                akav_change_percent = (akav_fair - self.initial_prices["AKAV"]) / self.initial_prices["AKAV"]
                
                # AKIM should move in the opposite direction by the same percentage
                akim_change_percent = -akav_change_percent
                
                # Apply the percentage change to AKIM's initial price
                return self.initial_prices["AKIM"] * (1 + akim_change_percent)
            return None
        return None
    
    def update_pair_correlation(self):
        """Calculate correlation between AKIM and AKAV"""
        if "AKIM" not in self.price_history or "AKAV" not in self.price_history:
            return
            
        if len(self.price_history["AKIM"]) < 10 or len(self.price_history["AKAV"]) < 10:
            return
            
        # Calculate returns
        akim_returns = np.diff(np.log(self.price_history["AKIM"][-10:]))
        akav_returns = np.diff(np.log(self.price_history["AKAV"][-10:]))
        
        # Calculate correlation
        self.correlation = np.corrcoef(akim_returns, akav_returns)[0, 1]
        
        # Calculate spread
        self.pair_spread = np.mean(self.price_history["AKIM"][-10:]) - np.mean(self.price_history["AKAV"][-10:])
        
        print(f"AKIM-AKAV correlation: {self.correlation}, spread: {self.pair_spread}")

    def check_daily_reset(self):
        """Check if it's time for a daily reset and perform the reset if needed"""
        current_time = time.time()
        elapsed_since_reset = current_time - self.last_day_reset
        
        # Update time remaining in the day
        self.time_remaining_in_day = max(0, self.day_length - elapsed_since_reset)
        
        # Check if we're in the final minutes of the day
        self.is_final_minutes = self.time_remaining_in_day <= self.final_minutes_threshold
        
        # If we're in the final minutes, ensure AKIM position is neutral
        if self.is_final_minutes and self.inventory["AKIM"] != 0:
            print(f"FINAL MINUTES: Closing AKIM position of {self.inventory['AKIM']}")
            self.close_akim_position()
        
        # Check if we need to reset (each day is 90 seconds)
        if elapsed_since_reset >= self.day_length:
            # It's time for a daily reset
            self.current_day += 1
            self.last_day_reset = current_time
            
            # Get current prices for reset
            for symbol in self.symbols:
                book = self.parent_client.order_books[symbol]
                sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
                sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
                
                if sorted_bids and sorted_asks:
                    current_price = (sorted_bids[0][0] + sorted_asks[0][0]) / 2
                    self.initial_prices[symbol] = current_price
                else:
                    # If we can't get current prices, use theoretical prices
                    self.initial_prices[symbol] = self.calculate_theoretical_price(symbol)
            
            print(f"Daily reset (Day {self.current_day}): AKAV initial price: {self.initial_prices['AKAV']}, AKIM initial price: {self.initial_prices['AKIM']}")
            
            # Reset price history for the new day
            for symbol in self.symbols:
                if self.initial_prices[symbol] is not None:
                    self.price_history[symbol] = [self.initial_prices[symbol]]
            
            return True
        
        return False

    def close_akim_position(self):
        """Close the AKIM position to return to neutral"""
        akim_position = self.inventory["AKIM"]
        if akim_position == 0:
            return
            
        # Get current market price
        book = self.parent_client.order_books["AKIM"]
        sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
        sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
        
        if not sorted_bids or not sorted_asks:
            return
            
        # Determine side based on position
        if akim_position > 0:
            # Long position, need to sell
            side = xchange_client.Side.SELL
            price = sorted_bids[0][0]  # Best bid price
        else:
            # Short position, need to buy
            side = xchange_client.Side.BUY
            price = sorted_asks[0][0]  # Best ask price
            
        # Place the order
        self.parent_client.trade_queue.put({
            "symbol": "AKIM",
            "side": side,
            "qty": abs(akim_position),
            "price": price
        })
        print(f"CLOSING AKIM POSITION: {side} {abs(akim_position)} @ {price}")

    async def process_update(self, index):
        """Process updates for AKIM and AKAV"""
        # Update current time
        current_time = time.time()
        time_since_last_update = current_time - self.last_update_time
        self.last_update_time = current_time
        
        # Check for daily reset
        self.check_daily_reset()
        
        # Process each symbol
        for symbol in self.symbols:
            # Get current market price
            book = self.parent_client.order_books[symbol]
            sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
            sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
            
            if not sorted_bids or not sorted_asks:
                continue
                
            current_price = (sorted_bids[0][0] + sorted_asks[0][0]) / 2
            
            # Update price history
            self.price_history[symbol].append(current_price)
            if len(self.price_history[symbol]) > 100:  # Keep last 100 prices
                self.price_history[symbol] = self.price_history[symbol][-100:]
            
            # Calculate theoretical price
            theo_price = self.calculate_theoretical_price(symbol)
            self.theoretical_prices[symbol] = theo_price
            
            # Calculate momentum signal
            momentum_signal = self.compute_momentum(symbol)
            self.momentum_signals[symbol] = momentum_signal
            
            # Check for arbitrage signal
            arbitrage_signal = self.check_arbitrage_signal(symbol, current_price, theo_price)
            self.arbitrage_signals[symbol] = arbitrage_signal
            
            # Update market making quotes
            self.update_market_making_quotes(symbol)
            
            # Execute arbitrage if signal is present
            if arbitrage_signal:
                self.execute_arbitrage(symbol, arbitrage_signal)
            
            # Execute momentum trades if signal is strong
            if abs(momentum_signal) > max(self.bullish_threshold, abs(self.bearish_threshold)):
                self.execute_momentum_trade(symbol, momentum_signal)
        
        # Update pair correlation
        self.update_pair_correlation()
        
        # Update dynamic hedges
        self.update_dynamic_hedges()
        
        # Update inventory from parent client positions
        for symbol in self.symbols:
            self.inventory[symbol] = self.parent_client.positions.get(symbol, 0)
            
        # Log position status
        print(f"Current positions: AKIM={self.inventory['AKIM']}, AKAV={self.inventory['AKAV']}, Time remaining: {self.time_remaining_in_day:.1f}s")

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        """Handle trade messages for AKIM and AKAV"""
        # Update price history with the trade price
        if symbol in self.symbols:
            self.price_history[symbol].append(price)
            if len(self.price_history[symbol]) > 100:  # Keep last 100 prices
                self.price_history[symbol] = self.price_history[symbol][-100:]
            
            # Update inventory
            self.inventory[symbol] = self.parent_client.positions.get(symbol, 0)
            
            # Process the update
            await self.process_update(None)
