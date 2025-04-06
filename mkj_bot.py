from computebot import Compute
import polars as pl
import numpy as np
import asyncio
import time


class MKJBot(Compute):
    """Specialized bot for MKJ symbol with high volatility and order book dynamics"""
    
    def __init__(self, parent_client=None):
        super().__init__(parent_client)
        self.symbol = "MKJ"
        self.price_levels = []  # Track significant price levels
        
        # Volatility calculation
        self.volatility_window = 20  # Window for volatility calculation
        self.volatility = 0.0  # Current volatility
        
        # Regression parameters
        self.alpha = 0.0  # Spread coefficient
        self.beta = 0.0   # Order book imbalance coefficient
        self.regression_window = 50  # Window for regression
        
        # Fair value calculation parameters
        self.k_factor = 1.5  # Confidence interval factor
        self.min_spread = 0.1  # Minimum spread to consider
        self.max_spread = 5.0  # Maximum spread to consider
        
        # Order book imbalance tracking
        self.bid_volume = 0
        self.ask_volume = 0
        
    def calc_bid_ask_spread(self):
        """
        Calculate the bid-ask spread for MKJ
        
        Returns:
            tuple: (bid_price, ask_price)
        """
        # Get the current fair value
        fair_value = self.calc_fair_value()
        
        # Calculate a dynamic spread based on volatility
        # Higher volatility = wider spread
        spread = max(self.min_spread, min(self.max_spread, self.volatility * 2))
        
        # Set bid and ask prices around the fair value
        bid_price = max(0, fair_value - spread/2)
        ask_price = min(100, fair_value + spread/2)
        
        return bid_price, ask_price
        
    def calc_fair_value(self):
        """
        Calculate the fair value of MKJ based on order book dynamics
        
        Returns:
            float: Fair value of MKJ
        """
        # If we don't have enough data, return a default value
        if len(self.historical_midpoints) < 5:
            return 50.0  # Default to midpoint of possible range
        
        # Get the current midpoint
        current_midpoint = self.historical_midpoints[-1] if self.historical_midpoints else 50.0
        
        # Calculate order book imbalance
        imbalance = self._calculate_order_book_imbalance()
        
        # Calculate spread impact
        current_spread = self.historical_spreads[-1] if self.historical_spreads else 1.0
        avg_spread = np.mean(self.historical_spreads) if self.historical_spreads else 1.0
        spread_ratio = current_spread / avg_spread if avg_spread > 0 else 1.0
        
        # Calculate VWAP from recent trades
        vwap = self._calculate_vwap()
        
        # Calculate volatility-adjusted fair value
        # Fair value = midpoint + alpha*spread_change + beta*imbalance
        spread_change = spread_ratio - 1.0  # Normalized spread change
        
        # Apply regression coefficients if available
        if len(self.historical_midpoints) >= self.regression_window:
            fair_value = current_midpoint + self.alpha * spread_change + self.beta * imbalance
        else:
            # Simple heuristic if we don't have enough data for regression
            fair_value = current_midpoint + 0.1 * spread_change + 0.2 * imbalance
        
        # Apply confidence bands based on volatility
        # Fair value is within [fair_value - k*volatility, fair_value + k*volatility]
        lower_band = fair_value - self.k_factor * self.volatility
        upper_band = fair_value + self.k_factor * self.volatility
        
        # Ensure fair value is within reasonable bounds
        fair_value = max(0, min(100, fair_value))
        
        # If VWAP is available, use it as a sanity check
        if vwap is not None:
            # Blend VWAP with our calculated fair value
            fair_value = 0.7 * fair_value + 0.3 * vwap
        
        return fair_value
    
    def _calculate_order_book_imbalance(self):
        """
        Calculate the order book imbalance
        
        Returns:
            float: Order book imbalance between -1 and 1
        """
        total_volume = self.bid_volume + self.ask_volume
        if total_volume == 0:
            return 0.0
        
        # Imbalance is (bid_volume - ask_volume) / (bid_volume + ask_volume)
        return (self.bid_volume - self.ask_volume) / total_volume
    
    def _calculate_vwap(self):
        """
        Calculate the Volume Weighted Average Price from recent trades
        
        Returns:
            float: VWAP or None if no trades
        """
        if not self.recent_trades:
            return None
        
        total_volume = sum(trade['volume'] for trade in self.recent_trades)
        if total_volume == 0:
            return None
        
        vwap = sum(trade['price'] * trade['volume'] for trade in self.recent_trades) / total_volume
        return vwap
    
    def _update_volatility(self):
        """
        Update the volatility calculation
        """
        if len(self.trade_prices) < 2:
            return
        
        # Calculate returns
        returns = np.diff(list(self.trade_prices)) / list(self.trade_prices)[:-1]
        
        # Calculate volatility as standard deviation of returns
        if len(returns) >= self.volatility_window:
            # Use the most recent volatility_window returns
            recent_returns = returns[-self.volatility_window:]
            self.volatility = np.std(recent_returns)
        else:
            # Use all available returns
            self.volatility = np.std(returns) if len(returns) > 0 else 0.0
    
    def _update_regression_parameters(self):
        """
        Update the regression parameters alpha and beta
        """
        if len(self.historical_midpoints) < self.regression_window:
            return
        
        # Prepare data for regression
        midpoints = np.array(list(self.historical_midpoints))
        spreads = np.array(list(self.historical_spreads))
        
        # Calculate changes
        delta_midpoints = np.diff(midpoints)
        delta_spreads = np.diff(spreads)
        
        # Calculate order book imbalances
        imbalances = []
        for i in range(1, len(midpoints)):
            # This is a simplification - in reality, we'd need historical order book data
            # For now, we'll use a placeholder
            imbalances.append(0.0)
        
        # Prepare X matrix (spread changes and imbalances)
        X = np.column_stack((delta_spreads[:-1], imbalances))
        
        # Prepare y vector (midpoint changes)
        y = delta_midpoints[1:]
        
        # Perform regression if we have enough data
        if len(X) > 0 and len(y) > 0 and len(X) == len(y):
            try:
                # Use numpy's polyfit for simple linear regression
                coefficients = np.polyfit(X[:, 0], y, 1)
                self.alpha = coefficients[0]
                
                # If we have imbalance data, fit that too
                if X.shape[1] > 1:
                    coefficients = np.polyfit(X[:, 1], y, 1)
                    self.beta = coefficients[0]
            except:
                # If regression fails, use default values
                self.alpha = 0.1
                self.beta = 0.2
    
    def _update_support_resistance(self):
        """
        Identify support and resistance levels for MKJ
        """
        # This would analyze price action to identify key levels
        # For now, we'll use a simple placeholder
        pass

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
        # Record the trade
        self.recent_trades.append({
            'price': price,
            'volume': qty,
            'timestamp': time.time()
        })
        
        # Update trade prices for volatility calculation
        self.trade_prices.append(price)
        
        # Update volatility
        self._update_volatility()
        
        # Send the trade message back to the parent
        if self.parent_client:
            await self.send_to_parent("trade", {
                "symbol": symbol,
                "price": price,
                "qty": qty
            })
    
    def increment_trade(self):
        """
        Increment the trade counter
        """
        # For MKJ, we might want to update our model based on trades
        pass

    async def process_update(self, index):
        """Override for MKJ-specific update processing"""
        pass