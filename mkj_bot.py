from computebot import Compute
import polars as pl
import numpy as np
import asyncio
import time


class MKJBot(Compute):
    """Specialized bot for MKJ symbol"""
    
    def __init__(self, parent_client=None):
        super().__init__(parent_client)
        self.symbol = "MKJ"
        self.trade_count = 0
        self.fair_value = None
        self.fair_value_timeseries = pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "fair_value": pl.Float64,
            "midpoint": pl.Float64
        })
        
        # Order book tracking
        
        # Order book imbalance tracking with Polars
        self.imbalance_timeseries = pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "imbalance": pl.Float64,
            "bid_volume": pl.Int64,
            "ask_volume": pl.Int64
        })
        
        
        # Volatility calculation
        self.volatility_window = 20  # Window for volatility calculation
        self.volatility = 0.0  # Current volatility
        
        # Regression parameters
        self.alpha = 0.0  # Spread coefficient
        self.beta = 0.0   # Order book imbalance coefficient
        self.regression_window = 10  # Window for regression
        
        # Fair value calculation parameters
        self.k_factor = 1.5  # Confidence interval factor
        self.min_spread = 0.1  # Minimum spread to consider
        self.max_spread = 5.0  # Maximum spread to consider
        
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
        historical_midpoints = None
        historical_spreads = None
        imbalance = self._calculate_order_book_imbalance()
        with self.parent_client._lock:
            historical_midpoints = self.parent_client.stock_LOB_timeseries["MKJ"]["mid_price"]
            historical_spreads = self.parent_client.stock_LOB_timeseries["MKJ"]["spread"]
        
        
        # If we don't have enough data, return a default value
        if len(historical_midpoints) == 0:
            return 50.0
        if len(historical_midpoints) < 5:
            #mark return value as last value in historical_midpoints
            return historical_midpoints[-1]
        
        
        # Get the current midpoint
        current_midpoint = historical_midpoints[-1] if historical_midpoints.is_empty() is False else 50.0
        print("current_midpoint: ", current_midpoint)
        
        
        # Convert historical spreads to numpy array if not empty
        historical_spreads_np = historical_spreads.to_numpy() if not historical_spreads.is_empty() else None
        
        # Calculate spread impact using numpy array
        current_spread = historical_spreads_np[-1] if historical_spreads_np is not None else 1.0
        avg_spread = np.mean(historical_spreads_np) if historical_spreads_np is not None else 1.0
        spread_ratio = current_spread / avg_spread if avg_spread > 0 else 1.0


        
        # Calculate volatility-adjusted fair value
        # Fair value = midpoint + alpha*spread_change + beta*imbalance
        spread_change = spread_ratio - 1.0  # Normalized spread change
        
        # Apply regression coefficients if available
        if len(historical_midpoints) >= self.regression_window:
            self._update_regression_parameters(historical_midpoints, historical_spreads)
            fair_value = current_midpoint + self.alpha * spread_change + self.beta * imbalance
        else:
            # Simple heuristic if we don't have enough data for regression
            fair_value = current_midpoint + 0.1 * spread_change + 0.2 * imbalance
        
        # Apply confidence bands based on volatility
        # Fair value is within [fair_value - k*volatility, fair_value + k*volatility]
        # lower_band = fair_value - self.k_factor * self.volatility
        # upper_band = fair_value + self.k_factor * self.volatility
        
        # Ensure fair value is within reasonable bounds
        fair_value = max(0, min(5000, fair_value))
        
        print("fair_value MKJ: ", fair_value)
        self._update_fair_value_timeseries(fair_value, current_midpoint)
        return fair_value
    
    def _update_fair_value_timeseries(self, fair_value, midpoint):
        """
        Update the fair value timeseries with a new snapshot
        """
        timestamp = self.parent_client.stock_LOB_timeseries[self.symbol]["timestamp"].tail(1).item()
        new_row = pl.DataFrame([{
            "timestamp": timestamp,
            "fair_value": fair_value,
            "midpoint": midpoint
        }])
        self.fair_value_timeseries = pl.concat([self.fair_value_timeseries, new_row])
    
    def _calculate_order_book_imbalance(self):
        """
        Calculate the order book imbalance using the parent client's LOB_timeseries
        
        Returns:
            float: Order book imbalance between -1 and 1
        """
            
        # Get the LOB timeseries from the parent client
        lob_df = self.parent_client.stock_LOB_timeseries["MKJ"]
        
        # If the timeseries is empty, return 0
        if len(lob_df) == 0:
            return 0.0
            
        # Get the last row of the timeseries
        last_row = lob_df.tail(1)
        
        # Calculate total bid volume from all 4 price levels
        bid_volume = (
            last_row["best_bid_qt"].item() +  # Level 1
            last_row["2_bid_qt"].item() +     # Level 2
            last_row["3_bid_qt"].item() +     # Level 3
            last_row["4_bid_qt"].item()       # Level 4
        )
        
        # Calculate total ask volume from all 4 price levels
        ask_volume = (
            last_row["best_ask_qt"].item() +  # Level 1
            last_row["2_ask_qt"].item() +     # Level 2
            last_row["3_ask_qt"].item() +     # Level 3
            last_row["4_ask_qt"].item()       # Level 4
        )
        
        # Calculate total volume
        total_volume = bid_volume + ask_volume
        if total_volume == 0:
            return 0.0
        
        # Imbalance is (bid_volume - ask_volume) / (bid_volume + ask_volume)
        imbalance = (bid_volume - ask_volume) / total_volume
        timestamp = self.parent_client.stock_LOB_timeseries[self.symbol]["timestamp"].tail(1).item()
        #print("imbalance: ", imbalance)
        self._update_imbalance_timeseries(timestamp, imbalance, bid_volume, ask_volume)
        return imbalance
    
    def _update_imbalance_timeseries(self, timestamp, imbalance, bid_volume, ask_volume):
        """
        Update the imbalance timeseries with a new snapshot
        
        Args:
            timestamp: Current timestamp
            imbalance: Calculated order book imbalance
            bid_volume: Total bid volume
            ask_volume: Total ask volume
        """
        # Create a new row
        new_row = pl.DataFrame([{
            "timestamp": timestamp,
            "imbalance": imbalance,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume
        }])
        
        # Append to the timeseries
        #print("new_row: ", new_row)
        self.imbalance_timeseries = pl.concat([self.imbalance_timeseries, new_row])
    
   
    
    def _update_regression_parameters(self, historical_midpoints, historical_spreads):
        """
        Update the regression parameters alpha and beta using fast OLS
        """
        if len(historical_midpoints) < self.regression_window:
            return
        
        # Prepare data for regression using last regression_window points
        midpoints = np.array(list(historical_midpoints[-self.regression_window:]))
        spreads = np.array(list(historical_spreads[-self.regression_window:]))
        
        # Calculate changes
        delta_midpoints = np.diff(midpoints)
        delta_spreads = np.diff(spreads)
        
        # Get historical imbalances from the timeseries
        if len(self.imbalance_timeseries) >= self.regression_window:
            # Use the most recent imbalances
            imbalances = self.imbalance_timeseries.select("imbalance").tail(self.regression_window-1).to_series().to_numpy()
        else:
            # If we don't have enough imbalance data, use zeros
            imbalances = np.zeros(self.regression_window-1)
            
        # print("midpoints: ", midpoints)
        # print("spreads: ", spreads)
        # print("imbalances: ", imbalances)
        # print("delta_midpoints: ", delta_midpoints)
        # print("delta_spreads: ", delta_spreads)
        
        # Prepare X matrix (spread changes and imbalances)
        X = np.column_stack((delta_spreads, imbalances))
        
        # Prepare y vector (midpoint changes)
        y = delta_midpoints
        
        # Perform regression if we have enough data
        if len(X) > 0 and len(y) > 0 and len(X) == len(y):
            try:
                # Use numpy's lstsq for fast and stable OLS
                beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
                self.alpha = beta_hat[0]  # Coefficient for spread changes
                self.beta = beta_hat[1]   # Coefficient for imbalances
                print("alpha: ", self.alpha)
                print("beta: ", self.beta)
            except:
                # If regression fails, use default values
                self.alpha = 0.1
                self.beta = 0.2
    
    async def process_update(self, update):
        """
        Process updates from the parent client
        
        Args:
            update: The update to process
        """
        # This method would be called by the parent client's compute thread
        # It would process updates and calculate new fair values
        pass
    
    
    def increment_trade(self):
        """
        Increment the trade counter
        """
        # For MKJ, we want to only update our model based on a new LOB snapshot
        # do we want to perform a trade after a fixed number of trades or time interval?
        self.trade_count += 1
        
    def handle_snapshot(self):
        print("handle_snapshot")
        self.fair_value = self.calc_fair_value()
        
    
    def unstructured_update(self, news_data):
        """
        Handle unstructured news updates
        
        Args:
            news_data: The news data
        """
        # For MKJ, we might want to update our model based on news
        pass