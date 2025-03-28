import numpy as np
from typing import Tuple, Dict, Optional
from dataclasses import dataclass

@dataclass
class MarketParameters:
    gamma: float  # Risk aversion parameter
    k: float      # Order arrival intensity
    sigma: float  # Volatility
    T: float      # Terminal time
    t: float      # Current time

class MarketMaker:
    def __init__(self, params: MarketParameters):
        self.params = params
        self.inventory = 0  # Current inventory position
        
    def calculate_optimal_spread(self) -> float:
        """Calculate the optimal bid-ask spread using the Avellaneda & Stoikov model."""
        T_minus_t = self.params.T - self.params.t
        spread = (self.params.gamma * self.params.sigma**2 * T_minus_t + 
                 2 * self.params.gamma * np.log(1 + self.params.gamma * self.params.k))
        return spread
    
    def calculate_reservation_price(self, current_price: float) -> float:
        """Calculate the reservation price based on inventory position."""
        T_minus_t = self.params.T - self.params.t
        inventory_adjustment = self.inventory * (2 * self.params.gamma * self.params.sigma**2 * T_minus_t) / 2
        return current_price - inventory_adjustment
    
    def calculate_bid_ask_prices(self, current_price: float) -> Tuple[float, float]:
        """Calculate bid and ask prices based on reservation price and spread."""
        spread = self.calculate_optimal_spread()
        reservation_price = self.calculate_reservation_price(current_price)
        
        bid_price = reservation_price - spread / 2
        ask_price = reservation_price + spread / 2
        
        return bid_price, ask_price
    
    def calculate_execution_probability(self, bid_price: float, ask_price: float, alpha: float = 1.0) -> float:
        """Calculate the probability of order execution based on spread."""
        spread = ask_price - bid_price
        prob = min(1, 1 / (1 + alpha * abs(spread)))
        return prob
    
    def update_inventory(self, quantity: int):
        """Update the market maker's inventory position."""
        self.inventory += quantity
    
    def calculate_market_impact(self, order_size: float, liquidity: float, alpha: float = 1.0) -> float:
        """Calculate market impact based on order size and liquidity."""
        impact = alpha * (liquidity / order_size)
        return impact
    
    def calculate_power_law_impact(self, order_size: float, liquidity: float, 
                                 beta: float = 1.0, delta: float = 0.5) -> float:
        """Calculate market impact using the power law model."""
        impact = beta * (liquidity / order_size) ** delta
        return impact 