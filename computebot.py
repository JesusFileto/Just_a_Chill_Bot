# imports 
import polars as pl
import numpy as np
import asyncio
# super class for all compute bots
class Compute: 

    def __init__(self, parent_client=None): 
        self.parent_client = parent_client


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