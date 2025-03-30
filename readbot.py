import polars as pl 
import utcxchangelib
from utcxchangelib import xchange_client
import heapq 
import threading

class DataIngestion:
    def __init__(self, client):
        self.client = client
        self._lock = client._lock  # Use the same lock as the parent client
        

    async def handle_book_update(self, msg, index) -> None: 
        """
        Updates the book based on the incremental updates to the books
        provided by the exchange.
        :param msg: BookUpdate
        """

        is_bid = msg.side == xchange_client.utc_bot_pb2.BookUpdate.Side.BUY
        book = self.client.order_books[msg.symbol].bids if is_bid else self.client.order_books[msg.symbol].asks
        if msg.px not in book:
            book[msg.px] = msg.dq
        else:
            book[msg.px] += msg.dq

        print(msg)
        # Triggers server side event to update book in user interface
        if self.client.user_interface:
            utcxchangelib.requests.post('http://localhost:6060/updates', json={'update_type': 'book_update', 'symbol': msg.symbol, "is_bid": is_bid})

    async def bot_handle_book_update(self, msg, index) -> None:
        """Process book updates and store time series data"""
        #symbol being traded will never not be in dict
        book = self.client.order_books.get(msg.symbol)
        if not book:
            return

        # Process the order book
        sorted_bids = sorted(((k, v) for k, v in book.bids.items() if v), key=lambda x: x[0], reverse=True)
        best_bid = sorted_bids[0] if sorted_bids else None

        sorted_asks = sorted(((k, v) for k, v in book.asks.items() if v), key=lambda x: x[0])
        best_ask = sorted_asks[0] if sorted_asks else None
        
        if not best_bid or not best_ask:
            return
            
        # Create a row for the time series
        row = pl.DataFrame({
            "timestamp": index,
            "best_bid_px": best_bid[0],
            "best_bid_qt": best_bid[1],
            "best_ask_px": best_ask[0],
            "best_ask_qt": best_ask[1],
        })
        
        if (best_ask[0] - best_bid[0]) > 100:
            print(f"Spread is too wide for {msg.symbol}: {best_ask[0] - best_bid[0]}")
            print(f"Best ask: {best_ask}, Best bid: {best_bid}")
        
        print(index)
        
        # Create bid and ask DataFrames
        bids = pl.DataFrame({"px": [bid[0] for bid in sorted_bids], "qty": [bid[1] for bid in sorted_bids]})
        asks = pl.DataFrame({"px": [ask[0] for ask in sorted_asks], "qty": [ask[1] for ask in sorted_asks]})
        
        # Thread-safe updates to shared data structures
        with self._lock:
            # Update LOB time series
            self.client.stock_LOB_timeseries[msg.symbol][index] = {"bids": bids, "asks": asks}
            # Update best bid/ask time series
            self.client.stock_timeseries[msg.symbol] = pl.concat([self.client.stock_timeseries[msg.symbol], row])



    