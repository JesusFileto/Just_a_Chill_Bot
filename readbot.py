import polars as pl 
import utcxchangelib
from utcxchangelib import xchange_client
import heapq 

class DataIngestion(xchange_client.XChangeClient): 
    def __init__(self, client):
        self.client = client

    async def handle_book_update(self, msg, index) -> None: 
        """
        Updates the book based on the incremental updates to the books
        provided by the exchange.
        :param msg: BookUpdate
        """

        is_bid = msg.side == xchange_client.utc_bot_pb2.BookUpdate.Side.BUY
        book = self.order_books[msg.symbol].bids if is_bid else self.order_books[msg.symbol].asks
        if msg.px not in book:
            book[msg.px] = msg.dq
        else:
            book[msg.px] += msg.dq

        print(msg)
        # Triggers server side event to update book in user interface
        if self.user_interface:
            utcxchangelib.requests.post('http://localhost:6060/updates', json={'update_type': 'book_update', 'symbol': msg.symbol, "is_bid": is_bid})

    async def bot_handle_book_update(self, msg, index) -> None:
        #symbol being traded will never not be in dict
        #fastest way to append to pandas row is through a list
        self.order_books.items()
        
        book = self.order_books.get(msg.symbol)
        #print(book)

        #use heap to get best bid( faster than sorting)

        best_bid = heapq.nlargest(1, ((k, v) for k, v in book.bids.items() if v), key=lambda x: x[0])
        best_bid = best_bid[0] if best_bid else None 

        best_ask = heapq.nsmallest(1, ((k, v) for k, v in book.asks.items() if v), key=lambda x: x[0])
        best_ask = best_ask[0] if best_ask else None
        #these values are truthy just checking if not None
        print((best_bid or best_ask) )
        if not best_bid or not best_ask:
            return
        print(best_ask)
        row = pd.DataFrame([{
            "timestamp": index,
            "best_bid_px": best_bid[0],
            "best_bid_qt": best_bid[1],
            "best_ask_px": best_ask[0],
            "best_ask_qt": best_ask[1],
        }])
        self.stocks[msg.symbol] = pd.concat([self.stocks[msg.symbol],row], axis=0)
        #print(self.stocks[msg.symbol])



    