#%% 

from typing import Optional

import utcxchangelib
from utcxchangelib import xchange_client
import asyncio
import argparse
import polars as pl
import heapq
import matplotlib.pyplot as plt


class MyXchangeClient(xchange_client.XChangeClient):
    plot = False
    # Initialize empty DataFrames with optimized schema and settings
    stock_timeseries = {
        "APT": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64
        }),
        "DLR": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64
        }),
        "MKJ": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64
        }),
        "AKAV": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64
        }),
        "AKIM": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64
        }),
    }
    
    stock_LOB_timeseries = { 
        "APT": {},                
        "DLR": {},
        "MKJ": {},
        "AKAV": {},
        "AKIM": {},
    }

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        
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

        #print(msg)
        # Triggers server side event to update book in user interface
        if self.user_interface:
            utcxchangelib.requests.post('http://localhost:6060/updates', json={'update_type': 'book_update', 'symbol': msg.symbol, "is_bid": is_bid})

        await self.bot_handle_book_update(msg, index)

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str]) -> None:
        order = self.open_orders[order_id]
        print(f"{'Market' if order[2] else 'Limit'} Order ID {order_id} cancelled, {order[1]} unfilled")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print("order fill", self.positions)

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        print("order rejected because of ", reason)


    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass

    async def bot_handle_book_update(self, symbol: str) -> None:
        pass

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        pass

    async def bot_handle_news(self, news_release: dict):
        # Parsing the message based on what type was received
        timestamp = news_release["timestamp"] # This is in exchange ticks not ISO or Epoch
        news_type = news_release['kind']
        news_data = news_release["new_data"]

        if news_type == "structured":
            subtype = news_data["structured_subtype"]
            symb = news_data["asset"]
            if subtype == "earnings":
                
                earnings = news_data["value"]

                # Do something with this data

            else:

             
                new_signatures = news_data["new_signatures"]
                cumulative = news_data["cumulative"]

                # Do something with this data
        else:

            # Not sure what you would do with unstructured data....

            pass
    
    async def bot_handle_book_update(self, msg, index) -> None:
        #symbol being traded will never not be in dict
        #fastest way to append to pandas row is through a list
        self.order_books.items()
        
        book = self.order_books.get(msg.symbol)
        if not book:
            return

        #use heap to get best bid( faster than sorting)

        sorted_bids = sorted(((k, v) for k, v in book.bids.items() if v), key=lambda x: x[0], reverse=True)
        best_bid = sorted_bids[0] if sorted_bids else None

        sorted_asks = sorted(((k, v) for k, v in book.asks.items() if v), key=lambda x: x[0])
        best_ask = sorted_asks[0] if sorted_asks else None
        #these values are truthy just checking if not None
        
        #print((best_bid or best_ask) )
        if not best_bid or not best_ask:
            return
        #print(best_ask)
        row = pl.DataFrame({
            "timestamp": index,
            "best_bid_px": best_bid[0],
            "best_bid_qt": best_bid[1],
            "best_ask_px": best_ask[0],
            "best_ask_qt": best_ask[1],
        })
        if (best_ask[0] - best_bid[0]) > 100:
            print("spread is too wide")
            print(best_ask, best_bid)
        
        print(index)
        
        bids = pl.DataFrame({"px": [bid[0] for bid in sorted_bids], "qty": [bid[1] for bid in sorted_bids]})
        asks = pl.DataFrame({"px": [ask[0] for ask in sorted_asks], "qty": [ask[1] for ask in sorted_asks]})
        self.stock_LOB_timeseries[msg.symbol][index] = {"bids": bids, "asks": asks}
        self.stock_timeseries[msg.symbol] = pl.concat([self.stock_timeseries[msg.symbol],row])
        #print(self.stocks[msg.symbol])

    async def plot_best_bid_ask(self):
        for symbol, df in self.stock_timeseries.items():
            plt.figure(figsize=(12, 6))
            
            timestamp = df["timestamp"].to_list()
            best_bid_px = df["best_bid_px"].to_list()
            best_ask_px = df["best_ask_px"].to_list()
            
            plt.plot(timestamp, best_bid_px, label="Best Bid Price", linestyle="-",markersize=1)
            plt.plot(timestamp, best_ask_px, label="Best Ask Price", linestyle="-",markersize=1)

            plt.legend(["Best Bid Price", "Best Ask Price"])
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.grid()

            # Show plot
            print(f"Saving figure for {symbol}")
            plt.savefig(f"data/best_bid_ask_{symbol}.png")

    async def trade(self):
        await asyncio.sleep(5)
        print("attempting to trade")
        await self.place_order("APT",3, xchange_client.Side.BUY, 5)
        await self.place_order("APT",3, xchange_client.Side.SELL, 7)
        await asyncio.sleep(5)
        await self.cancel_order(list(self.open_orders.keys())[0])
        await self.place_swap_order('toAKAV', 1)
        await asyncio.sleep(5)
        await self.place_swap_order('fromAKAV', 1)
        await asyncio.sleep(5)
        await self.place_order("APT",1000, xchange_client.Side.SELL, 7)
        await asyncio.sleep(5)
        market_order_id = await self.place_order("APT",10, xchange_client.Side.SELL)
        print("MARKET ORDER ID:", market_order_id)
        await asyncio.sleep(5)
        print("my positions:", self.positions)

    async def view_books(self):
        # Use polars DataFrame for better performance
        securities = pl.DataFrame({
            'security': ['ATP', 'DLR', 'MKJ', 'AKAV', 'AKIM']
        })
        
        while True:
            await asyncio.sleep(3)
            for security, book in self.order_books.items():
                # Extract prices where quantity > 0 for printing
                sorted_bids = sorted((p, q) for p, q in book.bids.items() if q > 0)
                sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
                print(f"Bids for {security}:\n{sorted_bids}")
                print(f"Asks for {security}:\n{sorted_asks}")
    
    async def start(self, user_interface):
        #asyncio.create_task(self.trade())
        #asyncio.create_task(self.view_books())

        # This is where Phoenixhood will be launched if desired. There is no need to change these
        # lines, you can either remove the if or delete the whole thing depending on your purposes.
        if user_interface:
            self.launch_user_interface()
            asyncio.create_task(self.handle_queued_messages())

        await self.connect()
                
    ### THE FOLLOWING FUNCTIONS ARE LOANED AND MODIFIED FROM THE PARENTS CLASS
    ### in the utxchangelib we cannot modify the python files but we can overload its methods
    ### this needs to be done to access the full messages such as timestamp, prices and quantities
    ### not found otherwise, we should make sure to update these methods as the library is modified
    ### and cleared of bugs
                
    async def handle_book_snapshot(self, msg) -> None:
        """
        Update the books based on full snapshot from the exchange.
        :param msg: BookSnapshot message from the exchange
        """
        book = self.order_books[msg.symbol]
        book.bids = {bid.px: bid.qty for bid in msg.bids}
        book.asks = {ask.px: ask.qty for ask in msg.asks}

        if self.user_interface:
            requests.post('http://localhost:6060/updates', json={'update_type': 'book_snapshot', 'symbol': msg.symbol})

        #await self.bot_handle_book_update(msg.symbol, index)
                
        
    async def process_message(self, msg) -> None:
        """
        Identifies message type and calls the appropriate message handler.
        :param msg: ExchangeMessageToClient
        :return:
        """
        if msg == xchange_client.grpc.aio.EOF:
            xchange_client._LOGGER.info("End of GRPC stream. Shutting down.")

            # Need to terminate the react process here.
            exit(0)

        #near end of trading session display plots for analysis
        
        if msg.index >= 120000 and self.plot is False:
            xchange_client._LOGGER.info("plotting best bid ask")
            print(self.stock_LOB_timeseries)
            print("plotting best bid ask")
            self.plot = True
            await self.plot_best_bid_ask()
            exit(0)
        msg_type = msg.WhichOneof('body')
        # _LOGGER.info("Receieved message of type %s. index %d", msg_type, msg.index)
        # _LOGGER.info(msg)
        if msg_type not in ("book_snapshot", "book_update", "trade"):
            xchange_client._LOGGER.info("Receieved message of type %s. index %d", msg_type, msg.index)
            xchange_client._LOGGER.info(msg)
        if msg_type == "authenticated":
            self.handle_authenticate_response(msg.authenticated)
        elif msg_type == 'trade':
            await self.handle_trade_msg(msg.trade)
        elif msg_type == 'order_fill':
            await self.handle_order_fill(msg.order_fill)
        elif msg_type == 'order_rejected':
            await self.handle_order_rejected(msg.order_rejected)
        elif msg_type == 'cancel_response':
            await self.handle_cancel_response(msg.cancel_response)
        elif msg_type == 'swap_response':
            await self.handle_swap_response(msg.swap_response)
        elif msg_type == 'book_snapshot':
            await self.handle_book_snapshot(msg.book_snapshot)
        elif msg_type == 'book_update':
            await self.handle_book_update(msg.book_update, msg.index)
        elif msg_type == 'position_snapshot':
            self.handle_position_snapshot(msg.position_snapshot)
        elif msg_type == 'news_event':
            await self.handle_news_message(msg.news_event)
        elif msg_type == 'error':
            utcxchangelib._LOGGER.error(msg.error)
        return




async def main(user_interface: bool):
    # SERVER = '127.0.0.1:8000'   # run locally
    SERVER = '3.138.154.148:3333' # run on sandbox
    TEAMNAME = "yale_buffalo_rutgers_harvard"
    PASSWORD = "mre)2uJdR5"
    my_client = MyXchangeClient(SERVER,TEAMNAME,PASSWORD)
    await my_client.start(user_interface)
    return


if __name__ == "__main__":

    # This parsing is unnecessary if you know whether you are using Phoenixhood.
    # It is included here so you can see how one might start the API.

    parser = argparse.ArgumentParser(
        description="Script that connects client to exchange, runs algorithmic trading logic, and optionally deploys Phoenixhood"
    )

    parser.add_argument("--phoenixhood", required=False, default=False, type=bool, help="Starts phoenixhood API if true")
    args = parser.parse_args()

    user_interface = args.phoenixhood

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main(user_interface))




# %%
