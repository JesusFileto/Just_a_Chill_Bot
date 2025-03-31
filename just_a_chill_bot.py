#%% 

from typing import Optional
import threading
import utcxchangelib
from utcxchangelib import xchange_client
import asyncio
import argparse
import polars as pl
import heapq
import matplotlib.pyplot as plt
from computebot import Compute
from readbot import DataIngestion
from apt_bot import APTBot
from dlr_bot import DLRBot
from mkj_bot import MKJBot
from akim_akav_bot import AKIMAKAVBot
import concurrent.futures
import time
import queue
import signal
import sys


class ComputeThread(threading.Thread):
    """A thread class that runs an asyncio event loop for compute bots"""
    
    def __init__(self, name, bot, queue):
        super().__init__(name=name, daemon=True)
        self.bot = bot
        self.message_queue = queue
        self.stop_event = threading.Event()
        self.loop = None
        
    def run(self):
        """Run the asyncio event loop in this thread"""
        # Create a new event loop for this thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            # Run the compute bot coroutine
            self.loop.run_until_complete(self._process_updates())
        except Exception as e:
            print(f"{self.name}: Error in thread: {e}")
        finally:
            # Clean up
            self.loop.close()
            print(f"{self.name}: Thread and event loop stopped")
            
    def stop(self):
        """Signal the thread to stop"""
        self.stop_event.set()
        # If we're waiting on the queue, this will unblock it
        self.message_queue.put(None)
        
    async def _process_updates(self):
        """Process updates from the message queue"""
        print(f"{self.name}: Started processing")
        
        while not self.stop_event.is_set():
            try:
                # Wait for an update with asyncio to allow cancellation
                index = await self._get_update_async()
                
                # None is a signal to check the stop_event
                if index is None:
                    continue
             
                # Delegate to the bot's process_update method
                # Each bot class should implement this method
                await self.bot.process_update(index)
                
                # Mark the task as done in the queue (if using standard Queue)
                if hasattr(self.message_queue, 'task_done'):
                    self.message_queue.task_done()
                
            except Exception as e:
                print(f"{self.name}: Error processing update: {e}")
                
        print(f"{self.name}: Stopped processing updates")
        
    async def _get_update_async(self):
        """Get an update from the queue using asyncio"""
        # Use run_in_executor to move the blocking queue.get to a thread pool
        # This allows us to cancel it if needed
        loop = asyncio.get_event_loop()
        
        def get_from_queue():
            try:
                # Use a timeout to allow checking the stop event
                return self.message_queue.get(timeout=0.5)
            except queue.Empty:
                return None
                
        while not self.stop_event.is_set():
            update = await loop.run_in_executor(None, get_from_queue)
            if update is not None or self.stop_event.is_set():
                return update
                
        return None
        


class MyXchangeClient(xchange_client.XChangeClient):
    plot = False
    # Thread lock for synchronizing access to shared data
    _lock = threading.Lock()
    
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
    
    # Thread management
    _compute_threads = {}

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        # Initialize data ingestion bot
        self.data_bot = DataIngestion(self)
        
        # Initialize specialized compute bots
        self.compute_bots = {
            "APT": APTBot(),
            "DLR": DLRBot(),
            "MKJ": MKJBot(),
            "AKIM_AKAV": AKIMAKAVBot()
        }
        
        # Create message queues for each symbol
        self.compute_queues = {
            "APT": queue.Queue(),
            "DLR": queue.Queue(),
            "MKJ": queue.Queue(),
            "AKIM": queue.Queue(),
            "AKAV": queue.Queue()
        }
        
    def start_compute_threads(self):
        """Start separate compute threads, each with its own asyncio event loop"""
        # Create and start compute threads
        self._compute_threads["apt_thread"] = ComputeThread(
            name="APT-ComputeThread",
            bot=self.compute_bots["APT"],
            queue=self.compute_queues["APT"]
        )
        
        self._compute_threads["dlr_thread"] = ComputeThread(
            name="DLR-ComputeThread",
            bot=self.compute_bots["DLR"],
            queue=self.compute_queues["DLR"]
        )
        
        self._compute_threads["mkj_thread"] = ComputeThread(
            name="MKJ-ComputeThread",
            bot=self.compute_bots["MKJ"],
            queue=self.compute_queues["MKJ"]
        )
        
        # AKIM/AKAV bot will handle both symbols
        self._compute_threads["akim_akav_thread"] = ComputeThread(
            name="AKIM-AKAV-ComputeThread",
            bot=self.compute_bots["AKIM_AKAV"],
            queue=self.compute_queues["AKIM"]  # Use AKIM queue as primary
        )
        
        # Start all threads
        for thread_name, thread in self._compute_threads.items():
            thread.start()
            print(f"Started compute thread: {thread.name}")
            
        print(f"All {len(self._compute_threads)} compute threads started")
    
    def stop_compute_threads(self):
        """Signal all compute threads to stop and wait for them to finish"""
        print("Stopping all compute threads...")
        
        # Signal all threads to stop
        for thread_name, thread in self._compute_threads.items():
            thread.stop()
            
        # Wait for all threads to finish
        for thread_name, thread in self._compute_threads.items():
            if thread.is_alive():
                print(f"Waiting for {thread.name} to finish...")
                thread.join(timeout=3)
                
                if thread.is_alive():
                    print(f"WARNING: {thread.name} did not stop cleanly")
                else:
                    print(f"Successfully stopped {thread.name}")
                    
        # Clear thread dictionary
        self._compute_threads.clear()
        print("All compute threads stopped")

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

        # Triggers server side event to update book in user interface
        if self.user_interface:
            utcxchangelib.requests.post('http://localhost:6060/updates', json={'update_type': 'book_update', 'symbol': msg.symbol, "is_bid": is_bid})

        # Process the book update with the data bot
        symbol = msg.symbol
        if symbol in self.stock_timeseries:
            # Let data bot process the update first
            await self.data_bot.bot_handle_book_update(msg, index)
            
            
            # If we have data, send it to the appropriate compute queue
            try:
                # Create an update tuple with (symbol, timestamp, data)
                
                # Put the update in the symbol's queue
                if symbol in self.compute_queues:
                    self.compute_queues[symbol].put(index)
                    
                    # For AKIM and AKAV, also send to the combined bot if it's not the primary queue
                    if symbol == "AKAV":
                        self.compute_queues["AKIM"].put(index)
                        
            except Exception as e:
                print(f"Error queueing update for {symbol}: {e}")

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str]) -> None:
        order = self.open_orders[order_id]
        print(f"{'Market' if order[2] else 'Limit'} Order ID {order_id} cancelled, {order[1]} unfilled")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print("order fill", self.positions)

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        print("order rejected because of ", reason)


    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
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
    

    async def plot_best_bid_ask(self):
        for symbol, df in self.stock_timeseries.items():
            plt.figure(figsize=(12, 6))
            
            # Thread-safe read of timeseries data
            with self._lock:
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
        # Start compute threads
        self.start_compute_threads()
        
        # This is where Phoenixhood will be launched if desired.
        if user_interface:
            self.launch_user_interface()
            asyncio.create_task(self.handle_queued_messages())

        try:
            await self.connect()
        finally:
            # Ensure compute threads are stopped when the main coroutine exits
            self.stop_compute_threads()
                
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
            
            # Plot before exiting
            xchange_client._LOGGER.info("Plotting best bid ask before shutdown")
            await self.plot_best_bid_ask()
            
            # Need to terminate the react process here.
            exit(0)

        #near end of trading session display plots for analysis
        
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
    
    try:
        await my_client.start(user_interface)
    finally:
        # Plot data at the end of trading
        await my_client.plot_best_bid_ask()
    
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
    try:
        result = loop.run_until_complete(main(user_interface))
    finally:
        loop.close()

# %%
