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
    
    pnl_timeseries = pl.DataFrame(schema={
        "timestamp": pl.Int64,
        "pnl": pl.Int64
    })
    
    stock_LOB_timeseries = { 
        "APT": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64,
            "2_bid_px": pl.Int64,
            "2_bid_qt": pl.Int64,
            "3_bid_px": pl.Int64,
            "3_bid_qt": pl.Int64,
            "4_bid_px": pl.Int64,
            "4_bid_qt": pl.Int64,
            "2_ask_px": pl.Int64,
            "2_ask_qt": pl.Int64,
            "3_ask_px": pl.Int64,
            "3_ask_qt": pl.Int64,
            "4_ask_px": pl.Int64,
            "4_ask_qt": pl.Int64,
        }),
        "DLR": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64,
            "2_bid_px": pl.Int64,
            "2_bid_qt": pl.Int64,
            "3_bid_px": pl.Int64,
            "3_bid_qt": pl.Int64,
            "4_bid_px": pl.Int64,
            "4_bid_qt": pl.Int64,
            "2_ask_px": pl.Int64,
            "2_ask_qt": pl.Int64,
            "3_ask_px": pl.Int64,
            "3_ask_qt": pl.Int64,
            "4_ask_px": pl.Int64,
            "4_ask_qt": pl.Int64,
        }),
        "MKJ": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64,
            "2_bid_px": pl.Int64,
            "2_bid_qt": pl.Int64,
            "3_bid_px": pl.Int64,
            "3_bid_qt": pl.Int64,
            "4_bid_px": pl.Int64,
            "4_bid_qt": pl.Int64,
            "2_ask_px": pl.Int64,
            "2_ask_qt": pl.Int64,
            "3_ask_px": pl.Int64,
            "3_ask_qt": pl.Int64,
            "4_ask_px": pl.Int64,
            "4_ask_qt": pl.Int64,
        }),
        "AKAV": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64,
            "2_bid_px": pl.Int64,
            "2_bid_qt": pl.Int64,
            "3_bid_px": pl.Int64,
            "3_bid_qt": pl.Int64,
            "4_bid_px": pl.Int64,
            "4_bid_qt": pl.Int64,
            "2_ask_px": pl.Int64,
            "2_ask_qt": pl.Int64,
            "3_ask_px": pl.Int64,
            "3_ask_qt": pl.Int64,
            "4_ask_px": pl.Int64,
            "4_ask_qt": pl.Int64,
        }),
        "AKIM": pl.DataFrame(schema={
            "timestamp": pl.Int64,
            "best_bid_px": pl.Int64,
            "best_bid_qt": pl.Int64,
            "best_ask_px": pl.Int64,
            "best_ask_qt": pl.Int64,
            "2_bid_px": pl.Int64,
            "2_bid_qt": pl.Int64,
            "3_bid_px": pl.Int64,
            "3_bid_qt": pl.Int64,
            "4_bid_px": pl.Int64,
            "4_bid_qt": pl.Int64,
            "2_ask_px": pl.Int64,
            "2_ask_qt": pl.Int64,
            "3_ask_px": pl.Int64,
            "3_ask_qt": pl.Int64,
            "4_ask_px": pl.Int64,
            "4_ask_qt": pl.Int64,
        }),
    }
    
    # Thread management
    _compute_threads = {}

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        self.start_time = int(time.time())  # Changed from milliseconds to seconds

        
        # Initialize specialized compute bots with self as parent
        self.compute_bots = {
            "APT": APTBot(self),
            "DLR": DLRBot(self),
            "MKJ": MKJBot(self),
            "AKIM_AKAV": AKIMAKAVBot(self)
        }
        
        # Create message queues for each symbol
        self.compute_queues = {
            "APT": queue.Queue(),
            "DLR": queue.Queue(),
            "MKJ": queue.Queue(),
            "AKIM": queue.Queue(),
            "AKAV": queue.Queue()
        }

        self.start_time = int(time.time())  # Changed from milliseconds to seconds
        
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


    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str]) -> None:
        order = self.open_orders[order_id]
        print(f"{'Market' if order[2] else 'Limit'} Order ID {order_id} cancelled, {order[1]} unfilled")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print("order fill", self.positions)

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        print("order rejected because of ", reason)


    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        """
        Handle trade messages by sending them to the appropriate compute thread.
        """
        if symbol in self.stock_LOB_timeseries:
            # Let data bot process the update first
            if symbol == "AKIM" or symbol == "AKAV":
                await self.compute_bots["AKIM_AKAV"].bot_handle_trade_msg(symbol, price, qty)
            else:
                await self.compute_bots[symbol].bot_handle_trade_msg(symbol, price, qty)
    

    async def bot_handle_book_update(self, symbol: str):
        if symbol in self.stock_LOB_timeseries:
            # Let data bot process the update first
            if symbol == "AKIM" or symbol == "AKAV":
                self.compute_bots["AKIM_AKAV"].increment_trade()
            else:
                self.compute_bots[symbol].increment_trade()

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        pass

    async def bot_handle_news(self, news_release: dict):
        # Parsing the message based on what type was received
        timestamp = news_release["timestamp"] # This is in exchange ticks not ISO or Epoch
        news_type = news_release['kind']
        news_data = news_release["new_data"]
        
        print(news_data)

        if news_type == "structured":
            subtype = news_data["structured_subtype"]
            symb = news_data["asset"]
            if subtype == "earnings":
                
                earnings = news_data["value"]

                self.compute_bots["APT"].handle_earnings_update(earnings)
                
            else:
                new_signatures = news_data["new_signatures"]
                cumulative = news_data["cumulative"]
                self.compute_bots["DLR"].signature_update(new_signatures, cumulative)
        else:
            for bot in self.compute_bots.values():
                bot.unstructured_update(news_data)
    

    async def plot_best_bid_ask(self):
        for symbol, df in self.stock_LOB_timeseries.items():
            plt.figure(figsize=(12, 6))
            
            # Thread-safe read of timeseries data
            with self._lock:
                timestamp = df["timestamp"].to_list()
                best_bid_px = df["best_bid_px"].to_list()
                best_ask_px = df["best_ask_px"].to_list()
                bid_ask_spread = [ask - bid for ask, bid in zip(best_ask_px, best_bid_px)]
            
            plt.subplot(2, 1, 1)
            plt.plot(timestamp, best_bid_px, label="Best Bid Price", linestyle="-",markersize=1)
            plt.plot(timestamp, best_ask_px, label="Best Ask Price", linestyle="-",markersize=1)

            plt.legend(["Best Bid Price", "Best Ask Price"])
            plt.grid(True)
            plt.xticks(rotation=45)

            plt.subplot(2, 1, 2)
            plt.plot(timestamp, bid_ask_spread, label="Bid Ask Spread", linestyle="-", markersize=1)
            plt.legend("Bid Ask Spread")
            plt.grid(True)
            plt.xticks(rotation=45)

            # Show plot
            print(f"Saving figure for {symbol}")
            plt.tight_layout()
            plt.savefig(f"data/best_bid_ask_{symbol}.png")
            plt.close()

    async def trade(self):
        await asyncio.sleep(15)
        print("attempting to trade")
        # buy 20 shares of APT
        await self.place_order("APT",1, xchange_client.Side.BUY, int(9))
        await asyncio.sleep(5)
        with self._lock: 
            latest_timestamp = self.stock_LOB_timeseries["APT"].select("timestamp").max().item()
            print("type of latest_timestamp: ", type(latest_timestamp))
            bid_price, ask_price = self.compute_bots["APT"].calc_bid_ask_price(latest_timestamp)
        print("========================================")
        print("Adjusted Bid Price:", bid_price)
        await self.place_order("APT",self.compute_bots["APT"].q_tilde, xchange_client.Side.BUY, bid_price)
        print("Adjusted Ask Price:", ask_price)
        await self.place_order("APT",self.compute_bots["APT"].q_tilde, xchange_client.Side.SELL, ask_price)
        print("my positions:", self.positions)
        
    def calculate_unrealized_pnl(self, symbol_position, sorted_bids, sorted_asks):
        unrealized_pnl = 0
        if symbol_position > 0:  # Long position
            # Use best bid price (highest price we could sell at)
            if sorted_bids:
                # Calculate weighted average price if position is larger than best bid quantity
                remaining_position = symbol_position
                weighted_price = 0
                total_quantity = 0
                
                for bid_price, bid_qty in sorted_bids:
                    if remaining_position <= 0:
                        break
                        
                    # Use the smaller of the remaining position or the bid quantity
                    qty_to_use = min(remaining_position, bid_qty)
                    weighted_price += bid_price * qty_to_use
                    total_quantity += qty_to_use
                    remaining_position -= qty_to_use
                
                if total_quantity > 0:
                    avg_price = weighted_price / total_quantity
                    unrealized_pnl = symbol_position * avg_price
                else:
                    # Fallback to best bid if we couldn't calculate a weighted average
                    unrealized_pnl = symbol_position * sorted_bids[0][0]
                    
        elif symbol_position < 0:  # Short position
            # Use best ask price (lowest price we could buy at)
            if sorted_asks:
                # Calculate weighted average price if position is larger than best ask quantity
                remaining_position = abs(symbol_position)
                weighted_price = 0
                total_quantity = 0
                
                for ask_price, ask_qty in sorted_asks:
                    if remaining_position <= 0:
                        break
                        
                    # Use the smaller of the remaining position or the ask quantity
                    qty_to_use = min(remaining_position, ask_qty)
                    weighted_price += ask_price * qty_to_use
                    total_quantity += qty_to_use
                    remaining_position -= qty_to_use
                
                if total_quantity > 0:
                    avg_price = weighted_price / total_quantity
                    unrealized_pnl = symbol_position * avg_price
                else:
                    # Fallback to best ask if we couldn't calculate a weighted average
                    unrealized_pnl = symbol_position * sorted_asks[0][0]
        return unrealized_pnl

    async def view_books(self):
        # Use polars DataFrame for better performance
        
        while True:
            await asyncio.sleep(1)
            pnl = self.positions['cash']
            
            for symbol, book in self.order_books.items():
                # Extract prices where quantity > 0 for printing
                sorted_bids = sorted(((p, q) for p, q in book.bids.items() if q > 0), reverse=True)
                sorted_asks = sorted((p, q) for p, q in book.asks.items() if q > 0)
                
                # Calculate unrealized PnL for this symbol
                symbol_position = self.positions.get(symbol, 0)
                unrealized_pnl = self.calculate_unrealized_pnl(symbol_position, sorted_bids, sorted_asks)
                pnl += unrealized_pnl
                
                
                
                # Create a new row with the first 3 levels of bids and asks
                if symbol in self.stock_LOB_timeseries:
                    # Get current timestamp
                    current_time = int(time.time()) - self.start_time 
                    
                    # Create row data with first 3 levels
                    row_data = {
                        "timestamp": current_time,
                        "best_bid_px": sorted_bids[0][0] if sorted_bids else 0,
                        "best_bid_qt": sorted_bids[0][1] if sorted_bids else 0,
                        "best_ask_px": sorted_asks[0][0] if sorted_asks else 0,
                        "best_ask_qt": sorted_asks[0][1] if sorted_asks else 0,
                    }
                    
                    # Add second, third, and fourth levels if available
                    if len(sorted_bids) > 1:
                        row_data["2_bid_px"] = sorted_bids[1][0]
                        row_data["2_bid_qt"] = sorted_bids[1][1]
                    else:
                        row_data["2_bid_px"] = 0
                        row_data["2_bid_qt"] = 0
                    if len(sorted_bids) > 2:
                        row_data["3_bid_px"] = sorted_bids[2][0]
                        row_data["3_bid_qt"] = sorted_bids[2][1]
                    else:
                        row_data["3_bid_px"] = 0
                        row_data["3_bid_qt"] = 0
                    if len(sorted_bids) > 3:
                        row_data["4_bid_px"] = sorted_bids[3][0]
                        row_data["4_bid_qt"] = sorted_bids[3][1]
                    else:
                        row_data["4_bid_px"] = 0
                        row_data["4_bid_qt"] = 0
                        
                    if len(sorted_asks) > 1:
                        row_data["2_ask_px"] = sorted_asks[1][0]
                        row_data["2_ask_qt"] = sorted_asks[1][1]
                    else:
                        row_data["2_ask_px"] = 0
                        row_data["2_ask_qt"] = 0   
                    if len(sorted_asks) > 2:
                        row_data["3_ask_px"] = sorted_asks[2][0]
                        row_data["3_ask_qt"] = sorted_asks[2][1]
                    else:
                        row_data["3_ask_px"] = 0
                        row_data["3_ask_qt"] = 0
                    if len(sorted_asks) > 3:
                        row_data["4_ask_px"] = sorted_asks[3][0]
                        row_data["4_ask_qt"] = sorted_asks[3][1]
                    else:
                        row_data["4_ask_px"] = 0
                        row_data["4_ask_qt"] = 0
                    
                    # Create new row DataFrame
                    new_row = pl.DataFrame([row_data])
                    
                    #print("new_row: ", new_row)
                    
                    # Thread-safe update to the timeseries
                    with self._lock:
                        self.stock_LOB_timeseries[symbol] = pl.concat([
                            self.stock_LOB_timeseries[symbol],
                            new_row
                        ])
            
            # Update PnL timeseries with total unrealized PnL
            current_time = int(time.time()) - self.start_time
            pnl_row = pl.DataFrame([{
                "timestamp": current_time,
                "pnl": pnl
            }])
            
            with self._lock:
                self.pnl_timeseries = pl.concat([
                    self.pnl_timeseries,
                    pnl_row
                ])

    async def plot_pnl(self):
        plt.figure(figsize=(12, 6))
        
        # Thread-safe read of timeseries data
        with self._lock:
            timestamp = self.pnl_timeseries["timestamp"].to_list()
            pnl = self.pnl_timeseries["pnl"].to_list()
            
        plt.plot(timestamp, pnl, label="PnL", linestyle="-", markersize=1)
        plt.legend(["PnL"])
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.title("Total PnL (Realized + Unrealized)")
        
        # Show plot
        print("Saving PnL figure")
        plt.tight_layout()
        plt.savefig("data/pnl.png")
        plt.close()

    async def start(self, user_interface):
        # Start compute threads
        self.start_compute_threads()
        asyncio.create_task(self.trade())
        asyncio.create_task(self.view_books())
        
        # This is where Phoenixhood will be launched if desired.
        if user_interface:
            self.launch_user_interface()
            asyncio.create_task(self.handle_queued_messages())

        try:
            await self.connect()
        finally:
            # Ensure compute threads are stopped when the main coroutine exits
            self.stop_compute_threads()
        



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
        await my_client.plot_pnl()
    
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
