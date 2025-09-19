import datetime as dt
import time
import random
import logging

from optibook.exporter import Exporter
from optibook.synchronous_client import Exchange

exporter = Exporter()
exchange = Exchange()
exchange.connect()

logging.getLogger("client").setLevel("ERROR")


def trade_would_breach_position_limit(instrument_id, volume, side, position_limit=100):
    positions = exchange.get_positions()
    position_instrument = positions[instrument_id]

    if side == "bid":
        return position_instrument + volume > position_limit
    elif side == "ask":
        return position_instrument - volume < -position_limit
    else:
        raise Exception(f"""Invalid side provided: {side}, expecting 'bid' or 'ask'.""")

pnl_history = []  # store pnl values here
position_history = []
initial_pnl = exchange.get_pnl()  # snapshot at start

def print_positions_and_pnl(always_display=None):
    positions = exchange.get_positions()
    print("Positions:")
    for instrument_id in positions:
        if (
            not always_display
            or instrument_id in always_display
            or positions[instrument_id] != 0
        ):
            print(f"  {instrument_id:20s}: {positions[instrument_id]:4.0f}")
            position_history.append(positions[instrument_id])

    
    pnl = exchange.get_pnl()
    pnl = pnl - initial_pnl
    pnl_history.append(pnl)
    

    if pnl:
        print(f"\nPnL: {pnl:.2f}")


INSTRUMENT_IDS = ["ASML","ASML_DUAL"]
total_time_sleep = 0

while total_time_sleep<3600:
    print(f"")
    print(f"-----------------------------------------------------------------")
    print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
    print(f"-----------------------------------------------------------------")

    print_positions_and_pnl(always_display=["ASML_DUAL"])
    print(f"")

    order_books = {}
    

    # Fetch all order books
    for stock_id in INSTRUMENT_IDS:
        book = exchange.get_last_price_book(stock_id)
        if book and book.bids and book.asks:
            order_books[stock_id] = book
            print(f"{stock_id}: Order book stored with {len(book.bids)} bids and {len(book.asks)} asks.")
        else:
            print(f"No valid order book for {stock_id}. Sleeping 1 second and retrying...")
            time.sleep(1)
            order_books = {}  # clear partial results
            break  # retry loop
    if len(order_books) == len(INSTRUMENT_IDS):
        ASML = order_books["ASML"]
        ASML_DUAL = order_books["ASML_DUAL"]

        print()
        print(f"ASML      top bid: {ASML.bids[0].price:.2f}, top ask: {ASML.asks[0].price:.2f}")
        print(f"ASML_DUAL top bid: {ASML_DUAL.bids[0].price:.2f}, top ask: {ASML_DUAL.asks[0].price:.2f}")

        #introduce small margin because sometimes there are bids of xx.000000006 which mess up the if statement
        epsilon = 0.0001
        volume=1

        if ASML.asks[0].price < ASML_DUAL.bids[0].price - epsilon:
                # Sell ASML_DUAL
                print("Opportunity: Sell ASML_DUAL @ bid")
                side = "ask"
                price = ASML_DUAL.bids[0].price
                if not trade_would_breach_position_limit("ASML_DUAL", volume, side):
                    print(f"Inserting {side} for ASML_DUAL: {volume} lot(s) at price {price:.2f}.")
                    exchange.insert_order(
                        instrument_id="ASML_DUAL",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
                        )
        elif ASML_DUAL.asks[0].price < ASML.bids[0].price - epsilon:
            print("Opportunity: Buy ASML_DUAL @ ask")
                    # Buy ASML_DUAL
            side = "bid"
            price = ASML_DUAL.asks[0].price
            
            if not trade_would_breach_position_limit("ASML_DUAL", volume, side):
                print(f"Inserting {side} for ASML_DUAL: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="ASML_DUAL",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
                    )

        else:
            print("No arbitrage opportunity.")
    
    print()
     # fetch all trades
    trades = exchange.get_trade_history('ASML_DUAL')
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade ASML_DUAL: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
        print("No trades yet for ASML_DUAL")

    

    print(f"\nSleeping for 1 second.")
    time.sleep(1)
    total_time_sleep = total_time_sleep+1

logrequest = { 
    "A1_1.csv": [
        ["PnL", "Position"],
       *[[p, pos] for p, pos in zip(pnl_history, position_history)] 
    ]
} 
exporter.reset()
exporter.export(logrequest)
