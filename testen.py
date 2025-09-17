import datetime as dt
import time
import logging
from optibook.synchronous_client import Exchange

exchange = Exchange()
exchange.connect()

logging.getLogger("client").setLevel("ERROR")
INSTRUMENT_IDS = ["SAP", "SAP_DUAL"]

epsilon = 0.0001  # ignore tiny differences
volume=100

def trade_would_breach_position_limit(instrument_id, volume, side, position_limit=100):
    positions = exchange.get_positions()
    position_instrument = positions[instrument_id]

    if side == "bid":
        return position_instrument + volume > position_limit
    elif side == "ask":
        return position_instrument - volume < -position_limit
    else:
        raise Exception(f"""Invalid side provided: {side}, expecting 'bid' or 'ask'.""")


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

    pnl = exchange.get_pnl()
    if pnl:
        print(f"\nPnL: {pnl:.2f}")


while True:
    print(f"\n-----------------------------------------------------------------")
    print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now())} UTC.")
    print(f"-----------------------------------------------------------------")

    print_positions_and_pnl(always_display=["SAP", "SAP_DUAL"])
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

    # Only proceed if both books are valid
    if len(order_books) == len(INSTRUMENT_IDS):
        SAP = order_books["SAP"]
        SAP_DUAL = order_books["SAP_DUAL"]

        print(f"SAP      top bid: {SAP.bids[0].price:.2f}, top ask: {SAP.asks[0].price:.2f}")
        print(f"SAP_DUAL top bid: {SAP_DUAL.bids[0].price:.2f}, top ask: {SAP_DUAL.asks[0].price:.2f}")

        # Arbitrage checks with epsilon tolerance
        if SAP.asks[0].price < SAP_DUAL.bids[0].price - epsilon:
            print("Opportunity: Buy SAP @ ask, Sell SAP_DUAL @ bid")
            side = "bid"
            price = SAP.asks[0].price
            # Buy SAP
            if not trade_would_breach_position_limit("SAP", volume, side):
                print(f"Inserting {side} for SAP: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="SAP",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="limit"
                    )
            # Sell SAP_DUAL
            side = "ask"
            price = SAP_DUAL.bids[0].price
            if not trade_would_breach_position_limit("SAP_DUAL", volume, side):
                print(f"Inserting {side} for SAP_DUAL: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                    instrument_id="SAP_DUAL",
                    price=price,
                    volume=volume,
                    side=side,
                    order_type="limit"
                    )

        elif SAP_DUAL.asks[0].price < SAP.bids[0].price - epsilon:
            print("Opportunity: Buy SAP_DUAL @ ask, Sell SAP @ bid")
                     # Buy SAP_DUAL
            side = "bid"
            price = SAP_DUAL.asks[0].price
            if not trade_would_breach_position_limit("SAP_DUAL", volume, side):
                print(f"Inserting {side} for SAP_DUAL: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="SAP_DUAL",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="limit"
                    )
                    # Sell SAP
            side = "ask"
            price = SAP.bids[0].price
            if not trade_would_breach_position_limit("SAP", volume, side):
                print(f"Inserting {side} for SAP: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="SAP",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="limit"
                    )
        else:
            print("No arbitrage opportunity.")

    trades = exchange.get_trade_history('SAP')  # fetch all trades
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
         print("No trades yet for ASML")
         trades = exchange.get_trade_history('SAP')  # fetch all trades
    trades = exchange.get_trade_history('SAP_DUAL')
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
         print("No trades yet for ASML")
    # Sleep between iterations so you can follow the output
    # Sleep between iterations so you can follow the output

    print("\nSleeping 5 seconds before next iteration...\n")
    time.sleep(1)

    

