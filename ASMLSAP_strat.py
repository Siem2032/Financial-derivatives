import datetime as dt
import time
import logging
from optibook.synchronous_client import Exchange

exchange = Exchange()
exchange.connect()

logging.getLogger("client").setLevel("ERROR")
INSTRUMENT_IDS = ["SAP", "SAP_DUAL","ASML","ASML_DUAL"]

epsilon = 0.0001  # ignore tiny differences
volume=20

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

    print_positions_and_pnl(always_display=["SAP", "SAP_DUAL","ASML","ASML_DUAL"])
    

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
        ASML = order_books["ASML"]
        ASML_DUAL = order_books["ASML_DUAL"]

        print()
        print(f"SAP      top bid: {SAP.bids[0].price:.2f}, top ask: {SAP.asks[0].price:.2f}")
        print(f"SAP_DUAL top bid: {SAP_DUAL.bids[0].price:.2f}, top ask: {SAP_DUAL.asks[0].price:.2f}")
        

        # Arbitrage checks with epsilon tolerance
        if SAP.asks[0].price < SAP_DUAL.bids[0].price - epsilon:
            print("Opportunity: Buy SAP @ ask, Sell SAP_DUAL @ bid")
            side = "bid"
            price = SAP.asks[0].price
            sap_ask_vol = SAP.asks[0].volume
            sap_dual_bid_vol = SAP_DUAL.bids[0].volume
            
            volume = min(sap_ask_vol, sap_dual_bid_vol, 30)
            # Buy SAP
            if not trade_would_breach_position_limit("SAP", volume, side):
                print(f"Inserting {side} for SAP: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="SAP",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
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
                    order_type="ioc"
                    )

        elif SAP_DUAL.asks[0].price < SAP.bids[0].price - epsilon:
            print("Opportunity: Buy SAP_DUAL @ ask, Sell SAP @ bid")
                     # Buy SAP_DUAL
            side = "bid"
            price = SAP_DUAL.asks[0].price
            sap_bid_vol = SAP.bids[0].volume
            sap_dual_ask_vol = SAP_DUAL.asks[0].volume
            
            volume = min(sap_bid_vol, sap_dual_ask_vol, 30)
            if not trade_would_breach_position_limit("SAP_DUAL", volume, side):
                print(f"Inserting {side} for SAP_DUAL: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="SAP_DUAL",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
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
                        order_type="ioc"
                    )
        else:
            print("No arbitrage opportunity.")

    

        # NOW do the same for ASML 
        print()
        print(f"ASML      top bid: {ASML.bids[0].price:.2f}, top ask: {ASML.asks[0].price:.2f}")
        print(f"ASML_DUAL top bid: {ASML_DUAL.bids[0].price:.2f}, top ask: {ASML_DUAL.asks[0].price:.2f}")

        if ASML.asks[0].price < ASML_DUAL.bids[0].price - epsilon:
            print("Opportunity: Buy ASML @ ask, Sell ASML_DUAL @ bid")
            side = "bid"
            price = ASML.asks[0].price
            ASML_ask_vol = ASML.asks[0].volume
            ASML_dual_bid_vol = ASML_DUAL.bids[0].volume
            
            volume = min(ASML_ask_vol, ASML_dual_bid_vol, 30)
            # Buy ASML
            if not trade_would_breach_position_limit("ASML", volume, side):
                print(f"Inserting {side} for ASML: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="ASML",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
                    )
            # Sell ASML_DUAL
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
            print("Opportunity: Buy ASML_DUAL @ ask, Sell ASML @ bid")
                    # Buy ASML_DUAL
            side = "bid"
            price = ASML_DUAL.asks[0].price
            ASML_bid_vol = ASML.bids[0].volume
            ASML_dual_ask_vol = ASML_DUAL.asks[0].volume
            
            volume = min(ASML_bid_vol, ASML_dual_ask_vol, 30)
            if not trade_would_breach_position_limit("ASML_DUAL", volume, side):
                print(f"Inserting {side} for ASML_DUAL: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="ASML_DUAL",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
                    )
                    # Sell ASML
            side = "ask"
            price = ASML.bids[0].price
            if not trade_would_breach_position_limit("ASML", volume, side):
                print(f"Inserting {side} for ASML: {volume} lot(s) at price {price:.2f}.")
                exchange.insert_order(
                        instrument_id="ASML",
                        price=price,
                        volume=volume,
                        side=side,
                        order_type="ioc"
                    )
        else:
            print("No arbitrage opportunity.")

    print()
    trades = exchange.get_trade_history('SAP')  # fetch all trades
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade SAP: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
         print("No trades yet for SAP")
         trades = exchange.get_trade_history('SAP')  # fetch all trades
    trades = exchange.get_trade_history('SAP_DUAL')
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade SAP_DUAL: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
         print("No trades yet for SAP_DUAL")

    trades = exchange.get_trade_history('ASML')  # fetch all trades
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade ASML: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
        print("No trades yet for ASML")
        trades = exchange.get_trade_history('ASML')  # fetch all trades
    trades = exchange.get_trade_history('ASML_DUAL')
    if trades:
        last_trade = trades[-1]  # get the most recent trade
        print(f"Last trade ASML_DUAL: Price={last_trade.price}, Volume={last_trade.volume}, Side={last_trade.side}, Timestamp={last_trade.timestamp}")
    else:
        print("No trades yet for ASML_DUAL")
    
        # Sleep between iterations so you can follow the output

    print("\nSleeping 5 seconds before next iteration...\n")
        
    time.sleep(0.1)
