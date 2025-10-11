import datetime as dt
import time
import logging

from optibook.synchronous_client import Exchange
from optibook.common_types import InstrumentType, OptionKind

from math import floor, ceil

# region setup
import sys
import subprocess


def install_and_import(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    finally:
        globals()[package] = __import__(package)


install_and_import("scipy")
sys.path.append("/home/workspace/your_optiver_workspace")
# endregion

from common.black_scholes import call_value, put_value, call_delta, put_delta
from common.libs import calculate_current_time_to_date

exchange = Exchange()
exchange.connect()

logging.getLogger("client").setLevel("ERROR")


def round_down_to_tick(price, tick_size):
    """
    Rounds a price down to the nearest tick, e.g. if the tick size is 0.10, a price of 0.97 will get rounded to 0.90.
    """
    return floor(price / tick_size) * tick_size


def round_up_to_tick(price, tick_size):
    """
    Rounds a price up to the nearest tick, e.g. if the tick size is 0.10, a price of 1.34 will get rounded to 1.40.
    """
    return ceil(price / tick_size) * tick_size


def get_midpoint_value(instrument_id):
    """
    This function calculates the current midpoint of the order book supplied by the exchange for the instrument
    specified by <instrument_id>, returning None if either side or both sides do not have any orders available.
    """
    order_book = exchange.get_last_price_book(instrument_id=instrument_id)

    if not (order_book and order_book.bids and order_book.asks):
        return None
    else:
        midpoint = (order_book.bids[0].price + order_book.asks[0].price) / 2.0
        return midpoint


def calculate_theoretical_option_value(
    expiry, strike, option_kind, stock_value, interest_rate, volatility
):
    time_to_expiry = calculate_current_time_to_date(expiry)

    if option_kind == OptionKind.CALL:
        option_value = call_value(
            S=stock_value, K=strike, T=time_to_expiry, r=interest_rate, sigma=volatility
        )
    elif option_kind == OptionKind.PUT:
        option_value = put_value(
            S=stock_value, K=strike, T=time_to_expiry, r=interest_rate, sigma=volatility
        )

    return option_value


def calculate_option_delta(
    expiry_date, strike, option_kind, stock_value, interest_rate, volatility
):
    time_to_expiry = calculate_current_time_to_date(expiry_date)

    if option_kind == OptionKind.CALL:
        option_delta = call_delta(
            S=stock_value, K=strike, T=time_to_expiry, r=interest_rate, sigma=volatility
        )
    elif option_kind == OptionKind.PUT:
        option_delta = put_delta(
            S=stock_value, K=strike, T=time_to_expiry, r=interest_rate, sigma=volatility
        )
    else:
        raise Exception(
            f"""Got unexpected value for option_kind argument, should be OptionKind.CALL or OptionKind.PUT but was {option_kind}."""
        )

    return option_delta


def update_quotes(
    option_id, theoretical_price, credit, volume, position_limit, tick_size
):
    """
    This function updates the quotes specified by <option_id>.
    """

    # Print any new trades
    trades = exchange.poll_new_trades(instrument_id=option_id)
    for trade in trades:
        print(
            f"- Last period, traded {trade.volume} lots in {option_id} at price {trade.price:.2f}, side {trade.side}."
        )

    # Pull (remove) all existing outstanding orders
    orders = exchange.get_outstanding_orders(instrument_id=option_id)
    for order_id, order in orders.items():
        print(
            f"- Deleting old {order.side} order in {option_id} for {order.volume} @ {order.price:8.2f}."
        )
        exchange.delete_order(instrument_id=option_id, order_id=order_id)

    # Calculate bid and ask price
    bid_price = round_down_to_tick(theoretical_price - credit, tick_size)
    ask_price = round_up_to_tick(theoretical_price + credit, tick_size)

    # Calculate bid and ask volumes, taking into account the provided position_limit
    position = exchange.get_positions()[option_id]

    max_volume_to_buy = position_limit - position
    max_volume_to_sell = position_limit + position

    bid_volume = min(volume, max_volume_to_buy)
    ask_volume = min(volume, max_volume_to_sell)

    # Insert new limit orders
    if bid_volume > 0:
        print(
            f"- Inserting bid limit order in {option_id} for {bid_volume} @ {bid_price:8.2f}."
        )
        exchange.insert_order(
            instrument_id=option_id,
            price=bid_price,
            volume=bid_volume,
            side="bid",
            order_type="limit",
        )
    if ask_volume > 0:
        print(
            f"- Inserting ask limit order in {option_id} for {ask_volume} @ {ask_price:8.2f}."
        )
        exchange.insert_order(
            instrument_id=option_id,
            price=ask_price,
            volume=ask_volume,
            side="ask",
            order_type="limit",
        )


def hedge_delta_position(stock_id, options, stock_value):
    """
    Hedging hook (not implemented).
    """
    positions = exchange.get_positions()
    total_delta = 0.0

    for option_id, option in options.items():
        position = positions.get(option_id, 0)
        if position != 0:
            option_delta = calculate_option_delta(
                expiry_date=option.expiry,
                strike=option.strike,
                option_kind=option.option_kind,
                stock_value=stock_value,
                interest_rate=0.03,
                volatility=3,
            )
            total_delta += option_delta * position
            print(
                f"Option {option_id}: delta={option_delta:.4f}, "
                f"position={position}, delta*position={option_delta*position:.4f}"
            )

    print(f"\nTotal portfolio delta: {total_delta:.4f}")

    stock_position = positions[stock_id]
    print(f"- The current position in the stock {stock_id} is {stock_position}.")
    print(f"- Delta hedge not implemented. Doing nothing.")


def compute_portfolio_value(stock_id, options):
    # (kept for completeness, unused in metrics now)
    positions = exchange.get_positions()
    total_value = 0.0

    stock_pos = positions.get(stock_id, 0)
    stock_mid = get_midpoint_value(stock_id)
    if stock_mid:
        total_value += stock_pos * stock_mid

    for option_id, option in options.items():
        pos = positions.get(option_id, 0)
        mid = get_midpoint_value(option_id)
        if mid:
            total_value += pos * mid

    return total_value


def compute_total_delta(stock_id, options, stock_value, interest_rate=0.03, volatility=3.0):
    positions = exchange.get_positions()
    total_options_delta = 0.0
    breakdown = []
    for option_id, option in options.items():
        pos = positions.get(option_id, 0)
        d = calculate_option_delta(
            option.expiry, option.strike, option.option_kind,
            stock_value, interest_rate, volatility
        )
        contrib = pos * d
        total_options_delta += contrib
        breakdown.append((option_id, pos, d, contrib))
    stock_pos = positions.get(stock_id, 0)
    return total_options_delta + stock_pos, stock_pos, breakdown


def load_instruments_for_underlying(underlying_stock_id):
    all_instruments = exchange.get_instruments()
    stock = all_instruments[underlying_stock_id]
    options = {
        instrument_id: instrument
        for instrument_id, instrument in all_instruments.items()
        if instrument.instrument_type == InstrumentType.STOCK_OPTION
        and instrument.base_instrument_id == underlying_stock_id
    }
    return stock, options


# Load all instruments for use in the algorithm
STOCK_ID = "ASML"
stock, options = load_instruments_for_underlying(STOCK_ID)

import numpy as np
import csv
import os

# --- Performance tracking (simplified to use Exchange PnL only) ---
# We ONLY use the exchange's own PnL reading. No custom sums/Sharpe/etc.
# CSV stays the same file name you used before.
csv_filename = "A2_ii.csv"
if not os.path.exists(csv_filename):
    with open(csv_filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "pnl",              # <-- direct from exchange.get_pnl()
            "total_delta",
            "option_delta",
            "stock_position"
        ])

start_time = dt.datetime.utcnow()
while (dt.datetime.utcnow() - start_time).total_seconds() < 3600:
    print(f"")
    print(f"-----------------------------------------------------------------")
    print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
    print(f"-----------------------------------------------------------------")

    stock_value = get_midpoint_value(STOCK_ID)
    if stock_value is None:
        print("Empty stock order book on bid or ask-side, or both, unable to update option prices.")
        time.sleep(4)
        continue

    for option_id, option in options.items():
        print(f"\nUpdating instrument {option_id}")

        theoretical_value = calculate_theoretical_option_value(
            expiry=option.expiry,
            strike=option.strike,
            option_kind=option.option_kind,
            stock_value=stock_value,
            interest_rate=0.03,
            volatility=3.0,
        )

        update_quotes(
            option_id=option_id,
            theoretical_price=theoretical_value,
            credit=0.15,
            volume=3,
            position_limit=100,
            tick_size=0.10,
        )

        time.sleep(0.20)  # frequency guard

    # === PERFORMANCE & RISK LOGGING (PnL from Exchange only) ===
    now = dt.datetime.utcnow()
    try:
        current_pnl = exchange.get_pnl()  # <-- the only PnL we record
    except AttributeError:
        # In case your client uses a different method name, fail loudly with a clear hint.
        raise RuntimeError("Exchange object has no get_pnl(). If your API uses another name (e.g. getPnL / get_portfolio_pnl), replace it here.")

    # Context: delta & positions
    total_delta_now, stock_pos_now, delta_breakdown = compute_total_delta(
        stock_id=STOCK_ID, options=options, stock_value=stock_value
    )
    option_delta = sum(contrib for _, _, _, contrib in delta_breakdown)

    # Print (clean)
    print("\n===== PERFORMANCE METRICS =====")
    print(f"Timestamp: {now}")
    print(f"PnL (exchange): {current_pnl:.2f}")
    print(f"Total Δ: {total_delta_now:+.2f}")
    print(f"Option Δ: {option_delta:+.2f}")
    print(f"Stock Position: {stock_pos_now:+d}")
    print("================================")

    # Write to CSV (PnL only + context)
    with open(csv_filename, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            now.isoformat(),
            current_pnl,
            total_delta_now,
            option_delta,
            stock_pos_now
        ])

    print(f"\nHedging delta position")
    hedge_delta_position(STOCK_ID, options, stock_value)

    print(f"\nSleeping for 4 seconds.")
    time.sleep(4)
