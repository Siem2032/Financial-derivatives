#%%
import datetime as dt
import time
import math
import logging
import csv
import statistics
import os

from optibook.synchronous_client import Exchange

pnl_history = []
RISK_FREE_RATE = 0.02
THRESHOLD = 0.2
LOTS_PER_TRADE = 1  # Only one lot per trade

exchange = Exchange()
exchange.connect()

logging.getLogger("client").setLevel("ERROR")


def trade_would_breach_position_limit(instrument_id, volume, side, position_limit=100):
    positions = exchange.get_positions()
    position_instrument = positions[instrument_id]
    if volume == 0:
        return True
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


def get_mid_price(order_book):
    if order_book and order_book.bids and order_book.asks:
        best_bid = order_book.bids[0].price
        best_ask = order_book.asks[0].price
        return (best_bid + best_ask) / 2.0
    return None


def max_lots_allowed(instrument_id, side, position_limit=100):
    positions = exchange.get_positions()
    current_pos = positions.get(instrument_id, 0)
    if side == "bid":  # buying
        return max(position_limit - current_pos, 0)
    else:  # selling
        return max(position_limit + current_pos, 0)


#%%
print_positions_and_pnl()

#%%
csv_file = "C:\\Users\\kes.kromhout\\OneDrive - VINCI Energies\\Documenten\\FD_Assignment\\B3iii.csv"
write_header = not os.path.exists(csv_file)

with open(csv_file, "a", newline="") as f:
    writer = csv.writer(f)

    if write_header:
        writer.writerow([
            "run", "timestamp", "elapsed", "pnl_adj", "mean_pnl", "std_pnl",
            "MAIN_ID", "best_bid_main", "best_ask_main", "pos_main",
            "FUTURE_ID", "best_bid_future", "best_ask_future", "pos_dual",
            "successful_trades", "failed_trades"
        ])

    run = 0
    start_time = dt.datetime.utcnow()

    while True:
        run += 1
        now = dt.datetime.utcnow()
        elapsed = (now - start_time).total_seconds() / 60.0

        positions = exchange.get_positions()
        pos_main = positions.get("ASML", 0)
        pos_dual = positions.get("ASML_202512_F", 0)

        stock_book = exchange.get_last_price_book("ASML")
        future_book = exchange.get_last_price_book("ASML_202512_F")

        best_bid_main = stock_book.bids[0].price if stock_book.bids else 0
        best_ask_main = stock_book.asks[0].price if stock_book.asks else 0
        best_bid_future = future_book.bids[0].price if future_book.bids else 0
        best_ask_future = future_book.asks[0].price if future_book.asks else 0

        pnl = exchange.get_pnl() or 0
        pnl_history.append(pnl)

        mean_pnl = statistics.mean(pnl_history)
        std_pnl = statistics.stdev(pnl_history) if len(pnl_history) > 1 else 0
        pnl_adj = exchange.get_pnl() or 0

        print(f"\n{'-'*65}")
        print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
        print(f"{'-'*65}")
        print_positions_and_pnl()
        print()

        spot_price = get_mid_price(stock_book)
        future_price = get_mid_price(future_book)

        if not spot_price or not future_price:
            print("No prices available, skipping iteration.")
            time.sleep(0.1)
            continue

        maturity = dt.datetime(2025, 12, 31)
        T = (maturity - now).days / 365.0

        F_theoretical = spot_price * math.exp(RISK_FREE_RATE * T)

        print(f"Spot Price: {spot_price:.2f}, Futures Price: {future_price:.2f}, Theoretical F: {F_theoretical:.2f}")
        mispricing = future_price - F_theoretical

        if abs(mispricing) > THRESHOLD:
            if mispricing > 0:
                # Futures overpriced → SELL future, BUY stock
                if (
                    max_lots_allowed("ASML", "bid") >= 1 and
                    max_lots_allowed("ASML_202512_F", "ask") >= 1
                ):
                    exchange.insert_order("ASML", price=stock_book.asks[0].price,
                                          volume=1, side="bid", order_type="ioc")
                    exchange.insert_order("ASML_202512_F", price=future_book.bids[0].price,
                                          volume=1, side="ask", order_type="ioc")
                    print("Executed: BUY 1 stock, SELL 1 future")

            elif mispricing < 0:
                # Futures underpriced → BUY future, SELL stock
                if (
                    max_lots_allowed("ASML_202512_F", "bid") >= 1 and
                    max_lots_allowed("ASML", "ask") >= 1
                ):
                    exchange.insert_order("ASML_202512_F", price=future_book.asks[0].price,
                                          volume=1, side="bid", order_type="ioc")
                    exchange.insert_order("ASML", price=stock_book.bids[0].price,
                                          volume=1, side="ask", order_type="ioc")
                    print("Executed: SELL 1 stock, BUY 1 future")

        else:
            print("No significant mispricing. Waiting for next opportunity.")

        # --- DYNAMIC HEDGE ADJUSTMENT LOOP ---
        while True:
            positions = exchange.get_positions()
            stock_pos = positions.get("ASML", 0)
            future_pos = positions.get("ASML_202512_F", 0)

            hedge_diff = stock_pos + future_pos

            if hedge_diff == 0:
                print("Stock and futures positions are correctly hedged.")
                break

            if hedge_diff > 0:
                max_stock_sell = max_lots_allowed("ASML", "ask")
                if max_stock_sell >= 1:
                    exchange.insert_order("ASML", price=stock_book.bids[0].price,
                                          volume=1, side="ask", order_type="ioc")
                    print("Hedge adjustment: SOLD 1 stock to reduce net long exposure")
                else:
                    print("Cannot sell more stock due to position limits.")
                    break

            else:
                max_stock_buy = max_lots_allowed("ASML", "bid")
                if max_stock_buy >= 1:
                    exchange.insert_order("ASML", price=stock_book.asks[0].price,
                                          volume=1, side="bid", order_type="ioc")
                    print("Hedge adjustment: BOUGHT 1 stock to reduce net short exposure")
                else:
                    print("Cannot buy more stock due to position limits.")
                    break

            time.sleep(0.1)

        # ---- Log each loop ----
        writer.writerow([
            run,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed,
            pnl_adj,
            mean_pnl,
            std_pnl,
            "ASML",
            best_bid_main,
            best_ask_main,
            pos_main,
            "ASML_202512_F",
            best_bid_future,
            best_ask_future,
            pos_dual,
        ])
        f.flush()

        time.sleep(0.1)

#%%
exchange.insert_order("ASML_202606_F", price=exchange.get_last_price_book("ASML_202606_F").bids[0].price,
                      volume=1, side="ask", order_type="ioc")

# %%
