

#%%
import datetime as dt
import time
import random
import logging
import math
import csv
import statistics
from optibook.synchronous_client import Exchange
r = 0.03
exchange = Exchange()
exchange.connect()
pnl_history = []
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


    #%%
    NEAR_ID = "ASML_202603_F"
    FAR_ID = "ASML_202606_F"

   
    #%%
    print_positions_and_pnl()
#%%
run = 0
start_time = dt.datetime.utcnow()
csv_file = "C:\\Users\\kes.kromhout\\OneDrive - VINCI Energies\\Documenten\\FD_Assignment\\B3iii.csv"
write_header = not os.path.exists(csv_file)

# Open the CSV file once and append in each loop
with open(csv_file, "a", newline="") as f:
    writer = csv.writer(f)
    
    # Write header if file is new
    if write_header:
        writer.writerow([
            "run", "timestamp", "elapsed", "pnl_adj", "mean_pnl", "std_pnl",
            "MAIN_ID", "best_bid_main", "best_ask_main", "pos_main",
            "FUTURE_ID", "best_bid_future", "best_ask_future", "pos_dual",
            "successful_trades", "failed_trades"
        ])
    while True:
        run += 1
        now = dt.datetime.utcnow()

        positions = exchange.get_positions()
        pos_FAR = positions.get("ASML_202606_F", 0)
        pos_NEAR = positions.get("ASML_202603_F", 0)

        elapsed = (now - start_time).total_seconds() / 60.0  # in minutes
        pnl = exchange.get_pnl() or 0
        pnl_history.append(pnl)

        # Compute mean and standard deviation of PnL
        mean_pnl = statistics.mean(pnl_history)
        std_pnl = statistics.stdev(pnl_history) if len(pnl_history) > 1 else 0
        pnl_adj = exchange.get_pnl() or 0

        print(f"")
        print(f"-----------------------------------------------------------------")
        print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
        print(f"-----------------------------------------------------------------")
        positions = exchange.get_positions()
        print_positions_and_pnl()
        print(f"")
        near_order_book = exchange.get_last_price_book(NEAR_ID)
        far_order_book = exchange.get_last_price_book(FAR_ID)
    
        if(near_order_book.bids and far_order_book.asks):
            near_bid_price = near_order_book.bids[0].price
            near_bid_vol = near_order_book.bids[0].volume
            far_ask_price = far_order_book.asks[0].price
            far_ask_vol = far_order_book.asks[0].volume
       
            market_ratio_low = far_ask_price / near_bid_price
            fair_ratio = math.exp((r) * (0.25))
            if market_ratio_low < fair_ratio:
                if not trade_would_breach_position_limit(FAR_ID, min(near_bid_vol, far_ask_vol, 100 - positions[FAR_ID]), "bid") and not trade_would_breach_position_limit(NEAR_ID, min(near_bid_vol, far_ask_vol, 100 +    positions[NEAR_ID]), "ask"):
                    print("trade done")
                    exchange.insert_order(FAR_ID, price= far_ask_price, volume= min(near_bid_vol, far_ask_vol, 100 - positions[FAR_ID]), side="bid", order_type="ioc")
                    exchange.insert_order(NEAR_ID, price= near_bid_price, volume= min(near_bid_vol, far_ask_vol, 100 + positions[NEAR_ID]), side="ask", order_type="ioc")

        if(far_order_book.bids and near_order_book.asks):
            far_bid_price = far_order_book.bids[0].price
            far_bid_vol = far_order_book.bids[0].volume
            near_ask_price = near_order_book.asks[0].price
            near_ask_vol = near_order_book.asks[0].volume

            market_ratio_high = far_bid_price / near_ask_price

            fair_ratio = math.exp((r) * (0.25))
            if market_ratio_high > fair_ratio:
                if not trade_would_breach_position_limit(FAR_ID, min(far_bid_vol, near_ask_vol, 100 + positions[FAR_ID]), "ask") and not trade_would_breach_position_limit(NEAR_ID, min(far_bid_vol, near_ask_vol, 100 - positions[NEAR_ID]), "bid"):
                    print("trade done")
                    exchange.insert_order(FAR_ID, price= far_bid_price, volume= min(far_bid_vol, near_ask_vol, 100 + positions[FAR_ID]), side="ask", order_type="ioc")
                    exchange.insert_order(NEAR_ID, price= near_ask_price, volume= min(far_bid_vol, near_ask_vol, 100 - positions[NEAR_ID]), side="bid", order_type="ioc")

        
        positions = exchange.get_positions()
        stock_pos = positions.get("ASML_202606_F", 0)
        future_pos = positions.get("ASML_202603_F", 0)

        # Compute net exposure: long stock + long futures = net exposure
        hedge_diff = stock_pos + future_pos  # note the plus here!

        if hedge_diff == 0:
            print("Stock and futures positions are correctly hedged.")
              # already hedged

        elif hedge_diff > 0:
            # Too much net long exposure → sell stocks
            sell_volume = hedge_diff
            if sell_volume == 0:
                print("Cannot sell more stock due to position limits.")
                if not trade_would_breach_position_limit("ASML_202606_F", sell_volume, "ask"):    
                    exchange.insert_order("ASML_202606_F", price=far_order_book.bids[0].price, volume=sell_volume, side="ask", order_type="ioc")
                    print(f"Hedge adjustment: SOLD {sell_volume} stock to reduce net exposure")
        elif hedge_diff < 0:
            
            buy_volume = -hedge_diff
            if buy_volume == 0:
                print("Cannot buy more stock due to position limits.")
                if not trade_would_breach_position_limit("ASML_202606_F", buy_volume, "bid"):    
                    exchange.insert_order("ASML_202606_F", price=far_order_book.asks[0].price, volume=buy_volume, side="bid", order_type="ioc")
                    print(f"Hedge adjustment: BOUGHT {buy_volume} stock to reduce net exposure")


        # ---- Log each loop ----
        writer.writerow([
            run,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed,
            pnl_adj,
            mean_pnl,
            std_pnl,
            "ASML_202606_F",
            far_bid_price,
            far_ask_price,
            pos_FAR,
            "ASML_202603_F",
            near_bid_price,
            near_ask_price,
            pos_NEAR,
        ])
        f.flush()  # ensure data is written to disk
        time.sleep(.1)


# %%

exchange.insert_order("ASML_202606_F", price= exchange.get_last_price_book("ASML_202606_F").bids[0].price, volume= 43, side="ask", order_type="ioc")

# %%

#%%
print_positions_and_pnl()
# %%
