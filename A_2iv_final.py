#%%
import datetime as dt
import time
import logging
import numpy as np

from optibook.synchronous_client import Exchange
from optibook.exporter import Exporter

# ===================== CONFIG =====================
POSITION_LIMIT   = 100
VOLUME_PER_TRADE = 1
SLEEP_SECS       = 0.1
RUN_MINUTES      = 60     # per run
N_RUNS           = 3      # number of runs
QUESTION_LABEL   = "a_2iv_kes"
TINY_TICK        = 0.05   # aggressive price step

# ===================== FILE NAME =====================
start_time = dt.datetime.now()
filename = f"{start_time.strftime('%Y-%m-%d-%H%M%S')}-{QUESTION_LABEL}-latest.csv"
print(f"📂 Exporting all runs to: {filename}")

# ===================== PAIR SELECTION =====================
pairs = [
    #("SAP", "SAP_DUAL"),
    ("ASML", "ASML_DUAL")
]

# ===================== SETUP =====================
logging.getLogger("client").setLevel("ERROR")
exporter = Exporter()

# ===================== OUTER LOOP =====================
for run in range(N_RUNS):
    print(f"\n================ RUN {run+1}/{N_RUNS} =================")
    exchange = Exchange()
    exchange.connect()

    pnl0 = exchange.get_pnl()
    loop_start = time.time()
    pnl_history = []
    successful_trades = 0
    failed_trades = 0

    while True:
        now = time.time()
        elapsed = now - loop_start
        if elapsed > RUN_MINUTES * 60:
            print("⏹️ Time limit reached, stopping run.")
            break

        pnl = exchange.get_pnl()
        pnl_adj = pnl - pnl0 if pnl is not None else 0.0
        pnl_history.append(pnl_adj)
        mean_pnl = np.mean(pnl_history)
        std_pnl  = np.std(pnl_history) if len(pnl_history) > 1 else 0

        positions = exchange.get_positions()

        for MAIN_ID, DUAL_ID in pairs:
            book_main = exchange.get_last_price_book(MAIN_ID)
            book_dual = exchange.get_last_price_book(DUAL_ID)

            if not (book_main and book_dual and
                    book_main.bids and book_main.asks and
                    book_dual.bids and book_dual.asks):
                continue

            best_bid_main, best_ask_main = book_main.bids[0].price, book_main.asks[0].price
            best_bid_dual, best_ask_dual = book_dual.bids[0].price, book_dual.asks[0].price

            pos_main = positions[MAIN_ID]
            pos_dual = positions[DUAL_ID]

            # ---- Arbitrage: stock bid > dual ask ----
            if best_bid_main > best_ask_dual:
                if pos_dual + VOLUME_PER_TRADE <= POSITION_LIMIT and pos_main - VOLUME_PER_TRADE >= -POSITION_LIMIT:
                    resp1 = exchange.insert_order(DUAL_ID, price=best_bid_main - TINY_TICK,
                                                  volume=VOLUME_PER_TRADE, side="bid", order_type="ioc")
                    resp2 = exchange.insert_order(MAIN_ID, price=best_bid_main,
                                                  volume=VOLUME_PER_TRADE, side="ask", order_type="ioc")
                    success = resp1.success and resp2.success
                    successful_trades += success
                    failed_trades     += not success

            # ---- Arbitrage: dual bid > stock ask ----
            elif best_bid_dual > best_ask_main:
                if pos_dual - VOLUME_PER_TRADE >= -POSITION_LIMIT and pos_main + VOLUME_PER_TRADE <= POSITION_LIMIT:
                    resp1 = exchange.insert_order(DUAL_ID, price=best_ask_main + TINY_TICK,
                                                  volume=VOLUME_PER_TRADE, side="ask", order_type="ioc")
                    resp2 = exchange.insert_order(MAIN_ID, price=best_ask_main,
                                                  volume=VOLUME_PER_TRADE, side="bid", order_type="ioc")
                    success = resp1.success and resp2.success
                    successful_trades += success
                    failed_trades     += not success

            # ---- Log each loop ----
            logrequest = {
                filename: [[
                    run+1,          # run id
                    elapsed/60,     # elapsed time (minutes)
                    pnl_adj,
   
   
                    mean_pnl,
                    std_pnl,
                    MAIN_ID,
                    best_bid_main,
                    best_ask_main,
                    pos_main,
                    DUAL_ID,
                    best_bid_dual,
                    best_ask_dual,
                    pos_dual,
                    successful_trades,
                    failed_trades
                ]]
            }
            exporter.export(logrequest)

        time.sleep(SLEEP_SECS)

    # ----------- NO UNWIND HERE (positions remain open) -----------

    exchange.disconnect()
    time.sleep(0.1)

print(f"\n✅ All {N_RUNS} runs finished. Data saved to exports/{filename}")
