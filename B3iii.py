#%% B3iii – Fast & Simple stock–two-futures arb (priority to highest edge + executable-edge gate)

import datetime as dt
import time
import math
import logging
import csv
import statistics
import os

from optibook.synchronous_client import Exchange

# -------------------- Config (kept close to your B2iii) --------------------
RISK_FREE_RATE = 0.03
THRESHOLD      = 0.20               # base mispricing gate (keep your value)
POSITION_LIMIT = 100

STOCK_ID = "ASML"
FUT1_ID  = "ASML_202603_F"          # near
FUT2_ID  = "ASML_202606_F"          # far

# Adjust maturities if your sim uses different exact dates
FUT1_MATURITY = dt.datetime(2026, 3, 31)
FUT2_MATURITY = dt.datetime(2026, 6, 30)

CSV_NAME = "B3iii_fast_priority.csv"

# --- speed & safety knobs (simple) ---
MAX_TRADE_PER_LOOP      = 12      # cap how much we try each loop (keeps control)
MIN_NET_FOR_BOTH        = 6       # if edges oppose, only trade both if net futures >= this (so stock hedge is non-trivial)
NEAR                    = 90      # treat |pos|>=NEAR as "near limit": only unwind there
BURST_EDGE              = 0.60    # if adjusted edge >= this, allow a larger one-shot in this loop
BURST_MULTIPLIER        = 2       # simple burst bump (still capped by limits)
EXEC_EDGE_EPS           = 0.00    # minimal executable-edge to accept; set small positive (e.g. 0.01) if you want extra cushion

# -------------------- Connect --------------------
exchange = Exchange()
exchange.connect()
logging.getLogger("client").setLevel("ERROR")

# -------------------- Helpers (same style as B2iii) --------------------
def print_positions_and_pnl(always_display=None):
    positions = exchange.get_positions()
    print("Positions:")
    for instrument_id, pos in positions.items():
        if (not always_display) or instrument_id in always_display or pos != 0:
            print(f"  {instrument_id:20s}: {pos:4.0f}")
    pnl = exchange.get_pnl()
    if pnl is not None:
        print(f"\nPnL: {pnl:.2f}")

def get_mid_price(book):
    if book and book.bids and book.asks:
        return (book.bids[0].price + book.asks[0].price) / 2.0
    return None

def top_of_book(book):
    bid = book.bids[0].price if (book and book.bids) else 0
    ask = book.asks[0].price if (book and book.asks) else 0
    return bid, ask

def half_spread(book):
    if book and book.bids and book.asks:
        return max(book.asks[0].price - book.bids[0].price, 0) / 2.0
    return 0.0

def max_lots_allowed(instrument_id, side, position_limit=POSITION_LIMIT):
    positions = exchange.get_positions()
    cur = positions.get(instrument_id, 0)
    if side == "bid":   # buying increases pos
        return max(position_limit - cur, 0)
    else:               # selling decreases pos
        return max(position_limit + cur, 0)

def years_to_maturity(maturity_dt):
    now = dt.datetime.utcnow()
    return max((maturity_dt - now).days, 0) / 365.0

def can_open_in_direction(pos, mis):
    """mis>0 → SELL future; mis<0 → BUY future. Near limit: only unwind toward 0."""
    if abs(pos) < NEAR:
        return True
    if mis > 0 and pos > 0:   # SELL reduces +pos
        return True
    if mis < 0 and pos < 0:   # BUY reduces -pos
        return True
    return False

def executable_edge(stock_book, fut_book, mis, theo_mult):
    """
    Edge using TOUCH prices (what you'll actually lock in).
    - mis>0 → SELL future @ bid, BUY stock @ ask: ex_edge = f_bid - s_ask * theo_mult
    - mis<0 → BUY future @ ask, SELL stock @ bid: ex_edge = s_bid * theo_mult - f_ask
    """
    s_bid, s_ask = top_of_book(stock_book)
    f_bid, f_ask = top_of_book(fut_book)
    if s_bid == 0 or s_ask == 0 or (f_bid == 0 and f_ask == 0):
        return -1e9  # block trade if books empty
    if mis > 0:
        return f_bid - s_ask * theo_mult
    else:
        return s_bid * theo_mult - f_ask

# -------------------- CSV setup --------------------
pnl_history = []
csv_path = os.path.join(os.path.dirname(__file__), CSV_NAME)
write_header = not os.path.exists(csv_path)
print(f"[CSV] Writing to: {csv_path}")
csvf = open(csv_path, "a", newline="")
writer = csv.writer(csvf)
if write_header:
    writer.writerow([
        "run","timestamp","elapsed_m","pnl","mean_pnl","std_pnl",
        "pos_stock","pos_F1","pos_F2",
        "spot_mid","f1_mid","f2_mid",
        "mis1","mis2","edge1","edge2",
        "ex_edge1","ex_edge2",
        "traded_F1","traded_F2","hedge_diff_after"
    ])

# -------------------- Trading primitive: ONE future vs stock (IOC) --------------------
def trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint=None):
    """
    Send IOC orders at touch for (future, stock) in the direction of 'mis'.
    If lots_hint is provided, use it (still capped by availability and MAX_TRADE_PER_LOOP).
    Returns lots actually attempted (int).
    """
    if abs(mis) <= THRESHOLD:
        return 0

    s_bid, s_ask = top_of_book(stock_book)
    if s_bid == 0 or s_ask == 0 or not (fut_book and (fut_book.bids or fut_book.asks)):
        return 0

    # decide direction + size
    desired = max(1, int(abs(mis) * 50))
    if lots_hint is not None:
        desired = min(desired, int(lots_hint))

    # cap per loop (fast but controlled)
    desired = min(desired, MAX_TRADE_PER_LOOP)

    if mis > 0:
        # Future overpriced → SELL future, BUY stock
        lots_stock_buy   = min(desired, max_lots_allowed(STOCK_ID, "bid"))
        lots_future_sell = min(desired, max_lots_allowed(fut_id, "ask"))
        lots = min(lots_stock_buy, lots_future_sell)
        if lots > 0:
            exchange.insert_order(STOCK_ID, price=s_ask, volume=lots, side="bid", order_type="ioc")
            exchange.insert_order(fut_id,   price=fut_book.bids[0].price, volume=lots, side="ask", order_type="ioc")
            print(f"[{fut_id}] BUY stock / SELL future x{lots}")
            return lots
    else:
        # Future underpriced → BUY future, SELL stock
        lots_future_buy = min(desired, max_lots_allowed(fut_id, "bid"))
        lots_stock_sell = min(desired, max_lots_allowed(STOCK_ID, "ask"))
        lots = min(lots_future_buy, lots_stock_sell)
        if lots > 0:
            exchange.insert_order(fut_id,   price=fut_book.asks[0].price, volume=lots, side="bid", order_type="ioc")
            exchange.insert_order(STOCK_ID, price=s_bid,                  volume=lots, side="ask", order_type="ioc")
            print(f"[{fut_id}] SELL stock / BUY future x{lots}")
            return lots

    return 0

# -------------------- Main loop --------------------
run = 0
start_time = dt.datetime.utcnow()
last_print = start_time

try:
    while True:
        run += 1
        now = dt.datetime.utcnow()
        elapsed_m = (now - start_time).total_seconds() / 60.0

        # Books
        stock_book = exchange.get_last_price_book(STOCK_ID)
        f1_book    = exchange.get_last_price_book(FUT1_ID)
        f2_book    = exchange.get_last_price_book(FUT2_ID)

        spot_mid = get_mid_price(stock_book)
        f1_mid   = get_mid_price(f1_book)
        f2_mid   = get_mid_price(f2_book)
        if not (spot_mid and f1_mid and f2_mid):
            time.sleep(0.05)
            continue

        # Theo + mis
        T1 = years_to_maturity(FUT1_MATURITY)
        T2 = years_to_maturity(FUT2_MATURITY)
        Theo1 = math.exp(RISK_FREE_RATE * T1)
        Theo2 = math.exp(RISK_FREE_RATE * T2)
        F1_theo = spot_mid * Theo1
        F2_theo = spot_mid * Theo2
        mis1 = f1_mid - F1_theo
        mis2 = f2_mid - F2_theo

        # Adjusted edges (mid-based) → quick ranking only
        cost1 = half_spread(stock_book) + half_spread(f1_book)
        cost2 = half_spread(stock_book) + half_spread(f2_book)
        edge1 = abs(mis1) - max(THRESHOLD, cost1)
        edge2 = abs(mis2) - max(THRESHOLD, cost2)

        # Executable edges (touch-based) → hard gate
        ex_edge1 = executable_edge(stock_book, f1_book, mis1, Theo1)
        ex_edge2 = executable_edge(stock_book, f2_book, mis2, Theo2)

        # Select what to trade this loop (fast & deterministic)
        cands = []
        if edge1 > 0 and ex_edge1 > EXEC_EDGE_EPS:
            cands.append(("F1", FUT1_ID, f1_book, mis1, edge1, ex_edge1, Theo1))
        if edge2 > 0 and ex_edge2 > EXEC_EDGE_EPS:
            cands.append(("F2", FUT2_ID, f2_book, mis2, edge2, ex_edge2, Theo2))

        traded_F1 = 0
        traded_F2 = 0

        if cands:
            if len(cands) == 1:
                tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult = cands[0]
                # Burst if mid-adjusted edge is very large (prioritize getting fills)
                lots_hint = None
                if e_mid >= BURST_EDGE:
                    lots_hint = MAX_TRADE_PER_LOOP * BURST_MULTIPLIER
                # Final executable-edge check right before send (books can move)
                ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                if ex_now > EXEC_EDGE_EPS:
                    lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint)
                    if fut_id == FUT1_ID: traded_F1 = lots
                    else: traded_F2 = lots
                else:
                    print(f"[Gate] {fut_id} exec-edge turned {ex_now:.4f} → skip")

            else:
                # both actionable
                # sort by executable edge first (what we can lock in), tie-breaker by mid-edge
                cands.sort(key=lambda x: (x[5], x[4]), reverse=True)

                # sign: mis>0 means SELL fut (negative), mis<0 means BUY fut (positive)
                sgn_map = {FUT1_ID: (1 if mis1 < 0 else -1), FUT2_ID: (1 if mis2 < 0 else -1)}

                # quick preview lots for net check
                def preview_lots(e_mid, mis):
                    base = min(MAX_TRADE_PER_LOOP, max(1, int(abs(mis)*50)))
                    if e_mid >= BURST_EDGE:
                        base = min(base * BURST_MULTIPLIER, POSITION_LIMIT)
                    return base

                # pull in order matching after sort
                # cands entries: (tag, fut_id, fut_book, mis, edge_mid, edge_exe, theo_mult)
                mis_map  = {FUT1_ID: mis1, FUT2_ID: mis2}
                e_mid_map= {FUT1_ID: edge1, FUT2_ID: edge2}

                lots1_hint = preview_lots(e_mid_map[FUT1_ID], mis_map[FUT1_ID])
                lots2_hint = preview_lots(e_mid_map[FUT2_ID], mis_map[FUT2_ID])

                sgn1 = sgn_map[FUT1_ID]
                sgn2 = sgn_map[FUT2_ID]
                same_sign = (mis1 * mis2 > 0)

                if same_sign:
                    # trade both vs stock; higher executable edge first
                    for tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult in cands:
                        ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                        if ex_now <= EXEC_EDGE_EPS:
                            print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} → skip")
                            continue
                        lots_hint = preview_lots(e_mid, mis)
                        lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint)
                        if fut_id == FUT1_ID: traded_F1 = lots
                        else: traded_F2 = lots
                else:
                    # opposite signs: only trade both if net futures is meaningful
                    net_if_both = sgn1*lots1_hint + sgn2*lots2_hint
                    if abs(net_if_both) >= MIN_NET_FOR_BOTH:
                        for tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult in cands:
                            ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                            if ex_now <= EXEC_EDGE_EPS:
                                print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} → skip")
                                continue
                            lots_hint = preview_lots(e_mid, mis)
                            lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint)
                            if fut_id == FUT1_ID: traded_F1 = lots
                            else: traded_F2 = lots
                    else:
                        # pick the bigger executable edge (then mid-edge) to avoid accidental calendar
                        tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult = cands[0]
                        ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                        if ex_now > EXEC_EDGE_EPS:
                            lots_hint = preview_lots(e_mid, mis)
                            lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint)
                            if fut_id == FUT1_ID: traded_F1 = lots
                            else: traded_F2 = lots
                        else:
                            print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} → skip")

        # -------------- Hedge once vs TOTAL futures (B3iii) ----------------
        if traded_F1 or traded_F2:
            # small iteration cap avoids "max iterations" errors if books vanish
            for _ in range(6):
                positions = exchange.get_positions()
                stock_pos = positions.get(STOCK_ID, 0)
                f1_pos    = positions.get(FUT1_ID, 0)
                f2_pos    = positions.get(FUT2_ID, 0)
                hedge_diff = stock_pos + f1_pos + f2_pos
                if hedge_diff == 0:
                    print("Hedge OK: stock + F1 + F2 = 0")
                    break
                s_bid, s_ask = top_of_book(stock_book)
                if s_bid == 0 or s_ask == 0:
                    break
                if hedge_diff > 0:
                    vol = min(hedge_diff, max_lots_allowed(STOCK_ID, "ask"))
                    if vol == 0: break
                    exchange.insert_order(STOCK_ID, price=s_bid, volume=vol, side="ask", order_type="ioc")
                    print(f"Hedge: SOLD {vol} stock")
                else:
                    vol = min(-hedge_diff, max_lots_allowed(STOCK_ID, "bid"))
                    if vol == 0: break
                    exchange.insert_order(STOCK_ID, price=s_ask, volume=vol, side="bid", order_type="ioc")
                    print(f"Hedge: BOUGHT {vol} stock")
                time.sleep(0.03)

        # -------------- Light logging / CSV (like your B2iii) --------------
        pnl = exchange.get_pnl() or 0.0
        pnl_history.append(pnl)
        mean_pnl = statistics.mean(pnl_history)
        std_pnl = statistics.stdev(pnl_history) if len(pnl_history) > 1 else 0.0

        # periodic stdout
        if (now - last_print).total_seconds() >= 1.0:
            print("\n-----------------------------------------------------------------")
            print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
            print("-----------------------------------------------------------------")
            print_positions_and_pnl(always_display=[STOCK_ID, FUT1_ID, FUT2_ID])
            print(f"Spot {spot_mid:.2f} | F1 {f1_mid:.2f} (theo {F1_theo:.2f}) | F2 {f2_mid:.2f} (theo {F2_theo:.2f})")
            print(f"mis1={mis1:.4f} edge1={edge1:.4f} ex1={ex_edge1:.4f} | mis2={mis2:.4f} edge2={edge2:.4f} ex2={ex_edge2:.4f}")
            last_print = now

        positions = exchange.get_positions()
        hedge_after = positions.get(STOCK_ID, 0) + positions.get(FUT1_ID, 0) + positions.get(FUT2_ID, 0)
        writer.writerow([
            run, now.strftime("%Y-%m-%d %H:%M:%S"), elapsed_m, pnl, mean_pnl, std_pnl,
            positions.get(STOCK_ID, 0), positions.get(FUT1_ID, 0), positions.get(FUT2_ID, 0),
            spot_mid, f1_mid, f2_mid, mis1, mis2, edge1, edge2,
            ex_edge1, ex_edge2,
            traded_F1, traded_F2, hedge_after
        ])
        csvf.flush()

        time.sleep(0.05)

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    csvf.close()
