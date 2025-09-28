import datetime as dt
import time
import math
import logging
import csv
import statistics
import os

from optibook.synchronous_client import Exchange

# -------------------- Parameters --------------------
RISK_FREE_RATE = 0.03
THRESHOLD      = 0.20
POSITION_LIMIT = 100

STOCK_ID = "ASML"
FUT1_ID  = "ASML_202603_F"   
FUT2_ID  = "ASML_202606_F"   

FUT1_MATURITY = dt.datetime(2026, 3, 31)
FUT2_MATURITY = dt.datetime(2026, 6, 30)

CSV_NAME = "B3iii.csv"

# --- speed & safety ---
MAX_TRADE_PER_LOOP      = 12
MIN_NET_FOR_BOTH        = 6
NEAR                    = 90
BURST_EDGE              = 0.60
BURST_MULTIPLIER        = 2
EXEC_EDGE_EPS           = 0.01

# --- hedge / rate knobs ---
HEDGE_SECOND_PASS_DELAY = 0.02
HEDGE_SECOND_PASS_CAP   = 3
MIN_ORDER_GAP_SEC       = 0.012

# -------------------- Connect & logging --------------------
exchange = Exchange()
exchange.connect()
logging.getLogger("client").setLevel("ERROR")

# -------------------- B2iii-style helpers --------------------
def trade_would_breach_position_limit(instrument_id, volume, side, position_limit=POSITION_LIMIT):
    """Convenience check (not used to gate flow here, kept for parity)."""
    positions = exchange.get_positions()
    cur = positions.get(instrument_id, 0)
    if volume == 0:
        return True
    if side == "bid":
        return cur + volume > position_limit
    elif side == "ask":
        return cur - volume < -position_limit
    else:
        raise ValueError(f"Invalid side: {side}. Use 'bid' or 'ask'.")

def print_positions_and_pnl(always_display=None):
    positions = exchange.get_positions()
    print("Positions:")
    for instrument_id in positions:
        if (not always_display) or instrument_id in always_display or positions[instrument_id] != 0:
            print(f"  {instrument_id:20s}: {positions[instrument_id]:4.0f}")
    pnl = exchange.get_pnl()
    if pnl is not None:
        print(f"\nPnL: {pnl:.2f}")

def get_mid_price(order_book):
    if order_book and order_book.bids and order_book.asks:
        best_bid = order_book.bids[0].price
        best_ask = order_book.asks[0].price
        return (best_bid + best_ask) / 2.0
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
    if side == "bid":   
        return max(position_limit - cur, 0)
    else:              
        return max(position_limit + cur, 0)

def years_to_maturity(maturity_dt):
    now = dt.datetime.utcnow()
    return max((maturity_dt - now).days, 0) / 365.0

def can_open_in_direction(pos, mis):
    """mis>0 → SELL future; mis<0 → BUY future. Near limit: only unwind toward 0."""
    if abs(pos) < NEAR:
        return True
    if mis > 0 and pos > 0:   
        return True
    if mis < 0 and pos < 0:   
        return True
    return False

def executable_edge(stock_book, fut_book, mis, theo_mult):
    """
    Touch-based executable edge:
      mis>0 → SELL fut @ bid & BUY stk @ ask: edge = f_bid - s_ask*theo
      mis<0 → BUY fut @ ask & SELL stk @ bid: edge = s_bid*theo - f_ask
    """
    s_bid, s_ask = top_of_book(stock_book)
    f_bid, f_ask = top_of_book(fut_book)
    if s_bid == 0 or s_ask == 0 or (f_bid == 0 and f_ask == 0):
        return -1e9
    return (f_bid - s_ask * theo_mult) if (mis > 0) else (s_bid * theo_mult - f_ask)

# --- very light rate limiting for insert_order ---
_last_order_ts = 0.0
def send_ioc(instrument_id, price, volume, side):
    global _last_order_ts
    now = time.time()
    gap = now - _last_order_ts
    if gap < MIN_ORDER_GAP_SEC:
        time.sleep(MIN_ORDER_GAP_SEC - gap)
    exchange.insert_order(instrument_id, price=price, volume=volume, side=side, order_type="ioc")
    _last_order_ts = time.time()

# -------------------- One-leg primitive (future vs stock, IOC) --------------------
def trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint=None):
    if abs(mis) <= THRESHOLD:
        return 0

    s_bid, s_ask = top_of_book(stock_book)
    if s_bid == 0 or s_ask == 0 or not (fut_book and (fut_book.bids or fut_book.asks)):
        return 0

    desired = max(1, int(abs(mis) * 50))
    if lots_hint is not None:
        desired = min(desired, int(lots_hint))
    desired = min(desired, MAX_TRADE_PER_LOOP)

    if mis > 0:
        # Overpriced future → SELL fut, BUY stock
        lots_stock_buy   = min(desired, max_lots_allowed(STOCK_ID, "bid"))
        lots_future_sell = min(desired, max_lots_allowed(fut_id,   "ask"))
        lots = min(lots_stock_buy, lots_future_sell)
        if lots > 0:
            send_ioc(STOCK_ID, s_ask, lots, "bid")
            send_ioc(fut_id,   fut_book.bids[0].price, lots, "ask")
            print(f"[{fut_id}] BUY stock / SELL future x{lots}")
            return lots
    else:
        # Underpriced future → BUY fut, SELL stock
        lots_future_buy = min(desired, max_lots_allowed(fut_id,   "bid"))
        lots_stock_sell = min(desired, max_lots_allowed(STOCK_ID, "ask"))
        lots = min(lots_future_buy, lots_stock_sell)
        if lots > 0:
            send_ioc(fut_id,   fut_book.asks[0].price, lots, "bid")
            send_ioc(STOCK_ID, s_bid,                  lots, "ask")
            print(f"[{fut_id}] SELL stock / BUY future x{lots}")
            return lots

    return 0

# -------------------- CSV  --------------------
pnl_history = []
csv_path = os.path.join(os.path.dirname(__file__), CSV_NAME)
write_header = not os.path.exists(csv_path)
print(f"[CSV] Writing to: {csv_path}")

with open(csv_path, "a", newline="") as f:
    writer = csv.writer(f)
    if write_header:
        writer.writerow([
            "run","timestamp","elapsed","pnl_adj","mean_pnl","std_pnl",
            "MAIN_ID","best_bid_main","best_ask_main","pos_main",
            "FUTURE_ID","best_bid_future","best_ask_future","pos_dual",
            "successful_trades","failed_trades"
        ])

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

            # Mids
            spot_mid = get_mid_price(stock_book)
            f1_mid   = get_mid_price(f1_book)
            f2_mid   = get_mid_price(f2_book)
            if not (spot_mid and f1_mid and f2_mid):
                time.sleep(0.05)
                continue

            # Theos & mispricings
            T1 = years_to_maturity(FUT1_MATURITY)
            T2 = years_to_maturity(FUT2_MATURITY)
            Theo1 = math.exp(RISK_FREE_RATE * T1)
            Theo2 = math.exp(RISK_FREE_RATE * T2)
            F1_theo = spot_mid * Theo1
            F2_theo = spot_mid * Theo2
            mis1 = f1_mid - F1_theo
            mis2 = f2_mid - F2_theo

            # Mid-based “edge” for quick ranking
            cost1 = half_spread(stock_book) + half_spread(f1_book)
            cost2 = half_spread(stock_book) + half_spread(f2_book)
            edge1 = abs(mis1) - max(THRESHOLD, cost1)
            edge2 = abs(mis2) - max(THRESHOLD, cost2)

            # Touch-based executable edges (hard gate)
            ex_edge1 = executable_edge(stock_book, f1_book, mis1, Theo1)
            ex_edge2 = executable_edge(stock_book, f2_book, mis2, Theo2)

            # Decide what to trade
            cands = []
            if edge1 > 0 and ex_edge1 > EXEC_EDGE_EPS:
                cands.append(("F1", FUT1_ID, f1_book, mis1, edge1, ex_edge1, Theo1))
            if edge2 > 0 and ex_edge2 > EXEC_EDGE_EPS:
                cands.append(("F2", FUT2_ID, f2_book, mis2, edge2, ex_edge2, Theo2))

            traded_F1 = 0
            traded_F2 = 0
            failed_trades = 0

            if cands:
                if len(cands) == 1:
                    tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult = cands[0]
                    lots_hint = None
                    if e_mid >= BURST_EDGE:
                        lots_hint = MAX_TRADE_PER_LOOP * BURST_MULTIPLIER
                    ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                    if ex_now > EXEC_EDGE_EPS and can_open_in_direction(exchange.get_positions().get(fut_id, 0), mis):
                        lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint)
                        if fut_id == FUT1_ID: traded_F1 = lots
                        else: traded_F2 = lots
                    else:
                        print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} or dir gate → skip")
                        failed_trades += 1
                else:
                    # Both actionable → sort by executable edge (then mid-edge)
                    cands.sort(key=lambda x: (x[5], x[4]), reverse=True)

                    # Sign: mis>0 SELL fut (−), mis<0 BUY fut (+)
                    sgn1 = 1 if mis1 < 0 else -1
                    sgn2 = 1 if mis2 < 0 else -1
                    same_sign = (mis1 * mis2 > 0)

                    def preview_lots(e_mid, mis):
                        base = min(MAX_TRADE_PER_LOOP, max(1, int(abs(mis) * 50)))
                        if e_mid >= BURST_EDGE:
                            base = min(base * BURST_MULTIPLIER, POSITION_LIMIT)
                        return base

                    lots1_hint = preview_lots(edge1, mis1)
                    lots2_hint = preview_lots(edge2, mis2)

                    if same_sign:
                        for tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult in cands:
                            ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                            if ex_now <= EXEC_EDGE_EPS or not can_open_in_direction(exchange.get_positions().get(fut_id, 0), mis):
                                print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} or dir gate → skip")
                                failed_trades += 1
                                continue
                            lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, preview_lots(e_mid, mis))
                            if fut_id == FUT1_ID: traded_F1 = lots
                            else: traded_F2 = lots
                    else:
                        net_if_both = sgn1 * lots1_hint + sgn2 * lots2_hint
                        if abs(net_if_both) >= MIN_NET_FOR_BOTH:
                            for tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult in cands:
                                ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                                if ex_now <= EXEC_EDGE_EPS or not can_open_in_direction(exchange.get_positions().get(fut_id, 0), mis):
                                    print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} or dir gate → skip")
                                    failed_trades += 1
                                    continue
                                lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, preview_lots(e_mid, mis))
                                if fut_id == FUT1_ID: traded_F1 = lots
                                else: traded_F2 = lots
                        else:
                            tag, fut_id, fut_book, mis, e_mid, e_exe, theo_mult = cands[0]
                            ex_now = executable_edge(stock_book, fut_book, mis, theo_mult)
                            if ex_now > EXEC_EDGE_EPS and can_open_in_direction(exchange.get_positions().get(fut_id, 0), mis):
                                lots = trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, preview_lots(e_mid, mis))
                                if fut_id == FUT1_ID: traded_F1 = lots
                                else: traded_F2 = lots
                            else:
                                print(f"[Gate] {fut_id} exec-edge {ex_now:.4f} or dir gate → skip")
                                failed_trades += 1

            # -------------------- Hedge once + micro-fix --------------------
            if traded_F1 or traded_F2:
                positions = exchange.get_positions()
                stock_pos = positions.get(STOCK_ID, 0)
                f1_pos    = positions.get(FUT1_ID, 0)
                f2_pos    = positions.get(FUT2_ID, 0)
                hedge_diff = stock_pos + f1_pos + f2_pos

                s_bid, s_ask = top_of_book(stock_book)

                if hedge_diff > 0 and s_bid > 0:
                    vol_req = hedge_diff
                    vol = min(vol_req, max_lots_allowed(STOCK_ID, "ask"))
                    if vol > 0:
                        send_ioc(STOCK_ID, s_bid, vol, "ask")
                        print(f"Hedge: SOLD {vol} stock (req {vol_req})")
                elif hedge_diff < 0 and s_ask > 0:
                    vol_req = -hedge_diff
                    vol = min(vol_req, max_lots_allowed(STOCK_ID, "bid"))
                    if vol > 0:
                        send_ioc(STOCK_ID, s_ask, vol, "bid")
                        print(f"Hedge: BOUGHT {vol} stock (req {vol_req})")

                time.sleep(HEDGE_SECOND_PASS_DELAY)
                positions = exchange.get_positions()
                residual = positions.get(STOCK_ID,0) + positions.get(FUT1_ID,0) + positions.get(FUT2_ID,0)

                if residual != 0:
                    fix = min(abs(residual), HEDGE_SECOND_PASS_CAP)
                    s_bid, s_ask = top_of_book(stock_book)
                    if residual > 0 and s_bid > 0:
                        vol = min(fix, max_lots_allowed(STOCK_ID, "ask"))
                        if vol > 0:
                            send_ioc(STOCK_ID, s_bid, vol, "ask")
                            print(f"Hedge micro-fix: SOLD {vol} stock (residual {residual})")
                    elif residual < 0 and s_ask > 0:
                        vol = min(fix, max_lots_allowed(STOCK_ID, "bid"))
                        if vol > 0:
                            send_ioc(STOCK_ID, s_ask, vol, "bid")
                            print(f"Hedge micro-fix: BOUGHT {vol} stock (residual {residual})")

            # -------------------- CSV row --------------------
            pnl = exchange.get_pnl() or 0.0
            pnl_history.append(pnl)
            mean_pnl = statistics.mean(pnl_history)
            std_pnl  = statistics.stdev(pnl_history) if len(pnl_history) > 1 else 0.0

            # Choose active future to log (single)
            active_fut_id = FUT1_ID
            if (traded_F1 > 0) ^ (traded_F2 > 0):
                active_fut_id = FUT1_ID if traded_F1 > 0 else FUT2_ID
            elif (traded_F1 > 0) and (traded_F2 > 0):
                active_fut_id = FUT1_ID if abs(ex_edge1) >= abs(ex_edge2) else FUT2_ID
            elif cands:
                best = sorted(cands, key=lambda x: (x[5], x[4]), reverse=True)[0]
                active_fut_id = best[1]

            main_bid, main_ask = top_of_book(stock_book)
            pos = exchange.get_positions()
            pos_main = pos.get(STOCK_ID, 0)

            active_book = f1_book if active_fut_id == FUT1_ID else f2_book
            fut_bid, fut_ask = top_of_book(active_book)
            pos_dual = pos.get(active_fut_id, 0)

            successful_trades = int(traded_F1 + traded_F2)

            writer.writerow([
                run,
                now.strftime("%Y-%m-%d %H:%M:%S"),
                elapsed_m,
                pnl,                
                mean_pnl,
                std_pnl,
                STOCK_ID,
                main_bid,
                main_ask,
                pos_main,
                active_fut_id,
                fut_bid,
                fut_ask,
                pos_dual,
                successful_trades,
                failed_trades
            ])
            f.flush()

            # -------------------- Light console output --------------------
            if (now - last_print).total_seconds() >= 1.0:
                print("\n-----------------------------------------------------------------")
                print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
                print("-----------------------------------------------------------------")
                print_positions_and_pnl(always_display=[STOCK_ID, FUT1_ID, FUT2_ID])
                print(f"Spot {spot_mid:.2f} | F1 {f1_mid:.2f} (theo {spot_mid*Theo1:.2f}) | F2 {f2_mid:.2f} (theo {spot_mid*Theo2:.2f})")
                print(f"mis1={mis1:.4f} edge1={edge1:.4f} ex1={ex_edge1:.4f} | mis2={mis2:.4f} edge2={edge2:.4f} ex2={ex_edge2:.4f}")
                last_print = now

            time.sleep(0.05)

    except KeyboardInterrupt:
        print("Stopped by user.")
