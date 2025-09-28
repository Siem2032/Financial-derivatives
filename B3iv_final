import datetime as dt
import time
import math
import logging
import csv
import statistics
import os
from collections import deque

from optibook.synchronous_client import Exchange

# -------------------- Parameters  --------------------
RISK_FREE_RATE = 0.03
THRESHOLD      = 0.20
POSITION_LIMIT = 100

STOCK_ID = "ASML"
FUT1_ID  = "ASML_202603_F"   
FUT2_ID  = "ASML_202606_F"   

FUT1_MATURITY = dt.datetime(2026, 3, 31)
FUT2_MATURITY = dt.datetime(2026, 6, 30)

CSV_NAME = "B3iv.csv"     

# --- speed & safety ---
MAX_TRADE_PER_LOOP      = 12
MIN_NET_FOR_BOTH        = 6
NEAR                    = 90
BURST_EDGE              = 0.60
BURST_MULTIPLIER        = 2
EXEC_EDGE_EPS           = 0.01

# --- hedge knobs ---
HEDGE_SECOND_PASS_DELAY = 0.02
HEDGE_SECOND_PASS_CAP   = 3

# --- strict RPS guard ---
MIN_ORDER_GAP_SEC       = 0.012
MAX_ORDERS_PER_SEC      = 10
MAX_ORDERS_PER_LOOP     = 12

# --- calendar ---
CAL_EPS           = 0.03
CAL_STEP          = 2
CAL_MAX_PER_LOOP  = 1
CAL_DIFF_TARGET   = 20

# -------------------- Connect & logging --------------------
exchange = Exchange()
exchange.connect()
logging.getLogger("client").setLevel("ERROR")

# -------------------- B2iii-style helpers --------------------
def trade_would_breach_position_limit(instrument_id, volume, side, position_limit=POSITION_LIMIT):
    """B2iii-style guard (not used for flow control here, kept for parity)."""
    positions = exchange.get_positions()
    cur = positions.get(instrument_id, 0)
    if volume == 0:
        return True
    if side == "bid":  
        return cur + volume > position_limit
    elif side == "ask": 
        return cur - volume < -position_limit
    else:
        raise ValueError(f"Invalid side: {side} (use 'bid' or 'ask')")

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
        bid = order_book.bids[0].price
        ask = order_book.asks[0].price
        return (bid + ask) / 2.0
    return None

def max_lots_allowed(instrument_id, side, position_limit=POSITION_LIMIT):
    positions = exchange.get_positions()
    cur = positions.get(instrument_id, 0)
    if side == "bid":  
        return max(position_limit - cur, 0)
    else:              
        return max(position_limit + cur, 0)

# -------------------- Small utilities --------------------
def top_of_book(book):
    bid = book.bids[0].price if (book and book.bids) else 0
    ask = book.asks[0].price if (book and book.asks) else 0
    return bid, ask

def half_spread(book):
    if book and book.bids and book.asks:
        return max(book.asks[0].price - book.bids[0].price, 0) / 2.0
    return 0.0

def clamp_volume(instrument_id, side, requested):
    return max(0, min(int(requested), int(max_lots_allowed(instrument_id, side))))

def years_to_maturity(maturity_dt):
    now = dt.datetime.utcnow()
    return max((maturity_dt - now).days, 0) / 365.0

def can_open_in_direction(cur_pos, mis):
    """mis>0 → SELL future; mis<0 → BUY future. Near limit: only unwind toward 0."""
    if abs(cur_pos) < NEAR:
        return True
    if mis > 0 and cur_pos > 0:   
        return True
    if mis < 0 and cur_pos < 0:  
        return True
    return False

def executable_edge(stock_book, fut_book, mis, theo_mult):
    """
    Touch-based executable edge:
    mis>0 → SELL fut @ bid + BUY stock @ ask  => edge = f_bid - s_ask*theo
    mis<0 → BUY fut @ ask + SELL stock @ bid  => edge = s_bid*theo - f_ask
    """
    s_bid, s_ask = top_of_book(stock_book)
    f_bid, f_ask = top_of_book(fut_book)
    if s_bid == 0 or s_ask == 0 or (f_bid == 0 and f_ask == 0):
        return -1e9
    if mis > 0:
        return f_bid - s_ask * theo_mult
    else:
        return s_bid * theo_mult - f_ask

def theo_spread(F1_theo, F2_theo):
    return F1_theo - F2_theo

# -------------------- RPS-safe IOC sender --------------------
_order_times = deque()
_last_order_ts = 0.0
_loop_order_count = 0

def _rps_wait():
    """Wait so we never exceed per-second/per-loop limits."""
    global _last_order_ts, _loop_order_count
    while True:
        now = time.time()
        # purge >1s
        while _order_times and now - _order_times[0] > 1.0:
            _order_times.popleft()
        gap_ok  = (now - _last_order_ts) >= MIN_ORDER_GAP_SEC
        sec_ok  = (len(_order_times) < MAX_ORDERS_PER_SEC)
        loop_ok = (_loop_order_count < MAX_ORDERS_PER_LOOP)
        if gap_ok and sec_ok and loop_ok:
            return
        # sleep minimal amount needed
        sleep_gap = 0.0 if gap_ok else (MIN_ORDER_GAP_SEC - (now - _last_order_ts))
        sleep_sec = 0.0
        if not sec_ok:
            sleep_sec = 1.0 - (now - _order_times[0]) + 1e-4
        time.sleep(max(sleep_gap, sleep_sec, 0.002))

def send_ioc(instrument_id, price, volume, side):
    """RPS-safe IOC submit."""
    global _last_order_ts, _loop_order_count
    if volume <= 0:
        return
    _rps_wait()
    exchange.insert_order(instrument_id, price=price, volume=volume, side=side, order_type="ioc")
    ts = time.time()
    _order_times.append(ts)
    _last_order_ts = ts
    _loop_order_count += 1

# -------------------- Trading (future vs stock, verify fills) --------------------
def trade_one_future_vs_stock(fut_id, fut_book, mis, stock_book, lots_hint=None):
    if abs(mis) <= THRESHOLD:
        return 0
    if not (stock_book and stock_book.bids and stock_book.asks):
        return 0
    if not (fut_book and (fut_book.bids or fut_book.asks)):
        return 0

    s_bid, s_ask = top_of_book(stock_book)
    f_bid, f_ask = top_of_book(fut_book)
    if s_bid == 0 or s_ask == 0 or (f_bid == 0 and f_ask == 0):
        return 0

    s_bid_sz = stock_book.bids[0].volume if stock_book.bids else 0
    s_ask_sz = stock_book.asks[0].volume if stock_book.asks else 0
    f_bid_sz = fut_book.bids[0].volume if fut_book.bids else 0
    f_ask_sz = fut_book.asks[0].volume if fut_book.asks else 0

    desired = max(1, int(abs(mis) * 50))
    if lots_hint is not None:
        desired = min(desired, int(lots_hint))
    desired = min(desired, MAX_TRADE_PER_LOOP)

    if mis > 0:
        # SELL future @ bid, BUY stock @ ask
        max_by_liq = min(f_bid_sz, s_ask_sz)
        lots = min(desired, max_by_liq,
                   clamp_volume(fut_id, "ask", desired),
                   clamp_volume(STOCK_ID, "bid", desired))
        if lots <= 0: 
            return 0
        pos0 = exchange.get_positions(); f0 = pos0.get(fut_id, 0)
        send_ioc(fut_id, f_bid, lots, "ask")          
        time.sleep(0.002)
        f1 = exchange.get_positions().get(fut_id, 0)
        got = max(0, f0 - f1)                          
        if got <= 0: 
            return 0
        send_ioc(STOCK_ID, s_ask, got, "bid")         
        time.sleep(0.002)
        print(f"[{fut_id}] BUY stock / SELL future x{got}")
        return int(got)

    else:
        # BUY future @ ask, SELL stock @ bid
        max_by_liq = min(f_ask_sz, s_bid_sz)
        lots = min(desired, max_by_liq,
                   clamp_volume(fut_id, "bid", desired),
                   clamp_volume(STOCK_ID, "ask", desired))
        if lots <= 0: 
            return 0
        pos0 = exchange.get_positions(); f0 = pos0.get(fut_id, 0)
        send_ioc(fut_id, f_ask, lots, "bid")
        time.sleep(0.002)
        f1 = exchange.get_positions().get(fut_id, 0)
        got = max(0, f1 - f0)                          
        if got <= 0: 
            return 0
        send_ioc(STOCK_ID, s_bid, got, "ask")
        time.sleep(0.002)
        print(f"[{fut_id}] SELL stock / BUY future x{got}")
        return int(got)

# -------------------- Calendar swap --------------------
def try_one_calendar_swap(f1_book, f2_book, pos_f1, pos_f2, theo_diff_val):
    """
    Tiny F1<->F2 swap if executable calendar ~ theoretical.
    Priority:
      1) Opposite-sign: reduce min(|F1|, |F2|)
      2) Same-sign: if |F1-F2| > CAL_DIFF_TARGET, move from bigger to smaller
    """
    if not (f1_book and f2_book):
        return 0
    if not ((f1_book.bids or f1_book.asks) and (f2_book.bids or f2_book.asks)):
        return 0

    def cap(vol, fid, side):
        return clamp_volume(fid, side, min(CAL_STEP, max(1, int(vol))))

    # Opposite-sign trim
    if pos_f1 > 0 and pos_f2 < 0 and f1_book.bids and f2_book.asks:
        f1_bid = f1_book.bids[0].price; f2_ask = f2_book.asks[0].price
        if abs((f1_bid - f2_ask) - theo_diff_val) <= CAL_EPS:
            v = min(abs(pos_f1), abs(pos_f2))
            v = min(cap(v, FUT1_ID, "ask"), cap(v, FUT2_ID, "bid"))
            if v > 0:
                send_ioc(FUT1_ID, f1_bid, v, "ask")
                send_ioc(FUT2_ID, f2_ask, v, "bid")
                print(f"Cal-swap opp: SELL {v} {FUT1_ID} / BUY {v} {FUT2_ID}")
                return 1

    if pos_f1 < 0 and pos_f2 > 0 and f1_book.asks and f2_book.bids:
        f1_ask = f1_book.asks[0].price; f2_bid = f2_book.bids[0].price
        if abs((f1_ask - f2_bid) - theo_diff_val) <= CAL_EPS:
            v = min(abs(pos_f1), abs(pos_f2))
            v = min(cap(v, FUT1_ID, "bid"), cap(v, FUT2_ID, "ask"))
            if v > 0:
                send_ioc(FUT1_ID, f1_ask, v, "bid")
                send_ioc(FUT2_ID, f2_bid, v, "ask")
                print(f"Cal-swap opp: BUY {v} {FUT1_ID} / SELL {v} {FUT2_ID}")
                return 1

    # Same-sign tidy if big imbalance
    if pos_f1 * pos_f2 > 0 and abs(pos_f1 - pos_f2) > CAL_DIFF_TARGET:
        # F1 bigger → SELL F1, BUY F2
        if pos_f1 > pos_f2 and f1_book.bids and f2_book.asks:
            f1_bid = f1_book.bids[0].price; f2_ask = f2_book.asks[0].price
            if abs((f1_bid - f2_ask) - theo_diff_val) <= CAL_EPS:
                v = min(abs(pos_f1 - pos_f2),
                        cap(CAL_STEP, FUT1_ID, "ask"),
                        cap(CAL_STEP, FUT2_ID, "bid"))
                if v > 0:
                    send_ioc(FUT1_ID, f1_bid, v, "ask")
                    send_ioc(FUT2_ID, f2_ask, v, "bid")
                    print(f"Cal-swap same: SELL {v} {FUT1_ID} / BUY {v} {FUT2_ID}")
                    return 1
        # F2 bigger → SELL F2, BUY F1
        if pos_f2 > pos_f1 and f2_book.bids and f1_book.asks:
            f2_bid = f2_book.bids[0].price; f1_ask = f1_book.asks[0].price
            if abs((f1_ask - f2_bid) - theo_diff_val) <= CAL_EPS:
                v = min(abs(pos_f2 - pos_f1),
                        cap(CAL_STEP, FUT2_ID, "ask"),
                        cap(CAL_STEP, FUT1_ID, "bid"))
                if v > 0:
                    send_ioc(FUT2_ID, f2_bid, v, "ask")
                    send_ioc(FUT1_ID, f1_ask, v, "bid")
                    print(f"Cal-swap same: SELL {v} {FUT2_ID} / BUY {v} {FUT1_ID}")
                    return 1

    return 0

# -------------------- CSV prep --------------------
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
            # reset strict per-loop cap
            global _loop_order_count
            _loop_order_count = 0

            run += 1
            now = dt.datetime.utcnow()
            elapsed_m = (now - start_time).total_seconds() / 60.0

            # books
            stock_book = exchange.get_last_price_book(STOCK_ID)
            f1_book    = exchange.get_last_price_book(FUT1_ID)
            f2_book    = exchange.get_last_price_book(FUT2_ID)

            spot_mid = get_mid_price(stock_book)
            f1_mid   = get_mid_price(f1_book)
            f2_mid   = get_mid_price(f2_book)

            if not (spot_mid and f1_mid and f2_mid):
                time.sleep(0.05)
                continue

            # theo + mis
            T1 = years_to_maturity(FUT1_MATURITY)
            T2 = years_to_maturity(FUT2_MATURITY)
            Theo1 = math.exp(RISK_FREE_RATE * T1)
            Theo2 = math.exp(RISK_FREE_RATE * T2)
            F1_theo = spot_mid * Theo1
            F2_theo = spot_mid * Theo2
            mis1 = f1_mid - F1_theo
            mis2 = f2_mid - F2_theo

            # quick rank (mid-based)
            cost1 = half_spread(stock_book) + half_spread(f1_book)
            cost2 = half_spread(stock_book) + half_spread(f2_book)
            edge1 = abs(mis1) - max(THRESHOLD, cost1)
            edge2 = abs(mis2) - max(THRESHOLD, cost2)

            # hard gate (touch-based)
            ex_edge1 = executable_edge(stock_book, f1_book, mis1, Theo1)
            ex_edge2 = executable_edge(stock_book, f2_book, mis2, Theo2)

            # candidate list
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
                    # both actionable → prioritize by executable edge
                    cands.sort(key=lambda x: (x[5], x[4]), reverse=True)

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

            # ------ One-pass delta hedge vs total futures ------
            if traded_F1 or traded_F2:
                positions = exchange.get_positions()
                stock_pos = positions.get(STOCK_ID, 0)
                f1_pos    = positions.get(FUT1_ID, 0)
                f2_pos    = positions.get(FUT2_ID, 0)
                hedge_diff = stock_pos + f1_pos + f2_pos

                s_bid, s_ask = top_of_book(stock_book)

                if hedge_diff > 0 and s_bid > 0:
                    vol_req = min(hedge_diff, HEDGE_SECOND_PASS_CAP)
                    vol = clamp_volume(STOCK_ID, "ask", vol_req)
                    if vol > 0:
                        send_ioc(STOCK_ID, s_bid, vol, "ask")
                        print(f"Hedge: SOLD {vol} stock (req {hedge_diff})")
                elif hedge_diff < 0 and s_ask > 0:
                    vol_req = min(-hedge_diff, HEDGE_SECOND_PASS_CAP)
                    vol = clamp_volume(STOCK_ID, "bid", vol_req)
                    if vol > 0:
                        send_ioc(STOCK_ID, s_ask, vol, "bid")
                        print(f"Hedge: BOUGHT {vol} stock (req {-hedge_diff})")

                time.sleep(HEDGE_SECOND_PASS_DELAY)
                positions = exchange.get_positions()
                residual = positions.get(STOCK_ID,0) + positions.get(FUT1_ID,0) + positions.get(FUT2_ID,0)

                if residual != 0:
                    fix = min(abs(residual), HEDGE_SECOND_PASS_CAP)
                    s_bid, s_ask = top_of_book(stock_book)
                    if residual > 0 and s_bid > 0:
                        vol = clamp_volume(STOCK_ID, "ask", fix)
                        if vol > 0:
                            send_ioc(STOCK_ID, s_bid, vol, "ask")
                            print(f"Hedge micro-fix: SOLD {vol} stock (residual {residual})")
                    elif residual < 0 and s_ask > 0:
                        vol = clamp_volume(STOCK_ID, "bid", fix)
                        if vol > 0:
                            send_ioc(STOCK_ID, s_ask, vol, "bid")
                            print(f"Hedge micro-fix: BOUGHT {vol} stock (residual {residual})")

            # ------ Optional calendar nudge ------
            attempts = CAL_MAX_PER_LOOP
            if attempts > 0:
                positions = exchange.get_positions()
                f1_pos = positions.get(FUT1_ID, 0)
                f2_pos = positions.get(FUT2_ID, 0)
                theo_diff_val = theo_spread(spot_mid * Theo1, spot_mid * Theo2)
                while attempts > 0:
                    did = try_one_calendar_swap(f1_book, f2_book, f1_pos, f2_pos, theo_diff_val)
                    if not did:
                        break
                    attempts -= 1
                    p = exchange.get_positions()
                    f1_pos = p.get(FUT1_ID, 0)
                    f2_pos = p.get(FUT2_ID, 0)

            # ------ Log CSV row ------
            pnl = exchange.get_pnl() or 0.0
            pnl_history.append(pnl)
            mean_pnl = statistics.mean(pnl_history)
            std_pnl  = statistics.stdev(pnl_history) if len(pnl_history) > 1 else 0.0

            # choose which future to log
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
            failed_trades = int(failed_trades)

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

            # ------ light console output ------
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
