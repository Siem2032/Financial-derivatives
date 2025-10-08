import datetime as dt
import time
import logging
from math import floor, ceil

from optibook.synchronous_client import Exchange
from optibook.common_types import InstrumentType, OptionKind

# ---------- setup ----------
import sys, subprocess
def install_and_import(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    finally:
        globals()[package] = __import__(package)
install_and_import("scipy")
sys.path.append("/home/workspace/your_optiver_workspace")

from common.black_scholes import call_value, put_value, call_delta, put_delta
from common.libs import calculate_current_time_to_date

exchange = Exchange()
exchange.connect()
logging.getLogger("client").setLevel("ERROR")

# ---------- constants ----------
TICK = 0.10
POS_LIMIT = 100           
SOFT_DELTA_LIMIT = 100    
BASE_VOL = 3             
MAX_QUOTE_VOL = 20       
DEADBAND = 20            

# ---------- helpers ----------
def round_down_to_tick(p, t): return floor(p / t) * t
def round_up_to_tick(p, t):   return ceil (p / t) * t

def get_midpoint_value(instrument_id):
    pb = exchange.get_last_price_book(instrument_id=instrument_id)
    if not (pb and pb.bids and pb.asks): return None
    return (pb.bids[0].price + pb.asks[0].price) / 2.0

def get_mid_and_spread(instrument_id):
    pb = exchange.get_last_price_book(instrument_id=instrument_id)
    if not (pb and pb.bids and pb.asks): return None, None
    bb, ba = pb.bids[0].price, pb.asks[0].price
    return (bb + ba) / 2.0, (ba - bb)

def get_best_bid_ask(instrument_id):
    pb = exchange.get_last_price_book(instrument_id=instrument_id)
    if not (pb and pb.bids and pb.asks): return None, 0, None, 0
    bb, bv = pb.bids[0].price, pb.bids[0].volume
    ba, av = pb.asks[0].price, pb.asks[0].volume
    return bb, bv, ba, av

# ---------- B&S wrappers (sigma fixed at 3) ----------
def calculate_theoretical_option_value(expiry, strike, option_kind, stock_value, interest_rate, volatility):
    T = calculate_current_time_to_date(expiry)
    if option_kind == OptionKind.CALL:
        return call_value(S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)
    else:
        return put_value (S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)

def calculate_option_delta(expiry, strike, option_kind, stock_value, interest_rate, volatility):
    T = calculate_current_time_to_date(expiry)
    if option_kind == OptionKind.CALL:
        return call_delta(S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)
    else:
        return put_delta (S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)

# ---------- simple dynamic credit (kept compact) ----------
def compute_dynamic_credit_for_option(option_id, stock_id, option_obj, stock_value_for_delta,
                                      *, c0=0.10, alpha=0.6, beta=1.6, k_s=0.06, k_o=0.22,
                                      r=0.03, sigma=3.0, last_credit_value=None, smooth_lambda=0.25,
                                      global_widen=1.0, option_exposure_widen=0.0):
    s_mid, s_spread = get_mid_and_spread(stock_id)
    o_mid, o_spread = get_mid_and_spread(option_id)

    if (s_mid is None) or (o_mid is None) or (s_mid <= 0) or (o_mid <= 0):
        mult = c0; s_rel = o_rel = None
    else:
        s_rel = s_spread / s_mid
        o_rel = o_spread / o_mid
        mult = c0 * (1.0 + alpha * s_rel) * (1.0 + beta * o_rel)
    add = (k_s * s_spread + k_o * o_spread) if (s_spread is not None and o_spread is not None) else 0.0
    raw = mult + add

    # gentle floor + tiny risk awareness
    min_floor = max(TICK, 0.6 * c0)
    if option_obj is not None:
        T = calculate_current_time_to_date(option_obj.expiry)
        min_floor = max(min_floor, c0 * (1.0 + 0.04 * (1.0 / max(1e-6, T**0.5))))
        try:
            dlt = calculate_option_delta(option_obj.expiry, option_obj.strike, option_obj.option_kind,
                                         stock_value_for_delta, r, sigma)
            min_floor = max(min_floor, c0 * (1.0 + 0.04 * abs(dlt)))
        except Exception:
            pass

    raw *= global_widen
    raw *= (1.0 + option_exposure_widen)
    raw = max(min_floor, min(5.0 * c0, raw))
    smoothed = raw if (last_credit_value is None) else (1.0 - smooth_lambda) * last_credit_value + smooth_lambda * raw

    # short debug
    if (s_mid and o_mid and s_mid > 0 and o_mid > 0):
        print(f"[{option_id}] S_rel={s_spread/s_mid:.5f} O_rel={o_spread/o_mid:.5f} S_sp={s_spread:.2f} O_sp={o_spread:.2f} credit={smoothed:.2f}")
    else:
        print(f"[{option_id}] credit={smoothed:.2f}")
    return smoothed

# ---------- total delta ----------
def compute_total_delta(stock_id, options, stock_value, interest_rate=0.03, volatility=3.0):
    positions = exchange.get_positions()
    total_options_delta = 0.0
    breakdown = []
    for option_id, option in options.items():
        pos = positions.get(option_id, 0)
        d = calculate_option_delta(option.expiry, option.strike, option.option_kind,
                                   stock_value, interest_rate, volatility)
        contrib = pos * d
        total_options_delta += contrib
        breakdown.append((option_id, pos, d, contrib))
    stock_pos = positions.get(stock_id, 0)
    return total_options_delta + stock_pos, stock_pos, breakdown

# ---------- simple dynamic volume ----------
def compute_dynamic_volume(option_id, option_obj, stock_id, total_delta_now, stock_pos_now):
    positions = exchange.get_positions()
    pos_in_opt = positions.get(option_id, 0)

    o_mid, o_spread = get_mid_and_spread(option_id)
    pb = exchange.get_last_price_book(instrument_id=option_id)
    depth_top = min(pb.bids[0].volume, pb.asks[0].volume) if (pb and pb.bids and pb.asks) else 0

    rel_o = (o_spread / o_mid) if (o_mid and o_mid > 0) else 0.05
    liq_factor   = 0.5 + (0.2 / max(0.002, rel_o))
    liq_factor   = min(3.0, max(0.6, liq_factor))
    depth_factor = 0.5 + min(2.0, depth_top / 5.0)

    port_factor  = max(0.30, (SOFT_DELTA_LIMIT - abs(total_delta_now)) / SOFT_DELTA_LIMIT)
    stock_headroom = max(0, POS_LIMIT - abs(stock_pos_now))
    stock_factor   = max(0.30, stock_headroom / POS_LIMIT)
    inst_factor    = max(0.30, (POS_LIMIT - abs(pos_in_opt)) / POS_LIMIT)

    raw = BASE_VOL * liq_factor * depth_factor * port_factor * stock_factor * inst_factor
    vol = max(1, min(MAX_QUOTE_VOL, int(round(raw))))

    # marginal-Δ cap (worst-case one-sided fill) with 50% buffer
    try:
        d_opt = calculate_option_delta(option_obj.expiry, option_obj.strike, option_obj.option_kind,
                                       get_midpoint_value(stock_id), 0.03, 3.0)
        delta_headroom = max(0.0, SOFT_DELTA_LIMIT - abs(total_delta_now))
        allowed_by_delta = int((0.5 * delta_headroom) / max(0.05, abs(d_opt)))
        vol = max(1, min(vol, allowed_by_delta))
    except Exception:
        pass

    max_buy  = POS_LIMIT - pos_in_opt
    max_sell = POS_LIMIT + pos_in_opt
    vol = max(1, min(vol, max_buy, max_sell, MAX_QUOTE_VOL))
    return vol

# ---------- quote maintenance ----------
def update_quotes(option_id, theoretical_price, credit, volume, position_limit, tick_size):
    for tr in exchange.poll_new_trades(instrument_id=option_id):
        print(f"- Last period, traded {tr.volume} lots in {option_id} at {tr.price:.2f}, side {tr.side}.")

    for order_id, order in exchange.get_outstanding_orders(instrument_id=option_id).items():
        print(f"- Deleting old {order.side} order in {option_id} for {order.volume} @ {order.price:8.2f}.")
        exchange.delete_order(instrument_id=option_id, order_id=order_id)

    bid_price = round_down_to_tick(theoretical_price - credit, tick_size)
    ask_price = round_up_to_tick  (theoretical_price + credit, tick_size)
    if bid_price >= ask_price:
        bid_price = round_down_to_tick(bid_price - tick_size, tick_size)
        ask_price = round_up_to_tick  (ask_price + tick_size, tick_size)

    position = exchange.get_positions()[option_id]
    bid_volume = min(volume, position_limit - position)
    ask_volume = min(volume, position_limit + position)

    if bid_volume > 0:
        print(f"- Inserting bid limit order in {option_id} for {bid_volume} @ {bid_price:8.2f}.")
        exchange.insert_order(instrument_id=option_id, price=bid_price, volume=bid_volume, side="bid", order_type="limit")
    if ask_volume > 0:
        print(f"- Inserting ask limit order in {option_id} for {ask_volume} @ {ask_price:8.2f}.")
        exchange.insert_order(instrument_id=option_id, price=ask_price, volume=ask_volume, side="ask", order_type="limit")

# ---------- fast IOC hedge (single-shot up to limits) ----------
def hedge_delta_position(stock_id, options, stock_value):
    if stock_value is None:
        print("- No stock midpoint available; skipping delta hedge.")
        return

    total_delta, stock_pos, breakdown = compute_total_delta(
        stock_id=stock_id, options=options, stock_value=stock_value, interest_rate=0.03, volatility=3.0
    )

    print(f"- Stock {stock_id} position: {stock_pos:+d} (delta = {stock_pos:+.4f})")
    for option_id, pos, opt_delta, contrib in breakdown:
        print(f"- {option_id:>20s} | pos={pos:+4d} | Δ={opt_delta:+.4f} | pos*Δ={contrib:+.4f}")
    print(f"- TOTAL PORTFOLIO DELTA (opts+stock)= {total_delta:+.4f}")

    if abs(total_delta) <= DEADBAND:
        print(f"- |delta| <= {DEADBAND:.1f}; no hedge needed.")
        return

    target_shares = int(round(-total_delta))
    curr_stock_pos = exchange.get_positions().get(stock_id, 0)
    max_buy  = POS_LIMIT - curr_stock_pos
    max_sell = POS_LIMIT + curr_stock_pos

    if target_shares > 0:
        trade_shares = min(target_shares, max_buy)
    else:
        trade_shares = -min(abs(target_shares), max_sell)

    if trade_shares == 0:
        print("- Hedge would breach position limits; skipping.")
        return

    bb, _, ba, _ = get_best_bid_ask(stock_id)
    if bb is None or ba is None:
        print("- Missing best bid/ask; cannot hedge now.")
        return

    if trade_shares > 0:
        px = ba; vol = trade_shares; side = "bid"   
    else:
        px = bb; vol = abs(trade_shares); side = "ask"  

    print(f"- HEDGING: placing IOC {side.upper()} for {vol} {stock_id} @ {px:.2f} to offset ~{total_delta:+.2f} Δ.")
    try:
        exchange.insert_order(instrument_id=stock_id, price=px, volume=vol, side=side, order_type="ioc")
    except Exception as e:
        print(f"- IOC insert failed: {e}")

# ---------- instruments ----------
def load_instruments_for_underlying(underlying_stock_id):
    all_instr = exchange.get_instruments()
    stock = all_instr[underlying_stock_id]
    options = {
        iid: instr
        for iid, instr in all_instr.items()
        if instr.instrument_type == InstrumentType.STOCK_OPTION and instr.base_instrument_id == underlying_stock_id
    }
    return stock, options

# ---------- main ----------
STOCK_ID = "ASML"
stock, options = load_instruments_for_underlying(STOCK_ID)

# A4(iii): trade ALL listed ASML options (no maturity filter)
print(f"- Trading ALL {len(options)} ASML options.")

last_credit = {}
fills = 0
edge_sum = 0.0
credit_sum = 0.0
size_sum = 0
loops = 0
delta_abs_sum = 0.0

while True:
    print("\n-----------------------------------------------------------------")
    print(f"TRADE LOOP ENTERED AT {str(dt.datetime.now())[:19]} UTC")
    print("-----------------------------------------------------------------")

    stock_value = get_midpoint_value(STOCK_ID)
    if stock_value is None:
        print("Empty stock book; skipping.")
        time.sleep(4)
        continue

    # global widen slightly if portfolio Δ drifts far from 0 (simple & symmetric)
    total_delta_now, stock_pos_now, _ = compute_total_delta(
        stock_id=STOCK_ID, options=options, stock_value=stock_value, interest_rate=0.03, volatility=3.0
    )
    abs_d = abs(total_delta_now)
    global_widen = 1.0 + 0.02 * max(0.0, abs_d - 10.0)
    global_widen = min(global_widen, 1.25)

    for option_id, option in options.items():
        print(f"\nUpdating instrument {option_id}")

        theo = calculate_theoretical_option_value(
            expiry=option.expiry, strike=option.strike, option_kind=option.option_kind,
            stock_value=stock_value, interest_rate=0.03, volatility=3.0
        )

        pos = exchange.get_positions().get(option_id, 0)
        is_call = option.option_kind == OptionKind.CALL
        signed_exposure = pos * (1.0 if is_call else -1.0)
        per_option_widen = min(0.15, max(0.0, 0.002 * max(0.0, signed_exposure)))

        prev = last_credit.get(option_id)
        dyn_credit = compute_dynamic_credit_for_option(
            option_id=option_id, stock_id=STOCK_ID, option_obj=option,
            stock_value_for_delta=stock_value,
            c0=0.10, alpha=0.6, beta=1.6, k_s=0.06, k_o=0.22,
            r=0.03, sigma=3.0,
            last_credit_value=prev, smooth_lambda=0.25,
            global_widen=global_widen, option_exposure_widen=per_option_widen
        )
        last_credit[option_id] = dyn_credit

        vol_to_quote = compute_dynamic_volume(
            option_id=option_id, option_obj=option, stock_id=STOCK_ID,
            total_delta_now=total_delta_now, stock_pos_now=stock_pos_now
        )
        print(f"[{option_id}] dynamic_volume={vol_to_quote}")

        update_quotes(
            option_id=option_id,
            theoretical_price=theo,
            credit=dyn_credit,
            volume=vol_to_quote,
            position_limit=POS_LIMIT,
            tick_size=TICK,
        )

        # perf tallies (very rough)
        credit_sum += dyn_credit
        size_sum   += vol_to_quote

        time.sleep(0.20)  # frequency guard

    # hedge
    print("\nHedging delta position")
    hedge_delta_position(STOCK_ID, options, stock_value)

    # simple performance snapshot (optional but handy for report)
    loops += 1
    fills += sum(tr.volume for oid in options.keys() for tr in exchange.poll_new_trades(instrument_id=oid))
    avg_credit = credit_sum / max(1, loops * len(options))
    avg_size   = size_sum   / max(1, loops * len(options))
    # keep track of avg |Δ|
    td, _, _ = compute_total_delta(STOCK_ID, options, stock_value)
    delta_abs_sum += abs(td)

    print("\n===== PERFORMANCE SNAPSHOT =====")
    print(f"Fills: {fills}, Avg credit used: {avg_credit:.3f}, Avg quoted size: {avg_size:.2f}")
    print(f"Avg |total Δ|: {delta_abs_sum/max(1,loops):.2f}")
    print("================================")

    print("\nSleeping for 4 seconds.")
    time.sleep(4)
