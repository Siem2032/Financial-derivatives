import datetime as dt
import time
import logging

from optibook.synchronous_client import Exchange
from optibook.common_types import InstrumentType, OptionKind
from math import floor, ceil

# region setup
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
# endregion

from common.black_scholes import call_value, put_value, call_delta, put_delta
from common.libs import calculate_current_time_to_date

exchange = Exchange(); exchange.connect()
logging.getLogger("client").setLevel("ERROR")

TICK = 0.10   # exchange tick
POS_LIMIT = 100

# ---------- helpers ----------
def round_down_to_tick(price, tick_size): return floor(price / tick_size) * tick_size
def round_up_to_tick(price, tick_size):   return ceil(price / tick_size) * tick_size

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
    """
    Returns (best_bid_price, best_bid_vol, best_ask_price, best_ask_vol) or (None,0,None,0)
    if either side is missing.
    """
    pb = exchange.get_last_price_book(instrument_id=instrument_id)
    if not (pb and pb.bids and pb.asks):
        return None, 0, None, 0
    bb, bv = pb.bids[0].price, pb.bids[0].volume
    ba, av = pb.asks[0].price, pb.asks[0].volume
    return bb, bv, ba, av

def calculate_theoretical_option_value(expiry, strike, option_kind, stock_value, interest_rate, volatility):
    """
    Black–Scholes fair value for call/put given inputs.
    """
    T = calculate_current_time_to_date(expiry)

    if option_kind == OptionKind.CALL:
        return call_value(S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)
    else:
        return put_value (S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)

def calculate_option_delta(expiry, strike, option_kind, stock_value, interest_rate, volatility):
    """
    Black–Scholes delta for call/put given inputs.
    """
    T = calculate_current_time_to_date(expiry)

    if option_kind == OptionKind.CALL:
        return call_delta(S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)
    else:
        return put_delta (S=stock_value, K=strike, T=T, r=interest_rate, sigma=volatility)

# ---------- A1(i): dynamic credit (stock & option spreads) + gentle floor + smoothing ----------
def compute_dynamic_credit_for_option(
    option_id,
    stock_id,
    option_obj,
    stock_value_for_delta,
    *,
    c0=0.12,          
    alpha=1.0,        
    beta=2.0,        
    k_s=0.10,        
    k_o=0.25,        
    r=0.03, sigma=3.0,
    last_credit_value=None, smooth_lambda=0.25
):
    # fetch microstructure
    s_mid, s_spread = get_mid_and_spread(stock_id)
    o_mid, o_spread = get_mid_and_spread(option_id)

    # multiplicative base (uses relative spreads)
    if (s_mid is None) or (o_mid is None) or (s_mid <= 0) or (o_mid <= 0):
        mult = c0
        s_rel = None; o_rel = None
    else:
        s_rel = s_spread / s_mid
        o_rel = o_spread / o_mid
        mult = c0 * (1.0 + alpha * s_rel) * (1.0 + beta * o_rel)

    # additive bump (uses absolute spreads)
    add = 0.0
    if (s_spread is not None) and (o_spread is not None):
        add = k_s * s_spread + k_o * o_spread

    raw = mult + add

    # gentle risk-aware minimum floor so α/β and additive terms still matter
    tick = 0.10
    base_floor = max(tick, 0.6 * c0) 
    min_floor = base_floor
    if option_obj is not None:
        T = calculate_current_time_to_date(option_obj.expiry)
        # softer time & delta components
        min_floor = max(min_floor, c0 * (1.0 + 0.05 * (1.0 / max(1e-6, T**0.5))))
        if stock_value_for_delta is not None:
            try:
                dlt = calculate_option_delta(
                    option_obj.expiry, option_obj.strike, option_obj.option_kind,
                    stock_value_for_delta, r, sigma
                )
                min_floor = max(min_floor, c0 * (1.0 + 0.05 * abs(dlt)))
            except Exception:
                pass

    raw = max(min_floor, raw)
    raw = min(5.0 * c0, raw)  

    # smoothing
    smoothed = raw if (last_credit_value is None) else (1.0 - smooth_lambda) * last_credit_value + smooth_lambda * raw

    # quick debug (feel free to keep it)
    if (s_mid and o_mid and s_mid > 0 and o_mid > 0):
        print(f"[{option_id}] S_rel={s_rel:.5f}  O_rel={o_rel:.5f}  S_sp={s_spread:.2f}  O_sp={o_spread:.2f}  "
              f"mult={mult:.2f} add={add:.2f} credit={smoothed:.2f}")
    else:
        print(f"[{option_id}] (fallback) credit={smoothed:.2f}")

    return smoothed

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

def update_quotes(option_id, theoretical_price, credit, volume, position_limit, tick_size):
    # print new trades
    for tr in exchange.poll_new_trades(instrument_id=option_id):
        print(f"- Last period, traded {tr.volume} lots in {option_id} at {tr.price:.2f}, side {tr.side}.")

    # pull existing orders
    for order_id, order in exchange.get_outstanding_orders(instrument_id=option_id).items():
        print(f"- Deleting old {order.side} order in {option_id} for {order.volume} @ {order.price:8.2f}.")
        exchange.delete_order(instrument_id=option_id, order_id=order_id)

    # prices from theo ± credit
    bid_price = round_down_to_tick(theoretical_price - credit, tick_size)
    ask_price = round_up_to_tick  (theoretical_price + credit, tick_size)

    # crossed-quote guard after rounding
    if bid_price >= ask_price:
        bid_price = round_down_to_tick(bid_price - tick_size, tick_size)
        ask_price = round_up_to_tick  (ask_price + tick_size, tick_size)

    # volumes with hard position limit
    position = exchange.get_positions()[option_id]
    bid_volume = min(volume, position_limit - position)
    ask_volume = min(volume, position_limit + position)

    if bid_volume > 0:
        print(f"- Inserting bid limit order in {option_id} for {bid_volume} @ {bid_price:8.2f}.")
        exchange.insert_order(instrument_id=option_id, price=bid_price, volume=bid_volume, side="bid", order_type="limit")
    if ask_volume > 0:
        print(f"- Inserting ask limit order in {option_id} for {ask_volume} @ {ask_price:8.2f}.")
        exchange.insert_order(instrument_id=option_id, price=ask_price, volume=ask_volume, side="ask", order_type="limit")

def hedge_delta_position(stock_id, options, stock_value):
    """
    A3(iii): Delta-hedge the portfolio by trading the stock via IOC orders.

    Steps:
      1) Compute total portfolio delta (A2).
      2) If |delta| > deadband, trade stock to offset: target_shares = -round(total_delta).
      3) Clamp trade size so final stock position stays within ±100 lots.
      4) Use IOC at best bid/ask to execute immediately (or as much as available).
    """
    # Safety: if we cannot price the stock, skip
    if stock_value is None:
        print("- No stock midpoint available; skipping delta hedge.")
        return

    # 1) Compute total delta & print breakdown (A2 recap)
    total_delta, stock_pos, breakdown = compute_total_delta(
        stock_id=stock_id,
        options=options,
        stock_value=stock_value,
        interest_rate=0.03,
        volatility=3.0,
    )

    print(f"- Stock {stock_id} position: {stock_pos:+d} (delta = {stock_pos:+.4f})")
    for option_id, pos, opt_delta, contrib in breakdown:
        print(f"- {option_id:>20s} | pos={pos:+4d} | Δ={opt_delta:+.4f} | pos*Δ={contrib:+.4f}")
    options_delta_sum = sum(b[3] for b in breakdown)
    print(f"- TOTAL OPTION DELTA                 = {options_delta_sum:+.4f}")
    print(f"- TOTAL PORTFOLIO DELTA (opts+stock)= {total_delta:+.4f}")

    # 2) Deadband: avoid churning tiny residuals
    deadband = 0.5  
    if abs(total_delta) <= deadband:
        print(f"- |delta| <= {deadband:.1f}; no hedge needed.")
        return

    # 3) Target shares to trade (round to nearest whole share)
    target_shares = int(round(-total_delta))

    # 4) Respect stock position limit ±100
    POS_LIMIT = 100
    positions = exchange.get_positions()
    curr_stock_pos = positions.get(stock_id, 0)
    # If target is buy (>0), ensure curr_stock_pos + trade <= +100; if sell (<0), ensure >= -100
    max_buy  = POS_LIMIT - curr_stock_pos
    max_sell = POS_LIMIT + curr_stock_pos  

    if target_shares > 0:
        trade_shares = min(target_shares, max_buy)
    else:
        trade_shares = -min(abs(target_shares), max_sell)

    if trade_shares == 0:
        print("- Hedge would breach position limits; skipping.")
        return

    # 5) Price selection & IOC placement
    bb, bv, ba, av = get_best_bid_ask(stock_id)
    if bb is None or ba is None:
        print("- Missing best bid/ask; cannot hedge now.")
        return

    # Choose IOC price likely to execute immediately
    if trade_shares > 0:
        px = ba
        vol = trade_shares
        side = "bid"
    else:
        px = bb
        vol = abs(trade_shares)
        side = "ask"

    vol = max(1, vol) 

    print(f"- HEDGING: placing IOC {side.upper()} for {vol} {stock_id} @ {px:.2f} to offset ~{total_delta:+.2f} Δ.")
    try:
        exchange.insert_order(
            instrument_id=stock_id,
            price=px,
            volume=vol,
            side=side,
            order_type="ioc",  
        )
    except Exception as e:
        print(f"- IOC insert failed: {e}")

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

last_credit = {}  

while True:
    print("\n-----------------------------------------------------------------")
    print(f"TRADE LOOP ITERATION ENTERED AT {str(dt.datetime.now()):18s} UTC.")
    print("-----------------------------------------------------------------")

    stock_value = get_midpoint_value(STOCK_ID)
    if stock_value is None:
        print("Empty stock book; skipping."); time.sleep(4); continue

    for option_id, option in options.items():
        print(f"\nUpdating instrument {option_id}")

        theo = calculate_theoretical_option_value(
            expiry=option.expiry, strike=option.strike, option_kind=option.option_kind,
            stock_value=stock_value, interest_rate=0.03, volatility=3.0
        )

        prev = last_credit.get(option_id)
        dyn_credit = compute_dynamic_credit_for_option(
            option_id=option_id,
            stock_id=STOCK_ID,
            option_obj=option,
            stock_value_for_delta=stock_value,
            c0=0.12,     
            alpha=1.0,  
            beta=2.0,       
            k_s=0.10,        
            k_o=0.25,        
            r=0.03, sigma=3.0,
            last_credit_value=prev, smooth_lambda=0.25 
        )
        last_credit[option_id] = dyn_credit

        s_mid, s_spread = get_mid_and_spread(STOCK_ID)
        o_mid, o_spread = get_mid_and_spread(option_id)
        if all(x is not None and x > 0 for x in (s_mid, o_mid)):
            print(f"[{option_id}] S_rel={s_spread/s_mid:.5f}  O_rel={o_spread/o_mid:.5f}  credit={dyn_credit:.2f}")

        update_quotes(
            option_id=option_id,
            theoretical_price=theo,
            credit=dyn_credit,
            volume=3,
            position_limit=POS_LIMIT,
            tick_size=TICK,
        )
        time.sleep(0.20) 

    print("\nHedging delta position")
    hedge_delta_position(STOCK_ID, options, stock_value)

    print("\nSleeping for 4 seconds.")
    time.sleep(4)
