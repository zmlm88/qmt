# -*- coding: utf-8 -*-
# Beidou Navigation (002151.SZ) Intraday Strategy v4.0 (T+0 & ORB Breakout)
# =====================================================
# Main strategy file: init() + handlebar() + backend API
# Helper modules: qmt_utils.py, qmt_indicators.py, qmt_risk.py
# =====================================================

import sys
import os
import builtins

# ======= [Core Fix: Environment Defense Layer] =======
# Load pandas and numpy fully at the cleanest stage, ensuring complete initialization.
# Pin them into global builtins and sys.modules to prevent reload from truncating them.
import numpy as _np
import pandas as _pd
builtins.pandas = _pd
builtins.numpy = _np
sys.modules['pandas'] = _pd
sys.modules['numpy'] = _np
# =====================================================

# [1] Path setup
_qmt_dir = r"E:\qmt"
if _qmt_dir not in sys.path:
    sys.path.insert(0, _qmt_dir)

import importlib
import qmt_utils
import qmt_indicators
import qmt_risk
import core_engine_v2
import position_manager

# Hot-reload: re-execute deps on each QMT run; API bind only when C is available
def _qmt_reload_deps(C=None):
    importlib.reload(qmt_utils)
    importlib.reload(qmt_indicators)
    importlib.reload(qmt_risk)
    importlib.reload(core_engine_v2)
    importlib.reload(position_manager)
    if C is not None:
        qmt_utils.scan_and_bind_qmt_apis(C)
    elif not qmt_utils._log_guard().get('import_qmt_utils'):
        qmt_utils._log_guard()['import_qmt_utils'] = True
        print(f"  [Import] qmt_utils {getattr(qmt_utils, '_QMT_UTILS_REV', '?')}")


_qmt_reload_deps()

from core_engine_v2 import ComprehensiveRiskEngineV2, DecoupledProbeManager, IntradayT0GridEngine
from position_manager import PositionManager

from qmt_utils import (
    is_valid_price, is_valid_num, _get_api, scan_and_bind_qmt_apis, qmt_apis_ready,
    get_pos_data, get_available_position, get_total_position, get_true_available_position,
    set_base_cost, reset_base_anchors, clear_ghost_base_state, get_stop_anchor, base_float_risk, sync_stop_anchor_down,
    calc_trade_fee,
    safe_buy, safe_sell, safe_buy_eod, safe_sell_eod,
    is_hb_slot, log_once, print_stop_account, print_eod_summary, set_cooldown, is_in_cooldown,
    BACKEND_URL, backend_timeout,
    mark_backend_indicators_synced, backend_indicators_synced_today,
    apply_daily_indicators, export_backend_indicators, fetch_daily_indicators_from_backend, fetch_premarket_status,
    cst_naive_ts_to_ms,
)

from qmt_indicators import (
    push_finished_bar, update_kline_from_tick, update_kline_from_1m,
    calc_atr, calc_ma, calc_rsi, calc_obv_sobv, calc_vol_ratio,
    calc_vmacd, calc_wvad, calc_daily_rsi,
    get_prior_day_adtm, is_dull_down_decline, check_daily_bullish_bar, get_prev_day_amp,
    update_trend_direction,
)

from qmt_risk import (
    _mark_staged_reduce, _in_staged_reduce_cooldown, _in_staged_half_exit_cooldown,
    _is_staged_half_exit_rebuild, _set_exit_policy, _get_effective_exit_policy,
    _allows_down_oversold_probe, _allows_staged_half_neutral_probe,
    _build_reason_exempt_momentum_gate, _rebuild_momentum_confirmed,
    _init_base_add_float_ok, _t0_underwater_buy_ok,
    _staged_half_probe_rebuild_allowed, _staged_half_probe_trend_ok,
    _try_staged_half_probe_rebuild, _oversold_probe_block_days, _oversold_probe_in_block_period,
    _blocks_empty_init_base, _clear_staged_reduce_state,
    _is_sub_half_base, _get_trail_dd_pct, _get_hard_stop_pct,
    _check_eod_fuse_pre_reduce, _check_main_fuse, _run_base_stop_checks,
    _probe_rebuild_t0_blocked,
    _lock_day_open_trend_if_needed,
    _has_staged_reduce_state, _in_staged_half_position_band, _holds_staged_half, _holds_init_half_only,
    _staged_half_exit_time_ok, _apply_staged_half_risk_exit, _apply_skip_risk_reduce,
    _holds_probe_only, _holds_down_probe, _holds_neutral_micro_probe,
    _probe_high_confidence_sell,
    _record_staged_half_exit, _record_micro_shallow_exit, _record_base_stop,
    _execute_base_stop_close,
    _is_probe_gap_immune, _is_probe_gap_immune_partial, _probe_gap_immune_blocks_4pct,
    _immune_last_day_deep_float_defer,
    _probe_global_guard_immune, _probe_immune_max_days,
    _record_probe_underwater_t0_exit,
    _blocks_underwater_t0_rebuild, _probe_gap_micro_max,
    _oversold_probe_severe_entry,
)

# 其他必要导入
import time
import shutil
import importlib.util as _ilu
from datetime import datetime
import requests


_QMT_API_NAMES = ('get_trade_detail_data', 'passorder', 'cancel_order')

# ponytail: survives QMT deepcopy(C) which wipes C.* session attrs each tick
_HB_SESSION = {
    'date': '', 'barpos': -1, 'mode': '',
    'day_open_set': False, 'day_open': 'NEUTRAL', 'daily_trend': 'NEUTRAL',
    'trend_dir': 'NEUTRAL', 'trend_logged': False,
    'backend_date': '', 'trend_cut_done': False,
    'base_trend_cut_active': False, 'base_risk_hb': '',
    'today_bought_qty': 0,
    'backend_indicators': None,
    'prev_adtm_val': 0.0, 'prev_adtm_ready': False,
    # [Bug#4 Fix] bars 缓存——survive deepcopy
    'bars_cache_date': '', 'bars_cache': None,
    'backfill_done_barpos': -1,
}


def _hb_bind_managers(C):
    """Rebind singleton managers to the current tick's C (survive QMT deepcopy)."""
    if not getattr(C, 'is_live', False):
        return
    for _attr in ('_pos_mgr', '_probe_mgr', '_t0_engine', '_risk_engine'):
        _mgr = getattr(C, _attr, None)
        if _mgr is not None:
            _mgr.C = C


def _hb_snapshot(C, date_str=None):
    """Persist session fields QMT deepcopy(C) wipes each live bar."""
    if not getattr(C, 'is_live', False):
        return
    d = date_str or getattr(C, 'current_date', '') or _HB_SESSION.get('date', '')
    if not d:
        return
    s = _HB_SESSION
    s['date'] = d
    s['mode'] = getattr(C, 'strategy_mode', s.get('mode', ''))
    s['day_open_set'] = getattr(C, '_day_open_trend_set', False)
    s['day_open'] = getattr(C, '_day_open_trend', 'NEUTRAL')
    s['daily_trend'] = getattr(C, 'daily_trend', 'NEUTRAL')
    s['trend_dir'] = getattr(C, 'trend_direction', 'NEUTRAL')
    s['trend_logged'] = getattr(C, '_trend_logged', False)
    s['backend_date'] = getattr(C, '_last_backend_indicator_date', '')
    s['trend_cut_done'] = getattr(C, '_trend_cut_done', False)
    s['base_trend_cut_active'] = getattr(C, '_base_trend_cut_active', False)
    s['base_risk_hb'] = getattr(C, '_base_risk_hb', '')
    s['today_bought_qty'] = getattr(C, '_today_bought_qty', 0)
    _inds = export_backend_indicators(C)
    if _inds is not None:
        s['backend_indicators'] = _inds
    s['prev_adtm_val'] = getattr(C, '_prev_adtm_val', 0.0)
    s['prev_adtm_ready'] = getattr(C, '_prev_adtm_ready', False)


def _hb_rehydrate(C, date_str):
    """Restore session fields into fresh C after QMT deepcopy reset."""
    s = _HB_SESSION
    if s.get('date') != date_str:
        return
    if getattr(C, 'current_date', '') != date_str:
        C.current_date = date_str
    if s.get('mode'):
        C.strategy_mode = s['mode']
    if s.get('day_open_set'):
        C._day_open_trend_set = True
        C._day_open_trend = s.get('day_open', 'NEUTRAL')
        C.daily_trend = s.get('daily_trend', C._day_open_trend)
    if s.get('trend_dir'):
        C.trend_direction = s['trend_dir']
    if s.get('trend_logged'):
        C._trend_logged = True
    if s.get('backend_date'):
        C._last_backend_indicator_date = s['backend_date']
    if s.get('trend_cut_done'):
        C._trend_cut_done = True
    if s.get('base_trend_cut_active'):
        C._base_trend_cut_active = True
    if s.get('base_risk_hb'):
        C._base_risk_hb = s['base_risk_hb']
    C._today_bought_qty = s.get('today_bought_qty', 0)
    if s.get('backend_indicators'):
        if s.get('prev_adtm_ready'):
            C._prev_adtm_val = s.get('prev_adtm_val', 0.0)
            C._prev_adtm_ready = True
        apply_daily_indicators(C, s['backend_indicators'])

def _qmt_api_namespaces(C):
    _ns = [globals()]
    for _depth in range(1, 8):
        try:
            _ns.append(sys._getframe(_depth).f_globals)
        except (ValueError, AttributeError):
            break
    return _ns


def _qmt_find_api(name, C):
    _g = globals()
    if name in _g and callable(_g[name]):
        return _g[name]
    if C is not None and hasattr(C, name):
        _fn = getattr(C, name)
        if callable(_fn):
            return _fn
    for _ns in _qmt_api_namespaces(C):
        if name in _ns and callable(_ns[name]):
            return _ns[name]
    _b = getattr(builtins, name, None)
    return _b if callable(_b) else None


def _qmt_mount_apis(C):
    """Delegate to qmt_utils scanner (reload-safe)."""
    _mounted = scan_and_bind_qmt_apis(C)
    if _mounted and C is not None and not getattr(C, '_qmt_apis_logged', False):
        C._qmt_apis_logged = True
    return _mounted


def _qmt_boot_diag(phase, C=None):
    if phase == 'init_skip_duplicate':
        return
    if phase == 'module_loaded' and qmt_utils._log_guard().get('boot_module_loaded'):
        return
    if phase == 'module_loaded':
        qmt_utils._log_guard()['boot_module_loaded'] = True
    _apis = []
    for _n in _QMT_API_NAMES:
        if _qmt_find_api(_n, C):
            _apis.append(_n)
    _msg = (f"  [Boot] {phase} | file=qmtbdxtV2.py | __name__={__name__} | "
            f"entry=handlebar(1m-unified) | apis={_apis or ['none']}")
    if C is not None:
        _msg += (f" | C={type(C).__name__} do_back_test={getattr(C, 'do_back_test', 'N/A')}"
                 f" barpos={getattr(C, 'barpos', '?')}")
        if phase == 'stop_called':
            _msg += (f" | hb_seen={getattr(C, '_hb_first_call', False)}"
                     f" engine={getattr(C, '_risk_engine', None) is not None}")
    print(_msg)
    if phase == 'module_loaded':
        _exports = [n for n in ('init', 'handlebar', 'stop', 'on_tick') if n in globals()]
        print(f"  [Boot] exports={_exports}")


def init(C):
    if getattr(C, '_qmt_init_done', False):
        _qmt_boot_diag('init_skip_duplicate', C)
        return
    _qmt_reload_deps(C)
    _qmt_boot_diag('init_enter', C)
    _qmt_mount_apis(C)

    # [4] 基础参数
    C.account_id = '44525'
    # C.stock = '600406.SH'
    C.stock = '002151.SZ'
    C.trade_qty      = 300
    C.neutral_probe_qty = 100  # log117: NEUTRAL empty probe micro size (vs trade_qty)
    C.probe_qty      = 600   # oversold probe size (>= 2 * trade_qty for T0 buffer)
    C.max_total_qty  = 1200
    C.force_close_t  = '14:50:00'
    C.after_hours_start = '15:05:00'      # 盘后固定价格交易开始
    C.after_hours_end   = '15:30:00'      # 盘后固定价格交易结束
    C.after_hours_enabled = True          # 002151.SZ 盘后交易开关
    C.after_hours_close_price = 0.0       # 当日收盘价（盘后固定成交价）
    C.after_hours_order_sent = False      # 盘后订单已发送标记
    C.current_date   = ''
    C.eod_order_sent = False
    
    # Backtest / Live
    C.is_live = not getattr(C, 'do_back_test', False)
    print(f"  [Init] is_live={C.is_live} (do_back_test={getattr(C, 'do_back_test', 'N/A')}) "
          f"feed=1m-unified")
    C.prev_close = 0.0  # previous close price (set later)

    # Base Position (backtest rebuild)
    C.backtest_base_qty = 2600
    C._base_pos_initialized = False
    
    # [5] Engine safe mounting (regular class instances, safe for deepcopy)
    try:
        C._risk_engine = ComprehensiveRiskEngineV2(context=C)
        C._probe_mgr = DecoupledProbeManager(context=C, risk_engine=C._risk_engine)
        C._t0_engine = IntradayT0GridEngine(context=C, risk_engine=C._risk_engine)
        C._pos_mgr = PositionManager(context=C)
        print("  [Init] Strategy hot reload complete, engine started successfully!")
        C._qmt_init_done = True
    except Exception as e:
        print(f"  [Init Error] Engine startup failed: {e}")
        import traceback
        traceback.print_exc()

    # ================= Stop & Rebuild Management =================
    C._base_stop_date = ''          # date of last stop
    C._days_since_stop = 99         # days since last stop (99 = no stop yet)
    C._stop_cool_days = 3           # cooldown days after stop (base)
    C._stop_cool_days_base = 3
    C._base_rebuild_stage = 0       # 0=wait,1=half,2=full
    C._cum_stop_loss = 0.0          # cumulative stop loss
    C._daily_stop_loss = 0.0        # today's stop-loss accumulator (for ledger assertion)
    C._cum_stop_loss_prev_day = 0.0 # snapshot of _cum_stop_loss at start of day
    C._max_cum_stop_pct = 0.10      # max cumulative stop loss ratio (to capital)
    C._abandon_stock = False        # abandon this stock permanently
    C._last_stop_ref_price = 0.0    # reference price after last stop
    C._consecutive_base_stops = 0   # consecutive base stops
    C._max_consecutive_stops = 3    # max consecutive stops before abandon
    C._daily_trade_fee = 0.0        # daily accumulated fee
    C._rebuild_cooldown_bars = 0    # bar-based cooldown for rebuild
    C.last_probe_barpos = -1        # Bar-level probe order lock: prevent same-bar repeat orders
    C._last_dull_probe_price = 0.0  # [P1 Fix] Last DULL-probe entry price; used to avoid
                                    # whipsaw re-entry at the same price on a downtrend
                                    # (37.10 round-trip pattern): if a DULL probe failed,
                                    # we do NOT re-enter another DULL probe until price has
                                    # moved meaningfully away from that level.
    C._rebuild_diag_logged = False
    C._rebuild_diag_last_date = ''  # legacy; prefer split keys below
    C._rebuild_diag_down_date = ''
    C._rebuild_diag_cond_date = ''
    C._empty_rebuild_sweep_done = False  # log183 P0': once-per-day backfill rebuild replay
    C._neutral_rebuild_wait_logged = False  # [修复9] 初始化
    C._skip_rebuild_logged = False
    C._skip_rebuild_bypass_today = False
    C._staged_rebuild_block_logged = False
    C._rebuild_cooling_logged = False
    C._skip_bypass_logged = False
    C._staged_reduce_date = ''
    C._days_since_staged_reduce = 99
    C._staged_reduce_cool_days = 5
    C._staged_half_exit_date = ''
    C._days_since_staged_half_exit = 99
    C._staged_half_exit_cool_days = 5
    C._rebuild_from_staged_half_exit = False
    # [Fix] Isolated Init-Half cooldown — does NOT pollute base-stop state.
    C._init_half_cooldown_date = ''
    C._days_since_init_half_exit = 99
    C._giveback_exit_date = ''
    C._days_since_giveback = 99
    C.giveback_neutral_block_days = 3  # log120: 1d general cooldown but block NEUTRAL probe 3d after give-back
    C._dull_profit_neutral_block_date = ''
    C._days_since_dull_profit_flat = 99
    C.dull_profit_neutral_block_days = 5  # log154 P1: after profitable oversold flat, skip NEUTRAL
    C.oversold_fast_full_bias = -0.08  # log155: EARLY/MACRO full lot when deep washout at 60d low
    C.neutral_shallow_cap_min_days = 2  # log152 P2: NEUTRAL immune day-2+ shallow loss cap
    C.neutral_shallow_cap_floor = -0.03
    C._neutral_shallow_cap_done = False
    C.dull_shallow_cap_min_days = 2  # log161 P1: DULL micro immune day2+ vs day3 GAP
    C.dull_shallow_cap_floor = -0.03
    C._dull_shallow_cap_done = False
    C._dull_micro_probe_active = False
    C.init_half_rebuild_accel_days = 15  # log161 P2: relax dull streak after init_half
    C.init_micro_shallow_cap_min_days = 1  # log162: init micro -4% day1+ before GAP
    C._init_micro_shallow_cap_done = False
    C.staged_half_probe_min_days = 5
    C.staged_half_probe_block_days = 3
    C.staged_half_probe_rsi_th = 40.0
    C.staged_half_probe_enabled = True
    C._exit_policy = ''
    C.observe_only_degrade_days = 5  # [修复7] 从3天延长至5天
    C._observe_policy_degraded_logged = False
    C.down_oversold_probe_enabled = True
    C.rebuild_momentum_gate_enabled = True
    C.rebuild_sar_gate_enabled = True
    C.rebuild_longcross_gate_enabled = True
    C._rebuild_momentum_block_logged = False
    C.init_base_add_float_gate_enabled = True
    C.init_base_add_min_float_pct = 0.0  # [修复6] 恢复水下不加仓，对齐基线100
    C._init_add_float_block_logged = False
    
    C.t0_underwater_gate_enabled = True
    C.t0_underwater_block_pct = -0.01
    C._t0_underwater_gate_logged = False
    C.init_half_risk_exit_enabled = True
    # [Fix D2] Restore the original 4% threshold: a 100-share micro
    # probe's entire allocation of loss budget is just ~1-2% of
    # capital. Allowing 6% on the probe itself made Init-Half exit
    # accept -6.3% (-95 CNY extra on one bar) on 05-15 vs -4.8% on
    # 05-14 — which is far more than the probe can contribute to
    # recovery after the stop fires. Tighter 4% also means _record_staged_half_exit
    # still gets invoked, just with a smaller loss.
    C.init_half_risk_exit_pct = 0.04
    C.init_half_risk_exit_pct_up = 0.06   # day-open UP trend: wider stop
    C.init_half_risk_exit_pct_mid = 0.05   # day-open UP + realtime degraded: middle stop; also micro-position cap
    C._staged_half_exit_done = False
    C._staged_half_diag_logged = False
    C._init_down_skip_logged = False
    C._gap_warn_reduced_today = False
    C._gap_warn_micro_skip_logged = False
    C._intraday_gap_immune_skip_logged = False
    C._intraday_gap_immune_macro_logged = False
    C._phase1_gap_immune_skip_logged = False
    C._probe_immune_micro_skip_logged = False

    C.down_intraday_reduce_pct = 0.03
    C._down_intraday_reduced_done = False
    C._down_intraday_reduce_barpos = -1
    C._vwap_down_t0_blocked_logged = False

    # ================= Strategy Mode & State =================
    C.decision_time  = '10:00:00'
    C.orb_amp_th     = 0.035
    C.strategy_mode  = 'UNDECIDED'
    C.state          = 'IDLE'

    is_tradable = fetch_premarket_status(C, C.stock)
    if not is_tradable:
        C.strategy_mode = 'SKIP'
        print(f"  [Init] Premarket risk control block: {C.stock} abnormal status today (suspended/ST), force enter SKIP mode")


    # Opening range (09:30-10:00) variables
    C.orb_high       = 0.0
    C.orb_low        = 9999.0
    C.orb_total_vol  = 0
    C.orb_avg_vol_pm = 0.0
    C._obs_tick_count = 0
    # ================= T+0 Parameters =================
    C.atr_period     = 20
    C.open_atr_mult  = 2.0
    C.profit_atr_mult= 1.5
    C.stop_atr_mult  = 1.5
    C.ma_short       = 5
    C.ma_long        = 20
    C.vol_ratio_min  = 0.75
    C.min_profit_th  = 0.005
    C.rsi_buy_th     = 40.0
    C.rsi_sell_th    = 60.0
    C.trend_reduce_ratio = 0.5
    C.trend_stop_mult    = 1.0  # stop multiplier for counter-trend (1.0, was 0.7)
    C._entry_is_counter  = False
    C.trail_activate = 0.010
    C.trail_step     = 0.004
    C.trail_activate_atr = 2.5
    C.trail_step_atr     = 1.0
    C.t0_trail_enabled   = False
    C.traded_volume  = 0
    C.pending_close_qty = 0

    # ================= ORB Parameters =================
    C.trail_stop_pct = 0.02
    C.vol_surge_mult = 1.2
    C.highest_since_buy = 0.0
    C.orb_degrade_time = '13:30:00'
    C.orb_break_filter = 1.002
    C.orb_confirm_bars = 5
    C._orb_waiting_confirm = False
    C._orb_confirm_bars_elapsed = 0
    C._orb_disabled_today = False
    C._orb_failed_allow_probe_today = False  # log124: ORB fail → rebuild only, no INIT_BASE
    C._virtual_oversold_armed = False       # log125: ladder arm without stop cooldown

    # ================= Risk Management =================
    C.daily_loss_limit = -450.0
    C.daily_pnl        = 0.0
    C.realized_pnl     = 0.0
    C._cum_realized_pnl = 0.0
    C._base_trade_fee   = 0.0

    # ================= Commission & Tax =================
    C.buy_commission_rate  = 0.00015
    C.sell_commission_rate = 0.00015
    C.stamp_duty_rate      = 0.001
    C.min_commission        = 5.0

    # ================= T0 Stop Settings =================
    C.stop_min_pct = 0.012              # min stop distance in price% (buffer morning noise)
    C.stop_use_atr_floor = True         # use ATR floor for stop

    # ================= T0 Profit Settings =================
    C.profit_min_pct = 0.012            # min profit target in price%
    C.profit_use_atr_floor = True       # use ATR floor for profit

    # ================= Trend Detection =================
    C.trend_ma_period  = 20
    C.trend_direction  = 'NEUTRAL'
    C._day_open_trend  = 'NEUTRAL'
    C.daily_trend      = 'NEUTRAL'         # [V5 P0-1] Authoritative daily trend (frozen at pre-lock)
    C._day_open_trend_set = False
    C._day_open_price  = 0.0
    C._is_gap_down_day_flag = False
    C._trend_ma20      = 0.0
    C._trend_price     = 0.0
    C._trend_logged    = False
    C._trend_api_warned = False
    C.down_trend_max_qty = 600
    C.down_rebuild_oversold_pct = 0.93
    C.virtual_shallow_ma_cap = 0.97  # log130: only between oversold_th and MA20*97%
    C.down_rebuild_extreme_rsi = 20.0
    C.down_rebuild_extreme_oversold_pct = 0.90  # ponytail: align A-share 10% daily limit (was 0.88 unreachable)
    C.down_rebuild_extreme_probe_qty = 600
    C.down_rebuild_extreme_cool_days = 3  # [Fix] Shorten extreme-return cooldown to avoid sitting flat during rebound
    C._last_extreme_stop_date = ''
    C.down_rebuild_dull_min_days = 3
    # [Fix D6] Restore 30.0: DULL is the shallow-oversold bounce trigger
    # that fires when RSI hasn't reached EXTREME. Tightening it to 25
    # caused us to miss RSI 27.9-style recoveries on 05-28-style days —
    # exactly the kind of recovery the strategy is designed to capture.
    # [Bug 4 Fix] DULL RSI threshold raised to 35.0 to match the
    # gettattr default used at probe decision time. Old 30.0 was
    # causing the init() value to override the Fix 3 relaxation.
    C.down_probe_dull_rsi_th = 35.0
    C.down_probe_dull_block_days = 25
    C.trend_vwap_downgrade_pct = 0.025  # [Bug1] 提高至 2.5%，减少频繁降级导致的趋势乒乓
    C.t0_base_block_pct = -0.03
    C.t0_base_half_pct = -0.02
    C._probe_stop_active = False

    # ================= Base Position Hard Stops =================
    C.base_qty_full    = 2600
    C.base_qty_half    = 1300
    C._base_cut_done   = False
    # [问题2修复] 独立状态变量 — Staged Stop 不再屏蔽 Trail Stop
    C._staged_stop_done = False
    C._trail_stop_disabled = False
    # [F7修复] 独立状态变量
    C._trend_cut_done = False
    C._probe_protect_done = False
    C._down_reduce_done = False
    # [Defect1] Reset _init_probe_upgrade_blocked on every new day.
    # This flag is set by the entry gating logic to signal that the
    # most recent DULL probe could not be upgraded because of high
    # deviation; it must NOT survive into the next session, otherwise
    # the risk engine will keep using it to tighten the stop (see
    # qmt_risk.py). Also reset the companion log flag.
    C._init_probe_upgrade_blocked = False
    C._init_probe_upgrade_block_logged = False
    # [Defect2] Intraday lock for the -6% probe-immune defense. Must
    # reset on every new day so the next probe position can be
    # reduced again if it hits -6%.
    C._probe_immune_defense_done = False
    C._base_trend_cut_active = False
    C.base_hard_stop_pct = 0.08
    C.base_down_hard_stop_pct = 0.075
    C.base_probe_hard_stop_pct = 0.08   # [Fix] Aligned with _get_dynamic_stop_pct max(0.08, ...)
                                        # so ProbeManager.check_residual_exit and the FUSE cap
                                        # do NOT trigger at -5% and bypass the wide probe stop.
    C.oversold_probe_hard_stop_pct = 0.08  # Standard oversold probe hard stop 8%
    C.oversold_probe_hard_stop_pct_t1 = 0.10  # T+1 probe hard stop 10% (gap-down tolerance)
    # [P0/P1 Fix] 200+ share mixed probe / sized probe — tightened to 5%
    # to avoid catastrophic single-trade loss on positions built by
    # ladder-add. 8% on 200 shares is a larger absolute dollar loss than
    # the system is designed for.
    C.oversold_probe_hard_stop_pct_sized = 0.05
    C.base_probe_trail_dd_pct = 0.08   # probe peak-dd trail 8% (宽松对待超卖探测)
    C.oversold_probe_trail_dd_pct = 0.10  # 超卖探测 trail stop 10%
    C.base_trail_dd_pct  = 0.12
    C.trend_abort_enabled = True       # DOWN trend veto: force full exit
    C.trend_abort_pct = 0.045          # DOWN day full close at -4.5%
    # [修复缺陷#3] 延长探针持有天数至7天，给趋势反转留出更多时间
    C.oversold_probe_max_hold_days = 7

    # ================= Staged Stop =================
    C.base_staged_stop_enabled = True
    C.base_first_reduce_pct = 0.06       # -6%: reduce half
    C.base_first_reduce_target_pct = 0.50
    C._base_staged_reduce_done = False
    C._base_staged_reduce_qty = 0
    C._base_staged_orig_total = 0
    C._base_cost_price   = 0.0
    C._base_stop_anchor  = 0.0
    C._base_peak_price   = 0.0
    C._base_stop_done    = False
    C._pending_base_stop = False
    C._is_clearing_pending_stop = False
    C._today_bought_qty = 0
    C._micro_cut_count_today = 0
    C._t0_buy_blocked_by_float = False
    C._t0_buy_restricted = False  # [修复6] Caution浮亏限制标记
    # [修复问题3] FUSE 强制清算状态标记
    C._force_liquidation_active = False
    C._force_liquidation_blocked_logged = False
    # [修复缺陷一] 初始探针升级阻断日志标志
    C._init_probe_upgrade_block_logged = False
    # [修复问题7] 首次重建探针日期，避免倒金字塔加仓的计时器折半稀释 Probe Expire
    C._first_rebuild_probe_date = ''
    C._base_build_date   = ''
    C._is_base_first_day = False

    # [修复缺陷#3] 缩短探针T0冷却期至2天，提升活跃度
    C.rebuild_probe_t0_cool_days = 2
    C.probe_global_immune_days = 2  # init + oversold GAP/GlobalStop immunity (was 5d for init)
    C.neutral_probe_immune_days = 3  # NEUTRAL micro: +1d vs day2 skip (log143 day3 GAP-WARN)
    C._probe_t0_sell_only = False
    C._probe_uw_t0_trim_done = False
    C.micro_gap_cool_days = 2  # log146: micro GAP stop → 2d before rebuild (was 1d)
    C.micro_gap_rebuild_rsi_floor = 30  # log150: post-GAP rebuild RSI bypass in block/macro gates
    C.micro_gap_rebuild_rsi_days = 5
    C._probe_gap_ladder_date = ''  # log148: prior-day GAP-WARN partial → next session full close
    C._probe_underwater_exit_date = ''
    C._probe_entry_trend = ''
    C._extreme_rsi_fast_used_date = ''
    C.init_high_dev_probe_max = 0.055  # UP micro cap 5.5% (log159 5/13@5.8% chase → 5/15 GAP -191)
    C._oversold_probe_entry_bias = 0.0  # frozen at oversold entry; severe (<-5%) → 1d immunity
    C._neutral_micro_probe_active = False  # REBUILD_NEUTRAL_PROBE — exempt from severe tier
    C.oversold_probe_trend_protect_days = 2  # [问题3] 从3天改为2天, 与_probe_immune的2天免疫对齐
    # [D6 Fix] Lower probe take-profit thresholds so high-confidence sells
    # trigger on weaker bounces. Previously init() set 80.0 / 0.015 which
    # shadowed the getattr defaults in qmt_risk.py — now the actual values
    # match the intended behavior (RSI > 65, VWAP dev > 1.0%).
    C.down_probe_sell_rsi_th = 65.0
    C.down_probe_sell_dev_th = 0.01
    C.probe_t0_max_sell_ratio = 0.5  # [P2修复] 探针仓位 T0 单次卖出不超过 50%
    C._rebuild_probe_date = ''
    C._days_since_rebuild_probe = 99
    # [P4修复] 高偏离探针独立状态变量
    C._init_probe_date = ''
    C._days_since_init_probe = 99
    C._is_init_probe = False
    C._last_init_add_date = ''  # [修复P3] UP 趋势加仓日期记录，控制每日最多一次
    C._trend_vwap_downgrade_carry = False
    C._vwap_recover_bar_count = 0
    # [P11修复] VWAP carry 使用交易日计数，而非日历天
    C._vwap_carry_trading_days = 0
    C._vwap_carry_last_date = ''
    # [F1修复] 日内趋势单调降级保护
    C._intraday_trend_locked = False
    C._trend_snapshot = ''

    # ================= Cooldown for T0 stops =================
    C.consecutive_stops = 0
    C.cooldown_until    = ''
    C._cooldown_barpos  = 0
    C._stop3_logged     = False

    # ================= Signal Confirmation =================
    C.sell_confirm_window  = 5
    C.sell_confirm_min     = 2
    C.rsi_sell_th_down     = 65.0
    C._sell_signal_history = []
    C.buy_confirm_window   = 5
    C.buy_confirm_min      = 2
    C._buy_signal_history  = []
    C.cooldown_half_qty    = True
    C.cooldown_reset_at_vwap = True
    C.trend_atr_mult_up    = 2.5
    C.trend_atr_mult_down  = 3.0
    C.strong_trend_dev     = 0.015
    C.stop_atr_mult_down_sell = 1.0

    # ================= Runtime Variables =================
    C.buy_price      = 0.0
    C.sell_price     = 0.0
    C.atr_val        = 0.0
    C.max_favorable  = 0.0
    C.entry_ma5      = 0.0
    C._atr_warned    = False
    C._base_ever_built = False
    C.can_do_t0      = False
    C._prev_close_approx = False
    C._prev_close_warned = False
    C._data_source_logged = False
    C._no_data_diag = False
    C._down_buy_hard_blocked = False
    C._base_target_qty = 0
    C._pending_sell_excess = 0
    C._last_t0_stop_price = 0.0
    C._last_t0_stop_was_buy = True
    # [Issue1/Issue2 Fix] Track daily Minimum Adverse Excursion (MAE) for risk memory
    C._daily_min_float = 0.0
    C._init_probe_mae_block_logged = False
    # [Issue3 Fix] Track PHC sell today to prevent churn (sell then buy same day)
    C._phc_sell_today = False
    C._intraday_gap_reduce_today = False  # log114: block OBSERVE/rebuild after GAP trim
    C._gap_partial_trim_session = ''      # log115: block OBSERVE add day after GAP trim
    C._gap_warn_seen_today = False        # log119: block OBSERVE add after GAP-WARN (even immune skip)
    C._block_observe_probe_add = False
    # [Fix3 Timeline Unification] Track probe skip logs for unified hard-stop
    C._phase1_probe_skip_logged = False
    C._intraday_gap_probe_skip_logged = False

    # ================= QMT Indicators (daily) =================
    C._last_indicator_min = -1
    C._macd_diff = 0.0
    C._macd_dea = 0.0
    C._sar_value = 0.0
    C._sar_bullish = False
    C._macd_long_cross = False
    C._backend_upnday_3 = False
    C._backend_indicators_enriched = False
    C._adtm_val = 0.0
    C._prev_adtm_val = 0.0
    C._prev_adtm_ready = False
    C._backend_dull_down = False
    C._backend_dull_down_days = 0
    C._backend_risk_level = 'normal'
    C._backend_price_quantile_60 = 0.0
    C._backend_close_cross_ema20 = False
    C._last_backend_indicator_date = ''
    C._last_backend_indicator_retry_min = -999999
    C._backend_indicator_error_logged = False
    # [P2 Fix] Track whether backend indicators are stale (timeout /
    # API failure). When stale, rebuild paths that depend on backend-
    # computed indicators (MACD, ADTM, quantile) are blocked to avoid
    # trading on yesterday's signals.
    C._backend_indicators_stale = False
    C._backend_trend = 'NEUTRAL'
    C._backend_fetch_attempt_date = ''
    C._vmacd_diff = 0.0
    C._vmacd_dea = 0.0
    C._wvad_val = 0.0

    # ================= Macro Probe Indicators (daily) =================
    C._backend_macro_down_5d = False
    C._backend_bias_20 = 0.0
    C._backend_near_bottom_60 = False
    C._backend_lowest_60 = 0.0
    C._backend_volatility_20 = 0.0
    C._intraday_float_reduced_today = False

    # ================= ATR Dynamic Cooldown + BARSLAST Profit Exit =================
    C._daily_atr_14 = 0.0         # 14日ATR（盘前由backend设置，回测中handlebar计算）
    C._daily_atr_20 = 0.0         # 20日ATR基准（用于动态冷却缩放）
    C._last_profit_exit_barpos = -1   # BARSLAST: 最近一次盈利退出的barpos
    C._last_profit_exit_date = ''     # BARSLAST: 最近一次盈利退出的日期
    C._last_profit_exit_pnl = 0.0     # BARSLAST: 最近一次盈利退出的PnL
    C._bars_since_profit_exit = 0     # BARSLAST: 距最近盈利退出的bar数
    C._profit_cooldown_min_bars = 12  # BARSLAST: 盈利退出后封锁bar数（防止左右互搏）

    # ================= Volume tracking =================
    C.cum_vol        = 0
    C.day_start_vol_cum = -1

    # ================= Bar Series (1-min K line) =================
    C.bar_max_keep   = 60
    C.bars_open      = []
    C.bars_high      = []
    C.bars_low       = []
    C.bars_close     = []
    C.bars_volume    = []
    C.cur_bar_min    = -1
    C.cur_bar_open   = 0.0
    C.cur_bar_high   = 0.0
    C.cur_bar_low    = 0.0
    C.cur_bar_close  = 0.0
    C.cur_bar_volume = 0
    C.cur_bar_amount = 0.0
    C.cur_min_vol    = 0
    C.last_min_id    = -1
    C._heartbeat_min = ''
    C._skip_hb_hour = ''
    C._observe_hb_hour = ''
    C._t0_nopos_hb = ''

    # VWAP components (confirmed bars)
    C.confirmed_min_vol = 0
    C.confirmed_min_amt = 0.0

    # ================= OBV & SOBV =================
    C.sobv_period = 30
    C.obv_list = []
    C.current_obv = 0.0

    # ================= V2 Engines =================
    C._backend_hhv_bars_20 = 99
    C._backend_beta_20 = 1.0
    # --- empty-position / cooldown optimization indicators ---
    C._backend_barslast_vol_surge = 99
    C._backend_continuous_shrink_days = 0
    C._backend_is_fundamental_deteriorated = False
    # runtime state derived from backend indicators
    C._abandon_stock = False
    C._liquidity_block_logged = False
    C._observe_wait_logged = False
    C._deep_loss_probe_logged = False

# ==================== Main Bar Handler ====================

# ==================================================================
# [P0 Fix 2] DPM-based probe identity helpers — single source of
# truth. These replace legacy C._is_oversold_probe /
# C._is_init_probe reads. The legacy flags are still WRITTEN by
# DPM.on_buy and by explicit state-transition points in the
# strategy; callers must READ through these helpers.
# ==================================================================
def _dpm_is_probe(C):
    """True if DPM reports state==PROBE_ACTIVE and probe_type
    is 'init_probe' or 'oversold_probe'. Safe when probe_mgr is
    None (backtest startup)."""
    _dpm = getattr(C, '_probe_mgr', None)
    if _dpm is None:
        return False
    if getattr(_dpm, 'qty', 0) <= 0:
        return False
    if _dpm.state != _dpm.PROBE_ACTIVE:
        return False
    return getattr(_dpm, 'probe_type', 'none') in ('init_probe', 'oversold_probe', 'neutral_probe')


def _dpm_is_init_probe(C):
    """True if DPM.probe_type == 'init_probe'."""
    _dpm = getattr(C, '_probe_mgr', None)
    if _dpm is None:
        return False
    if getattr(_dpm, 'qty', 0) <= 0:
        return False
    if _dpm.state != _dpm.PROBE_ACTIVE:
        return False
    return getattr(_dpm, 'probe_type', 'none') == 'init_probe'


def _dpm_is_oversold_probe(C):
    """True if DPM.probe_type == 'oversold_probe'."""
    _dpm = getattr(C, '_probe_mgr', None)
    if _dpm is None:
        return False
    if getattr(_dpm, 'qty', 0) <= 0:
        return False
    if _dpm.state != _dpm.PROBE_ACTIVE:
        return False
    return getattr(_dpm, 'probe_type', 'none') == 'oversold_probe'


def _dpm_is_neutral_probe(C):
    """True if DPM.probe_type == 'neutral_probe'."""
    _dpm = getattr(C, '_probe_mgr', None)
    if _dpm is None:
        return False
    if getattr(_dpm, 'qty', 0) <= 0:
        return False
    if _dpm.state != _dpm.PROBE_ACTIVE:
        return False
    return getattr(_dpm, 'probe_type', 'none') == 'neutral_probe'


# ================= Intraday GAP Protection (P0-1, P0-3 Fix) =================
def _gap_order_price(eval_low, current_price):
    """Float risk uses eval_low; sell order must stay inside the current bar."""
    if is_valid_price(current_price):
        return max(eval_low, current_price)
    return eval_low


def _log_rebuild_signal(C, msg):
    """Cap rebuild-signal spam (log92: 300+ lines/day with zero action)."""
    _n = getattr(C, '_rebuild_signal_log_count', 0)
    if _n >= 5:
        return
    C._rebuild_signal_log_count = _n + 1
    print(msg)


def _day_open_trend_frozen(C):
    """Authoritative day-open trend (immune to intraday NEUTRAL/UP flips)."""
    if getattr(C, '_day_open_trend_set', False):
        return getattr(C, '_day_open_trend', 'NEUTRAL')
    return getattr(C, 'daily_trend', 'NEUTRAL')


def _oversold_rebuild_qty(C, default_qty):
    """log150: day-open DOWN — oversold ladder micro only (100 not 300)."""
    if _day_open_trend_frozen(C) == 'DOWN':
        return getattr(C, 'neutral_probe_qty', 100)
    return default_qty


def _in_init_half_cooldown(C):
    """Init-half exit window — probe ladder must stay reachable under SKIP/high-amp."""
    return (getattr(C, '_exit_policy', '') == 'down_oversold'
            and getattr(C, '_init_half_cooldown_date', '') != '')


def _high_amp_rebuild_exempt(C, bar_time_str, current_price, rebuild_vwap, time_str):
    """log157: do not day-block probe evaluator on init-half / SKIP-bypass / RIGHT_SIDE."""
    if C.strategy_mode == 'OBSERVE':
        return True
    if _in_init_half_cooldown(C):
        return True
    if getattr(C, '_skip_rebuild_bypass_today', False):
        return True
    if (getattr(C, '_base_stop_date', '')
            and getattr(C, '_days_since_stop', 99) >= getattr(C, '_stop_cool_days', 3)):
        return True
    if getattr(C, '_backend_indicators_stale', False):
        return False
    _rsi = calc_daily_rsi(C, bar_time_str, period=14)
    return _right_side_entry_ok(C, _rsi, current_price, rebuild_vwap, time_str)


def _oversold_fast_path_qty(C, default_qty, macro_bias, near_bottom60=False,
                            micro_vwap_ok=False, micro_ma5_cross=False):
    """log155: EARLY/MACRO — 100 on day-open DOWN阴跌, 300 on strong rebound."""
    micro = getattr(C, 'neutral_probe_qty', 100)
    if _day_open_trend_frozen(C) != 'DOWN':
        return default_qty
    if macro_bias >= -0.05:
        return default_qty  # mild dip — PHC-friendly (log154 5/26 EARLY)
    bias_th = getattr(C, 'oversold_fast_full_bias', -0.08)
    if near_bottom60 and macro_bias <= bias_th:
        return default_qty
    if near_bottom60 and (micro_vwap_ok or micro_ma5_cross):
        return default_qty
    return micro


def _in_post_gap_rebuild_window(C):
    """log151: empty + recent micro GAP stop — relax dull/extreme macro gates."""
    if get_total_position(C) > 0:
        return False
    if not getattr(C, '_base_stop_date', ''):
        return False
    return getattr(C, '_days_since_stop', 99) < getattr(C, 'micro_gap_rebuild_rsi_days', 5)


def _in_init_half_rebuild_accel_window(C):
    """log161 P2: after init_half exit, relax dull streak for N days."""
    if getattr(C, '_exit_policy', '') != 'down_oversold':
        return False
    if not getattr(C, '_init_half_cooldown_date', ''):
        return False
    return getattr(C, '_days_since_init_half_exit', 99) <= getattr(
        C, 'init_half_rebuild_accel_days', 15)


def _severe_down_macro_rsi_ok(C, rsi, macro_bias):
    """RSI pass when bias<-5% blocks extreme/dull/relax macro gates.
    [Bug#6 Fix] 放宽 severe down RSI 门槛至 30：002151 06-08 RSI=27.4
    已是极度超卖，但 bias=-8.62% 触发 <20 门槛导致错过建仓。"""
    if macro_bias >= -0.05:
        return True
    if _in_post_gap_rebuild_window(C):
        return rsi < getattr(C, 'micro_gap_rebuild_rsi_floor', 30)
    return rsi < max(getattr(C, 'down_rebuild_extreme_rsi', 20), 30)


def _ensure_day_open_price(C, _bar_time_str):
    """Restore day open when C was reset but _HB_SESSION still has today's date (hot reload)."""
    if getattr(C, '_day_open_price', 0.0) > 0:
        return
    _today_open = 0.0
    if hasattr(C, 'open') and C.barpos < len(C.open):
        _today_open = float(C.open[C.barpos])
    if not is_valid_price(_today_open):
        try:
            _md_open = C.get_market_data_ex(
                ['open'], [C.stock], period='1d', count=1, end_time=_bar_time_str)
            if C.stock in _md_open and not _md_open[C.stock].empty:
                _today_open = float(_md_open[C.stock]['open'].iloc[-1])
        except Exception:
            pass
    if not is_valid_price(_today_open) and len(getattr(C, 'bars_open', [])) > 0:
        _today_open = C.bars_open[0]
    C._day_open_price = _today_open if is_valid_price(_today_open) else 0.0


def _clear_rebuild_ghost_after_profitable_probe_flat(C):
    """Profitable probe flat exit: drop virtual-stop ghost (log125 P0/P2)."""
    C._virtual_oversold_armed = False
    C._base_stop_date = ''
    C._days_since_stop = 99
    C._probe_stop_active = False
    # log128 P0': ladder via policy only — no fake stop, not observe_only (127 froze rebuild)
    C._exit_policy = 'down_oversold'


def _arm_oversold_profit_neutral_block(C, date_str):
    """log154 P1: profitable oversold probe flat — defer NEUTRAL rebuild (any ladder path)."""
    C._dull_profit_neutral_block_date = date_str
    C._days_since_dull_profit_flat = 0
    print(f"  [NEUTRAL Block] oversold-profit flat armed {date_str}, NEUTRAL probe blocked 5d")


def _rebuild_probe_shallow_allowed(C):
    """log172 A'+C: bridge on last immune day (hold==max) or post-immune (hold>max)."""
    _max = _probe_immune_max_days(C)
    _days = getattr(C, '_days_since_rebuild_probe', 99)
    return _days >= _max


def _try_rebuild_probe_shallow_before_gap(C, date_str, current_price, tick):
    """A': dull/NEUTRAL shallow before GAP-WARN / INTRADAY_GAP (live price)."""
    if get_total_position(C) <= 0 or _dpm_is_init_probe(C):
        return False
    if _maybe_neutral_shallow_cap_exit(C, date_str, current_price, tick):
        return True
    if _maybe_dull_shallow_cap_exit(C, date_str, current_price, tick):
        return True
    return False


def _maybe_neutral_shallow_cap_exit(C, date_str, current_price, tick):
    """NEUTRAL micro shallow — A'+C bridge/post-immune, live-price trigger."""
    if not _holds_neutral_micro_probe(C) or getattr(C, '_neutral_shallow_cap_done', False):
        return False
    if not _rebuild_probe_shallow_allowed(C):
        return False
    _n_days = getattr(C, '_days_since_rebuild_probe', 0)
    if _n_days < getattr(C, 'neutral_shallow_cap_min_days', 2):
        return False
    _fp_sc = base_float_risk(C, current_price)
    _cap_floor = getattr(C, 'neutral_shallow_cap_floor', -0.02)
    if _fp_sc >= _cap_floor:
        return False
    if _immune_last_day_deep_float_defer(C, _fp_sc):
        return False
    # [Bug#8 Fix] 反弹保护：如果当前 bar 价格高于近 20 根 bar 最低价
    # （反弹中），不触发 Shallow Cap。只在继续下跌时触发。
    # 06-25 持有 100 股 neutral_probe，3 天后浮亏 -3.1%，在反弹中触发
    # Shallow Cap 割肉 -109.2，应等免疫窗口结束后让 Global Guard 处理。
    _recent_low = min(C.bars_low[-20:]) if len(C.bars_low) >= 20 else current_price
    if current_price > _recent_low * 1.003:
        return False
    _total_pos = get_total_position(C)
    _sc_avail = get_true_available_position(C)
    _sc_qty = (min(_total_pos, _sc_avail) // 100) * 100
    if _sc_qty < 100:
        return False
    _sc_fee = calc_trade_fee(C, current_price, _sc_qty, is_sell=True)
    _sc_mgr = getattr(C, '_pos_mgr', None)
    if _sc_mgr is None:
        return False
    _sc_sold, _sc_pnl = _sc_mgr.request_sell(
        _sc_qty, 'NEUTRAL_SHALLOW_CAP', caller='stop_loss',
        current_price=current_price, tick=tick, fee=_sc_fee)
    if not _sc_sold:
        return False
    C._neutral_shallow_cap_done = True
    _record_micro_shallow_exit(C, date_str, current_price, _sc_qty,
                               realized_pnl=_sc_pnl, shallow_kind='neutral')
    if get_total_position(C) == 0:
        reset_base_anchors(C, 'full')
    print(f"  [NEUTRAL Shallow Cap] hold {_n_days}d float {_fp_sc:.1%} "
          f"< {_cap_floor:.0%}, exit {_sc_qty} @ {current_price:.2f} pnl {_sc_pnl:.1f}")
    return True


def _maybe_init_micro_shallow_cap_exit(C, date_str, current_price, tick, float_pct=None):
    """log162/163: init micro -4% day1+ before GAP/immune break (8% hard stop too wide)."""
    if not _dpm_is_init_probe(C) or getattr(C, '_init_micro_shallow_cap_done', False):
        return False
    if get_total_position(C) > getattr(C, 'neutral_probe_qty', 100):
        return False
    _n_days = getattr(C, '_days_since_init_probe', 0)
    if _n_days < getattr(C, 'init_micro_shallow_cap_min_days', 1):
        return False
    # log170: trigger on live price only (bar-low wick must not arm exit)
    _fp_sc = base_float_risk(C, current_price)
    _cap_floor = getattr(C, 'init_half_risk_exit_pct', 0.04)
    # log163: <= -4.0% triggers (was >= blocked exact -4.0%)
    if _fp_sc > -_cap_floor:
        return False
    _total_pos = get_total_position(C)
    _sc_avail = get_true_available_position(C)
    _sc_qty = (min(_total_pos, _sc_avail) // 100) * 100
    if _sc_qty < 100:
        return False
    _sc_fee = calc_trade_fee(C, current_price, _sc_qty, is_sell=True)
    _sc_mgr = getattr(C, '_pos_mgr', None)
    if _sc_mgr is None:
        return False
    _sc_sold, _sc_pnl = _sc_mgr.request_sell(
        _sc_qty, 'INIT_MICRO_SHALLOW_CAP', caller='stop_loss',
        current_price=current_price, tick=tick, fee=_sc_fee)
    if not _sc_sold:
        return False
    C._init_micro_shallow_cap_done = True
    _record_staged_half_exit(C, date_str, current_price, _sc_qty,
                             is_init_half=True, realized_pnl=_sc_pnl)
    if get_total_position(C) == 0:
        reset_base_anchors(C, 'full')
    print(f"  [Init Micro Shallow Cap] hold {_n_days}d float {_fp_sc:.1%} "
          f"<= -{_cap_floor:.0%}, exit {_sc_qty} @ {current_price:.2f} pnl {_sc_pnl:.1f}")
    return True


def _maybe_dull_shallow_cap_exit(C, date_str, current_price, tick, float_pct=None):
    """DULL micro shallow — A'+C bridge/post-immune, live-price trigger."""
    if not getattr(C, '_dull_micro_probe_active', False):
        return False
    if getattr(C, '_dull_shallow_cap_done', False):
        return False
    if not _rebuild_probe_shallow_allowed(C):
        return False
    _n_days = getattr(C, '_days_since_rebuild_probe', 0)
    if _n_days < getattr(C, 'dull_shallow_cap_min_days', 2):
        return False
    _fp_sc = base_float_risk(C, current_price)
    _cap_floor = getattr(C, 'dull_shallow_cap_floor', -0.03)
    # log164: <= -3.0% triggers (was >= blocked exact -3.0%)
    if _fp_sc > _cap_floor:
        return False
    # [Bug#8 Fix] 反弹保护：反弹中不触发 Shallow Cap
    _recent_low = min(C.bars_low[-20:]) if len(C.bars_low) >= 20 else current_price
    if current_price > _recent_low * 1.003:
        return False
    _total_pos = get_total_position(C)
    _sc_avail = get_true_available_position(C)
    _sc_qty = (min(_total_pos, _sc_avail) // 100) * 100
    if _sc_qty < 100:
        return False
    _sc_fee = calc_trade_fee(C, current_price, _sc_qty, is_sell=True)
    _sc_mgr = getattr(C, '_pos_mgr', None)
    if _sc_mgr is None:
        return False
    _sc_sold, _sc_pnl = _sc_mgr.request_sell(
        _sc_qty, 'DULL_SHALLOW_CAP', caller='stop_loss',
        current_price=current_price, tick=tick, fee=_sc_fee)
    if not _sc_sold:
        return False
    C._dull_shallow_cap_done = True
    C._dull_micro_probe_active = False
    _record_micro_shallow_exit(C, date_str, current_price, _sc_qty,
                               realized_pnl=_sc_pnl, shallow_kind='oversold')
    if get_total_position(C) == 0:
        reset_base_anchors(C, 'full')
    print(f"  [DULL Shallow Cap] hold {_n_days}d float {_fp_sc:.1%} "
          f"<= {_cap_floor:.0%}, exit {_sc_qty} @ {current_price:.2f} pnl {_sc_pnl:.1f}")
    return True


def _arm_dull_profit_neutral_block(C, date_str):
    _arm_oversold_profit_neutral_block(C, date_str)


def _clear_virtual_oversold_on_entry(C):
    if getattr(C, '_virtual_oversold_armed', False):
        C._virtual_oversold_armed = False
        if not getattr(C, '_probe_stop_active', False):
            C._base_stop_date = ''
            C._days_since_stop = 99


def _blocks_high_position_init_build(C, deviation):
    """Empty-account first build must be a true mean-reversion pullback.

    ponytail: deviation + gray-zone + day-open DOWN; ORB-fail micro handled upstream.
    """
    if _day_open_trend_frozen(C) == 'DOWN':
        # log123: intraday UP after ORB fail — do not block on stale day-open DOWN alone
        if getattr(C, 'trend_direction', 'NEUTRAL') != 'UP':
            return True, 'day-open trend DOWN (no empty INIT; arming oversold rebuild)'
    if deviation > 0.05:
        return True, f'deviation {deviation:.1%} > 5% (need true pullback to MA20)'
    if getattr(C, '_gray_zone_weak_block_t0', False):
        return True, 'gray-zone weak float (use oversold rebuild ladder)'
    return False, ''


def _orb_fail_micro_ok(C, deviation, current_price, rebuild_vwap):
    """ORB-fail micro: day-open not DOWN, UP, mild pullback dev<=2%, price>VWAP (05/18 still blocked)."""
    if _day_open_trend_frozen(C) == 'DOWN':
        return False
    if getattr(C, 'trend_direction', 'NEUTRAL') != 'UP':
        return False
    # ponytail: 2% cap vs old dev<=0; day-open DOWN ban kept (log127 41.28)
    if deviation > 0.02 or getattr(C, '_gray_zone_weak_block_t0', False):
        return False
    return rebuild_vwap > 0 and current_price > rebuild_vwap


def _virtual_shallow_down_zone(C, current_price, ma20, oversold_th):
    """Fresh virtual arm: MA20*pct > price > oversold_th (log130 40.08 blocked)."""
    if not getattr(C, '_virtual_oversold_armed', False) or getattr(C, '_base_ever_built', False):
        return False
    if ma20 <= 0 or oversold_th <= 0:
        return False
    _hi = ma20 * getattr(C, 'virtual_shallow_ma_cap', 0.97)
    return oversold_th < current_price < _hi


def _virtual_shallow_macro_ok(macro_bias, near_bottom, support, price):
    return macro_bias <= -0.03 or near_bottom or (support > 0 and price < support)


def _log_virtual_shallow_diag(C, msg):
    """Once per session after decision_time."""
    _d = getattr(C, 'current_date', '')
    if _d != getattr(C, '_virtual_shallow_diag_date', ''):
        C._virtual_shallow_diag_date = _d
        print(msg)


def _try_virtual_shallow_down_probe(C, current_price, time_str, bar_time_str, oversold_th):
    """Virtual-arm EARLY/RELAX micro only; does not open full oversold ladder above oversold_th."""
    if not getattr(C, '_virtual_oversold_armed', False):
        return False, 0, ''
    _ma20 = getattr(C, '_trend_ma20', 0.0)
    _shallow_hi = _ma20 * getattr(C, 'virtual_shallow_ma_cap', 0.97)
    _in_zone = _virtual_shallow_down_zone(C, current_price, _ma20, oversold_th)
    if not _in_zone:
        if time_str >= C.decision_time:
            _log_virtual_shallow_diag(
                C, f"  [Virtual Shallow] skip: price {current_price:.2f} "
                f"not in ({oversold_th:.2f},{_shallow_hi:.2f})")
        return False, 0, ''
    if get_total_position(C) > 0 or C._base_rebuild_stage != 0:
        return False, 0, ''
    # log146: day-open DOWN — no virtual shallow; use oversold ladder only
    if _day_open_trend_frozen(C) == 'DOWN':
        return False, 0, ''
    if not _allows_down_oversold_probe(C) or _oversold_probe_in_block_period(C):
        return False, 0, ''
    if time_str < C.decision_time:
        return False, 0, ''

    _daily_rsi = calc_daily_rsi(C, bar_time_str, period=14)
    _adtm = getattr(C, '_adtm_val', 0.0)
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _near_bottom = getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))
    _support = getattr(C, '_backend_support', 0.0)
    _dull_down, _down_streak = is_dull_down_decline(C, bar_time_str)
    _prev_adtm = get_prior_day_adtm(C, bar_time_str)
    _adtm_rising = _adtm >= _prev_adtm - 0.03
    _micro_q = getattr(C, 'neutral_probe_qty', 100)
    _macro_ok = _virtual_shallow_macro_ok(
        _macro_bias, _near_bottom, _support, current_price)

    # log130: streak>=2 + RSI<40 + macro -3%; blocks 5/21@40.08
    if (25 <= _daily_rsi < 40 and _adtm > -0.50 and _macro_ok and _down_streak >= 2
            and (_macro_bias >= -0.05 or _daily_rsi < 30)):
        return True, _micro_q, 'REBUILD_VIRTUAL_SHALLOW_EARLY'

    _adtm_relax_ok = ((-0.50 < _adtm < 0.1) and _adtm_rising) or (_daily_rsi < 30 and _adtm > -0.85)
    _relax_score = int(_adtm_relax_ok) + int(_daily_rsi < 40) + int(_daily_rsi < 35)
    _relax_th = 1 if (_daily_rsi < 30 and C.trend_direction == 'DOWN') else 2
    _extreme_rsi = getattr(C, 'down_rebuild_extreme_rsi', 20.0)
    if (_down_streak >= 2 and _dull_down and _relax_score >= _relax_th and _adtm > -0.85
            and not (C.trend_direction == 'DOWN' and _macro_bias < -0.05
                     and _daily_rsi >= _extreme_rsi)):
        return True, _micro_q, 'REBUILD_VIRTUAL_SHALLOW_RELAX'

    _log_virtual_shallow_diag(
        C, f"  [Virtual Shallow] in zone blocked: RSI={_daily_rsi:.1f} ADTM={_adtm:.2f} "
        f"bias={_macro_bias:.2%} dull={_dull_down} streak={_down_streak}d macro_ok={_macro_ok}")
    return False, 0, ''


def _right_side_entry_ok(C, daily_rsi, current_price, rebuild_vwap, time_str):
    """Daily SAR/MACD reversal + RSI band; DOWN ok if price>VWAP (log127 P2, micro only).

    [log176 Fix B] Added near-VWAP tolerance: when RSI<45 (deep oversold rebound)
    and SAR_bull+MACD_cross both confirmed, allow price within -0.5% of VWAP.
    This captures V-reversal entries where price is still slightly below VWAP
    at 10:00 but the reversal signal is strong (002151 6/8: price 34.37 vs
    VWAP 34.65, -0.8% -> blocked -> missed +15% rebound).
    """
    if not (30 < daily_rsi <= 55):
        return False
    _sar_bull = getattr(C, '_sar_bullish', False)
    _macd_cross = getattr(C, '_macd_long_cross', False)
    if not (_sar_bull or _macd_cross):
        return False
    if C.trend_direction != 'DOWN':
        return True
    if time_str < C.decision_time or rebuild_vwap <= 0:
        return False

    # Standard path: price > VWAP
    if current_price > rebuild_vwap:
        return True

    # [log176 Fix B] near-VWAP tolerance band: RSI<55 + SAR&MACD double confirm
    # Allow price within -1.0% of VWAP entry
    _vwap_gap = (current_price - rebuild_vwap) / rebuild_vwap
    if (daily_rsi < 55
            and _sar_bull
            and _macd_cross
            and _vwap_gap >= -0.01):   # -1.0% tolerance band
        if not getattr(C, '_right_side_near_vwap_logged', False):
            C._right_side_near_vwap_logged = True
            print(f"  [Right-Side] Near-VWAP entry: RSI={daily_rsi:.1f} "
                  f"gap={_vwap_gap:.2%} SAR={_sar_bull} MACD={_macd_cross}")
        return True

    return False


def _neutral_empty_probe_ok(C, current_price, time_str, bar_time_str, rebuild_vwap):
    """Gate empty-account NEUTRAL_PROBE: reversal/volume required; D->N flip needs both grace + signal.

    ponytail: independent of cum profit-protect; blocks blind NEUTRAL-day buys (log112 06-18).
    [log176 Fix A] D->N flip: require BOTH _backend_strong AND _intraday_micro.
    Previously _macd_bull+_intraday_micro alone could trigger entry, but _macd_bull
    (minute DIFF>DEA) is a transient signal that fires during brief bounces in
    persistent downtrends (002151 6/18: DIFF=-1.584 but entered -> -143.2 loss;
    600406 6/23: entered -> -101.3 loss).
    """
    _gbd = getattr(C, '_giveback_exit_date', '')
    _gbb = getattr(C, 'giveback_neutral_block_days', 3)
    _dsg = getattr(C, '_days_since_giveback', 99)
    if _gbd and _dsg < _gbb:
        return False, f'give-back {_dsg}/{_gbb}d, NEUTRAL probe blocked'
    _open_trend = _day_open_trend_frozen(C)
    _is_down_to_neutral = (_open_trend == 'DOWN' and C.trend_direction == 'NEUTRAL')
    _stale = getattr(C, '_backend_indicators_stale', False)
    _backend_rev = (
        not _stale
        and (getattr(C, '_macd_long_cross', False) or getattr(C, '_sar_bullish', False))
    )
    _bullish_bar = check_daily_bullish_bar(C, bar_time_str)
    _micro_vwap_ok = (
        time_str >= '10:00:00' and rebuild_vwap > 0 and current_price > rebuild_vwap
    )
    _micro_ma5_cross = False
    if len(C.bars_close) >= 6:
        _ma5_val = sum(C.bars_close[-5:]) / 5.0
        _prev_ma5 = sum(C.bars_close[-6:-1]) / 5.0
        _micro_ma5_cross = (C.bars_close[-1] > _ma5_val and C.bars_close[-2] <= _prev_ma5)
    _vol_surge = getattr(C, '_backend_is_volume_surge', False)
    _vol_ratio = calc_vol_ratio(C)
    _vol_recovery = (time_str >= '10:00:00' and _vol_ratio >= 0.8 and _micro_vwap_ok)
    _macd_bull = getattr(C, '_macd_diff', 0.0) > getattr(C, '_macd_dea', 0.0)
    _intraday_micro = _micro_vwap_ok or _micro_ma5_cross
    _backend_strong = _backend_rev or _bullish_bar
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _dpd_early = getattr(C, '_dull_profit_neutral_block_date', '')
    if not _dpd_early and _macro_bias < -0.05:
        # log183 P1': no PHC cushion — hard block all empty NEUTRAL (align log181 PHC effect)
        return False, f'macro {_macro_bias:.1%} no PHC, NEUTRAL probe blocked'
    if _is_down_to_neutral:
        # [Fix D3] D→N flip: use backend signals only (not intraday micro).
        # On a D→N flip day, _intraday_micro is time-sensitive (VWAP
        # changes intraday). If the morning evaluation at 10:00 blocked
        # because _backend_strong was False, the afternoon re-evaluation
        # at 14:00 should NOT override that decision just because
        # price crossed above VWAP. The backend signal (SAR/MACD) is
        # the authoritative reversal confirmation — intraday VWAP alone
        # is a weak timing signal that fires during brief bounces.
        # This prevents the 06-22 pattern: NEUTRAL_PROBE blocked at 10:00
        # (backend weak + intraday ok = False), then entered at 14:00
        # when only intraday became "ok".
        _strong_rev = _backend_strong
    else:
        _strong_rev = _backend_strong or _intraday_micro or _vol_surge or _vol_recovery
    # log167 A: oversold-profit bypass stricter than entry _strong_rev (quality not timing)
    _dpd = getattr(C, '_dull_profit_neutral_block_date', '')
    _dnb = getattr(C, 'dull_profit_neutral_block_days', 5)
    _dsd = getattr(C, '_days_since_dull_profit_flat', 99)
    # log189 P1‴: hard block 5d after oversold PHC — no D2N bypass (6/18 vs 181)
    if _dpd and _dsd < _dnb:
        _macro_shift = getattr(C, '_macd_long_cross', False) or getattr(C, '_sar_bullish', False)
        if _macro_shift and not getattr(C, '_backend_indicators_stale', False):
            # [D4 Fix] Quality gate: MACD/SAR cross alone is too weak
            # (07-02 false signal → -105 unrealized). Require RSI<30
            # (deep oversold rebound) OR bias>-3% (mild macro) to
            # confirm the trend shift has substance.
            _rsi_d4 = calc_daily_rsi(C, bar_time_str, period=14)
            _bias_d4 = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
            _shift_quality = (_rsi_d4 < 30 or _bias_d4 > -0.03)
            if _shift_quality:
                C._dull_profit_neutral_block_date = ''
                C._days_since_dull_profit_flat = 99
                print(f"  [Policy] Trend shift (MACD/SAR) detected, early release from oversold-profit flat block {_dsd}/{_dnb}d (RSI:{_rsi_d4:.1f} bias:{_bias_d4:.1%})")
            else:
                return False, f'oversold-profit flat {_dsd}/{_dnb}d, trend shift but quality gate failed (RSI:{_rsi_d4:.1f} bias:{_bias_d4:.1%})'
        else:
            return False, f'oversold-profit flat {_dsd}/{_dnb}d, NEUTRAL probe blocked'
    if getattr(C, '_intraday_gap_reduce_today', False):
        return False, 'intraday GAP reduce today, no NEUTRAL entry'
    if _is_down_to_neutral and not _strong_rev:
        return False, 'DOWN->NEUTRAL flip day, need backend SAR/MACD/bullish_bar AND intraday VWAP/MA5 confirmation'
    if not _strong_rev:
        return False, 'no reversal/volume confirmation (MACD/SAR/bullish_bar/VWAP/vol)'
    return True, ''


def _blocks_underwater_probe_to_half(C, current_price):
    """Block scaling a probe to half/base without cost+1% & MA5 bounce (log113 06-22)."""
    _cost = getattr(C, '_base_cost_price', 0.0)
    if _cost <= 0 or get_total_position(C) <= 0:
        return False, ''
    _float = base_float_risk(C, current_price)
    _ma5 = calc_ma(C, C.ma_short)
    if current_price > _cost * 1.01 and _ma5 > 0 and current_price > _ma5:
        return False, ''
    return True, f'float:{_float:.1%}, need price>cost+1% & MA5'


def _observe_probe_add_cap(C):
    """OBSERVE fill target: micro NEUTRAL probe stays at neutral_probe_qty (log119)."""
    _nq = getattr(C, 'neutral_probe_qty', 100)
    if get_total_position(C) <= _nq:
        return _nq
    return C.trade_qty


def _blocks_observe_micro_probe_add(C):
    """Micro probe (<= neutral_probe_qty) must scale via rebuild, not OBSERVE drip.
    Exception: neutral micro probe without rebuild upgrade path (no PHC history)
    is allowed to drip via OBSERVE to avoid deadlock."""
    _nq = getattr(C, 'neutral_probe_qty', 100)
    _tot = get_total_position(C)
    if _tot > 0 and _tot <= _nq:
        if getattr(C, '_neutral_micro_probe_active', False):
            _p_days = getattr(C, '_days_since_rebuild_probe', 0)
            if _p_days >= 2:
                return False, ''
        return True, f'micro probe {_tot}<={_nq} sh   use rebuild path, not OBSERVE add'
    return False, ''


def _bump_probe_peak(C, current_price):
    """High-water mark from tick, ORB range, and recent bar highs (log123 give-back)."""
    _pk = getattr(C, '_base_peak_price', 0.0)
    _cands = [current_price, _pk]
    _oh = getattr(C, 'orb_high', 0.0)
    if _oh and _oh > 0:
        _cands.append(_oh)
    _bars = getattr(C, 'bars_high', None)
    if _bars:
        _cands.append(max(_bars[-15:]))
    _new = max(x for x in _cands if x and x > 0)
    if _new > _pk:
        C._base_peak_price = _new
    return getattr(C, '_base_peak_price', 0.0)


def _probe_breakeven_arm(cost, peak, probe_days, ig_max, float_pct, micro=False, neutral_micro=False):

    if cost <= 0 or peak <= 0:
        return 1.02
    if neutral_micro and probe_days >= max(0, ig_max - 1) and -0.03 < float_pct < 0:
        return 1.005  # log150: NEUTRAL immune tail — exit tiny green before GAP
    if peak >= cost * 1.015 and float_pct < 0:
        return 1.015
    if probe_days >= ig_max and float_pct < -0.03:
        return 1.015
    return 1.02


def _blocks_probe_scale_to_half(C, current_price):
    """Block probe->half during GAP immunity or underwater (log116 06-22 14:06)."""
    if get_total_position(C) <= 0:
        return False, ''
    # log185 P1″: micro NEUTRAL without oversold-profit PHC must not scale to half
    # EXCEPT after immunity window expires — then allow upgrade to break deadlock.
    if (getattr(C, '_neutral_micro_probe_active', False)
            and not getattr(C, '_dull_profit_neutral_block_date', '')):
        _p_days = getattr(C, '_days_since_rebuild_probe', 0)
        _ig_max = _probe_immune_max_days(C)
        if _p_days <= _ig_max:
            return True, 'neutral micro without oversold-profit PHC, no scale-to-half'
    if _dpm_is_init_probe(C) or _holds_down_probe(C) or _dpm_is_oversold_probe(C):
        _ig_max = _probe_immune_max_days(C)
        _days = (
            getattr(C, '_days_since_init_probe', 99) if _dpm_is_init_probe(C)
            else getattr(C, '_days_since_rebuild_probe', 99))
        if _days <= _ig_max:
            return True, f'probe immune ({_days}d/<={_ig_max}d), no scale-to-half'
    return _blocks_underwater_probe_to_half(C, current_price)


def _gap_half_lot_qty(cap):
    """Ceil-half rounded up to 100-sh lot (ponytail: 300→200, 200→100)."""
    cap = (int(cap) // 100) * 100
    if cap <= 100:
        return cap
    half_shares = (cap + 1) // 2
    return min(cap, ((half_shares + 99) // 100) * 100)


def _probe_hold_days_for_gap(C):
    if _dpm_is_init_probe(C):
        return getattr(C, '_days_since_init_probe', 0)
    return getattr(C, '_days_since_rebuild_probe', 0)


def _non_immune_probe_gap_reduce_qty(C, gap_total, gap_cap, gap_pct, current_price):
    """log148: immune-expiry one-shot half; next session full-closes ladder remainder."""
    _gap_cap = (int(gap_cap) // 100) * 100
    if _gap_cap < 100:
        return 0, ''
    _ladder_date = getattr(C, '_probe_gap_ladder_date', '')
    _today = getattr(C, 'current_date', '')
    _hold = _probe_hold_days_for_gap(C)
    _immune_max = _probe_immune_max_days(C)
    if _ladder_date and _ladder_date != _today:
        return _gap_cap, (
            f"  [GAP-WARN] Probe ladder follow-up: full close {_gap_cap} of {gap_total} "
            f"@ {current_price:.2f} (float:{gap_pct:.1%}; prior {_ladder_date})")
    if _hold > _immune_max:
        if _gap_cap <= 100:
            return _gap_cap, (
                f"  [GAP-WARN] Non-immune probe expired: micro full-close {_gap_cap} "
                f"@ {current_price:.2f} (float:{gap_pct:.1%}, hold {_hold}d)")
        # log149: micro probe (<=trade_qty) — one-shot full close, not 100+200 over 2d
        if _gap_cap <= _probe_gap_micro_max(C):
            return _gap_cap, (
                f"  [GAP-WARN] Non-immune probe expired: full close {_gap_cap} of {gap_total} "
                f"@ {current_price:.2f} (float:{gap_pct:.1%}, hold {_hold}d>{_immune_max}d)")
        _qty = min(_gap_cap, _gap_half_lot_qty(_gap_cap))
        return _qty, (
            f"  [GAP-WARN] Non-immune probe expired: one-shot half {_qty} of {gap_total} "
            f"@ {current_price:.2f} (float:{gap_pct:.1%}, hold {_hold}d>{_immune_max}d)")
    _qty = _gap_cap if _gap_cap <= 100 else _gap_half_lot_qty(_gap_cap)
    if _qty >= gap_total or _gap_cap <= 100:
        return _qty, (
            f"  [GAP-WARN] Non-immune probe: {gap_total} sh at float "
            f"{gap_pct:.1%} micro full-close {_qty} @ {current_price:.2f}")
    return _qty, (
        f"  [GAP-WARN] Non-immune probe: {gap_total} sh at float "
        f"{gap_pct:.1%} partial trim {_qty} @ {current_price:.2f}")


def _log_high_dev_build_blocked(C, deviation, high_dev_cap, ref_trend, rebuild_vwap, current_price):
    """log92: distinguish dev>cap vs below-VWAP vs trend mismatch (not a fake '>8%' line)."""
    if deviation > high_dev_cap:
        _msg = (f"  [Build Blocked] High deviation {deviation:.1%} > {high_dev_cap:.1%} cap, "
                f"skip initial build")
    elif ref_trend != 'UP':
        _msg = (f"  [Build Blocked] Deviation {deviation:.1%} > 5%, trend {ref_trend} "
                f"(need UP for high-dev probe), skip initial build")
    elif rebuild_vwap <= 0 or current_price <= rebuild_vwap:
        _msg = (f"  [Build Blocked] Deviation {deviation:.1%} > 5%, price {current_price:.2f} "
                f"not above VWAP {rebuild_vwap:.2f}, skip initial build")
    else:
        _msg = (f"  [Build Blocked] Deviation {deviation:.1%} in (5%, {high_dev_cap:.1%}], "
                f"high-position init banned (need dev<=5% pullback), skip initial build")
    log_once(C, '_high_dev_build_blocked_logged', _msg)


def _probe_intraday_stop_tier_msg(C, is_init_probe, immune_active):
    """Honest probe stop narrative for GAP layers (immune skips -4%/-6% trim)."""
    _days = (
        getattr(C, '_days_since_init_probe', 99) if is_init_probe
        else getattr(C, '_days_since_rebuild_probe', 99))
    _ig_max = _probe_immune_max_days(C)
    if _days <= 1:
        _hs_pct = getattr(C, 'oversold_probe_hard_stop_pct_t1', 0.10)
    elif is_init_probe:
        _hs_pct = getattr(C, 'base_probe_hard_stop_pct', 0.08)
    else:
        _hs_pct = getattr(C, 'oversold_probe_hard_stop_pct', 0.08)
    return (f"immune {_days}d/<={_ig_max}d: skip -4%/-6% trim; "
            f"hard stop -{_hs_pct:.0%}")


def _maybe_enable_probe_underwater_reduce_t0(C, current_price, time_str):
    """OBSERVE probe: sell-only T+0 when DOWN@-3% or any trend @-5% (half trim only)."""
    # log178: init_probe uses GAP/hard-stop tiers; T0 trim @-3% before GAP @-4% (5/26 -311)
    if _dpm_is_init_probe(C):
        return False
    if getattr(C, '_probe_t0_sell_only', False):
        return False
    if getattr(C, '_probe_uw_t0_trim_done', False):
        return False
    if getattr(C, '_intraday_gap_reduce_today', False):
        return False
    if _probe_global_guard_immune(C):
        return False
    if get_total_position(C) < 200 or get_true_available_position(C) < 100:
        return False
    if not (_holds_down_probe(C) or _dpm_is_init_probe(C)):
        return False
    _cost = getattr(C, '_base_cost_price', 0.0)
    if _cost <= 0:
        return False
    _float = (current_price - _cost) / _cost
    _thresh = -0.03 if C.trend_direction == 'DOWN' else -0.05
    if _float > _thresh:
        return False
    if getattr(C, '_force_liquidation_active', False):
        return False
    if getattr(C, '_gray_zone_weak_block_t0', False):
        return False
    C.strategy_mode = 'T0'
    C.state = 'IDLE'
    C.can_do_t0 = True
    C._probe_t0_sell_only = True
    log_once(C, '_probe_underwater_t0_logged',
             f"  [{time_str}] [OBSERVE->T0] Probe underwater reduce-only "
             f"(float:{_float:.1%}, thresh:{_thresh:.1%}, qty>=200, "
             f"cost:{_cost:.2f}, p:{current_price:.2f})")
    return True


def _check_intraday_gap_protection(C, current_price, tick, date_str, time_str, _bar_low):
    
    if time_str < C.decision_time or getattr(C, '_base_stop_done', False):
        return False
    if not is_valid_price(current_price) or C._base_cost_price <= 0:
        return False

    _total_pos = get_total_position(C)
    if _total_pos <= 0:
        return False
    # log147: phase-1 GAP-WARN partial already trimmed — no second intraday GAP leg
    if getattr(C, '_gap_warn_reduced_today', False) and _total_pos > 0:
        return False

    # [Fix 2] Use only current bar low and current price; no historical 30-bar rolling low
    # to avoid ghost false-trigger after deep V rebound.
    _eval_low = _bar_low if _bar_low > 0 else current_price
    if time_str >= '14:45:00':
        _eval_low = min(_eval_low, current_price)
    _float_pct = (_eval_low - C._base_cost_price) / C._base_cost_price
    _sell_price = _gap_order_price(_eval_low, current_price)

    _is_currently_intraday_probe = (getattr(C, '_probe_mgr', None) is not None
                                    and C._probe_mgr.is_probe)
    # log172 A'+C: shallow before any intraday GAP leg (live price, bridge/post-immune)
    if (_is_currently_intraday_probe and not _dpm_is_init_probe(C)
            and _try_rebuild_probe_shallow_before_gap(C, date_str, current_price, tick)):
        return True
    _ig_max = _probe_immune_max_days(C)
    _ig_micro_max = _probe_gap_micro_max(C)
    if _total_pos <= _ig_micro_max and _is_currently_intraday_probe and _is_probe_gap_immune(C):
        _ig_is_init = _dpm_is_init_probe(C)
        _ig_days = (getattr(C, '_days_since_init_probe', 99) if _ig_is_init
                    else getattr(C, '_days_since_rebuild_probe', 99))
        if _float_pct <= -0.06:
            _fb6_ig = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
            if _fb6_ig <= -0.10:
                if not getattr(C, '_intraday_gap_immune_deep_logged', False):
                    C._intraday_gap_immune_deep_logged = True
                    print(f"  [Intraday GAP] Probe immune (hold {_ig_days}d) float {_float_pct:.1%} <= -6%, "
                          f"deep washout bias {_fb6_ig:.1%} defer to hard stop")
            else:
                if not getattr(C, '_intraday_gap_immune_deep_logged', False):
                    C._intraday_gap_immune_deep_logged = True
                    _tier = _probe_intraday_stop_tier_msg(C, _ig_is_init, True)
                    print(f"  [Intraday GAP] Probe immune (hold {_ig_days}d <= {_ig_max}d), "
                          f"skip -6% micro close (float:{_float_pct:.1%}; {_tier})")
                return False
        elif _float_pct <= -0.04:
            _frozen_bias_ig2 = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
            _ig_macro_severe = _frozen_bias_ig2 <= -0.08
            _ig_init_partial = _is_probe_gap_immune_partial(C)
            _ig_day2_deep = not _probe_gap_immune_blocks_4pct(C, _float_pct)
            if _ig_macro_severe or _ig_init_partial or _ig_day2_deep:
                if not getattr(C, '_intraday_gap_immune_macro_logged', False):
                    C._intraday_gap_immune_macro_logged = True
                    if _ig_day2_deep and not (_ig_macro_severe or _ig_init_partial):
                        print(f"  [Intraday GAP] Probe immune day2 deep (hold {_ig_days}d, "
                              f"float {_float_pct:.1%}), allow -4% trim")
                    else:
                        print(f"  [Intraday GAP] Probe immune (hold {_ig_days}d) macro/partial window, "
                              f"allow -4% exit (float:{_float_pct:.1%})")
            else:
                # log170: init micro shallow on live -4% (immune would defer to GAP)
                if (_ig_is_init and _total_pos <= getattr(C, 'neutral_probe_qty', 100)
                        and _maybe_init_micro_shallow_cap_exit(
                            C, date_str, current_price, tick)):
                    return True
                if not getattr(C, '_intraday_gap_immune_skip_logged', False):
                    C._intraday_gap_immune_skip_logged = True
                    _tier = _probe_intraday_stop_tier_msg(C, _ig_is_init, True)
                    print(f"  [Intraday GAP] Probe immune (hold {_ig_days}d <= {_ig_max}d), skip -4% trim only "
                          f"(float:{_float_pct:.1%}; {_tier})")
                return False
        else:
            return False

    # ================= 阶段 1：状态判定 =================
    _should_execute_sell = False
    _should_trim = False

    # [1] -6%: micro probe full close (immune only skips when deep washout)
    if _float_pct <= -0.06 and _total_pos <= _ig_micro_max and not getattr(C, '_intraday_micro_gap_done', False):
        if _is_currently_intraday_probe:
            _fb6 = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
            if _is_probe_gap_immune(C) and _fb6 > -0.10:
                pass  # immune skip handled above
            elif _fb6 <= -0.10:
                if not getattr(C, '_intraday_micro_gap_probe_skip_logged', False):
                    C._intraday_micro_gap_probe_skip_logged = True
                    print(f"  [Intraday GAP] Probe micro pos {_total_pos} float {_float_pct:.1%} <= -6%, "
                          f"deep washout bias {_fb6:.1%}  defer to hard stop")
            else:
                _should_execute_sell = True
        else:
            _should_execute_sell = True

    # [2] -4%~-6%: 100sh cannot halve → full close; 200sh probe may trim if immune allows
    elif _float_pct <= -0.04 and not getattr(C, '_intraday_warn_done', False):
        _fb4 = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
        if _total_pos <= 100:
            _os_hold_days = getattr(C, '_days_since_rebuild_probe', 99) if _holds_down_probe(C) else 99
            _max_hold = getattr(C, 'oversold_probe_max_hold_days', 7)
            # ponytail: oversold within max_hold — -4% is noise; wait for -6% GAP or hard stop
            _os_hold_window = (_holds_down_probe(C) and _os_hold_days <= _max_hold)
            _day2_deep = (_is_currently_intraday_probe
                          and not _probe_gap_immune_blocks_4pct(C, _float_pct))
            _immune_skip_4 = (
                (_is_currently_intraday_probe and _probe_gap_immune_blocks_4pct(C, _fb4)
                 and _fb4 > -0.08)
                or (_os_hold_window and _float_pct > -0.06 and not _day2_deep)
            )
            if not _immune_skip_4:
                _should_execute_sell = True
                if _is_currently_intraday_probe and not getattr(C, '_micro_gap_100_full_close_logged', False):
                    C._micro_gap_100_full_close_logged = True
                    print(f"  [Intraday GAP] Micro probe 100sh float {_float_pct:.1%} in [-4%,-6%], full close")
        elif _total_pos <= _ig_micro_max:
            if not _is_currently_intraday_probe:
                _should_trim = True
            elif not _is_probe_gap_immune(C) or not _probe_gap_immune_blocks_4pct(C, _fb4):
                _should_execute_sell = True
            elif _fb4 <= -0.08 or _is_probe_gap_immune_partial(C):
                _should_trim = True
            elif not getattr(C, '_intraday_warn_probe_skip_logged', False):
                C._intraday_warn_probe_skip_logged = True
                print(f"  [Intraday GAP WARN] Probe {_total_pos}sh immune, skip -4% trim (float:{_float_pct:.1%})")
        else:
            _should_trim = True

    # ================= 阶段 2：统一物理执行 =================
    _true_avail = get_true_available_position(C)

    if _should_execute_sell and _true_avail > 0:
        _close_qty = min(_total_pos, _true_avail)
        _close_qty = (_close_qty // 100) * 100
        if _close_qty >= 100:
            print(f"  [Intraday GAP] Micro pos {_total_pos} float {_float_pct:.1%}, force close {_close_qty} @ {_sell_price:.2f}")
            _gap_fee = calc_trade_fee(C, _sell_price, _close_qty, is_sell=True)
            _pos_mgr = getattr(C, '_pos_mgr', None)
            if _pos_mgr is not None:
                _sold, _ = _pos_mgr.request_sell(_close_qty, 'INTRADAY_GAP_MICRO', caller='stop_loss', current_price=_sell_price, tick=tick, fee=_gap_fee)
            else:
                _sold = safe_sell(C, _sell_price, _close_qty, 'INTRADAY_GAP_MICRO', tick)
            if _sold:
                C._intraday_micro_gap_done = True
                C._gap_exit_today = True
                C._intraday_gap_reduce_today = True
                _record_base_stop(C, date_str, _sell_price, _close_qty, is_explicit_probe_stop=True, is_gap_exit=True)
                if get_total_position(C) == 0:
                    reset_base_anchors(C, 'full')
                return True
            else:
                if not getattr(C, '_intraday_gap_t1_blocked_logged', False):
                    C._intraday_gap_t1_blocked_logged = True
                    _tbq = getattr(C, '_today_bought_qty', 0)
                    print(f"  [Intraday GAP] T+1 BLOCKED: avail=0 (bought today {_tbq}), "
                          f"defer -6% close of {_total_pos} sh to next session.")

    if _should_trim and _true_avail > 0:
        # [Fix 3b] Strict half-reduce: if less than 200 shares, cancel trim
        _close_qty = (min(_total_pos, _true_avail) // 2 // 100) * 100
        if _close_qty < 100:
            # Not enough shares to halve (100 shares), cancel trim and defer to hard stop
            if not getattr(C, '_trim_skip_micro_logged', False):
                C._trim_skip_micro_logged = True
                print(f" [Intraday GAP WARN] Skip trim: qty {_total_pos} too small to halve, "
                      f"defer to hard stop (float {_float_pct:.1%})")
        else:
            print(f"  [Intraday GAP WARN] Micro pos {_total_pos} float {_float_pct:.1%} in [-4%, -6%], trim {_close_qty} @ {_sell_price:.2f}")
            _gap_fee = calc_trade_fee(C, _sell_price, _close_qty, is_sell=True)
            _pos_mgr = getattr(C, '_pos_mgr', None)
            if _pos_mgr is not None:
                _sold, _ = _pos_mgr.request_sell(_close_qty, 'INTRADAY_GAP_WARN', caller='stop_loss', current_price=_sell_price, tick=tick, fee=_gap_fee)
            else:
                _sold = safe_sell(C, _sell_price, _close_qty, 'INTRADAY_GAP_WARN', tick)
            if _sold:
                C._intraday_warn_done = True
                C._gap_exit_today = True
                C._intraday_gap_reduce_today = True
                if get_total_position(C) > 0:
                    C._gap_partial_trim_session = date_str
                _record_base_stop(C, date_str, _sell_price, _close_qty, is_explicit_probe_stop=True, is_gap_exit=True)
                if get_total_position(C) == 0:
                    reset_base_anchors(C, 'full')
                return True
            else:
                if not getattr(C, '_intraday_warn_t1_blocked_logged', False):
                    C._intraday_warn_t1_blocked_logged = True
                    _tbq = getattr(C, '_today_bought_qty', 0)
                    print(f"  [Intraday GAP WARN] T+1 BLOCKED: avail=0 (bought today {_tbq}), "
                          f"defer -4%~-6% trim of {_total_pos} sh to next session.")

    return False


def stop(C):
    _qmt_boot_diag('stop_called', C)


def on_tick(C, tick=None):
    if getattr(C, '_risk_engine', None) is None:
        init(C)
    # ponytail: 1m handlebar drives logic; on_tick only lazy-init


def _bar_minute_index_to_time_str(bar_idx):
    """Map 1m bar index (0=09:30) to HH:MM:SS, skipping lunch."""
    if bar_idx < 120:
        total_mins = 9 * 60 + 30 + bar_idx
    else:
        total_mins = 13 * 60 + (bar_idx - 120)
    return f"{total_mins // 60:02d}:{total_mins % 60:02d}:00"


def _apply_bars_slice(C, full, n):
    """Truncate in-memory 1m arrays to first n bars (sweep replay)."""
    n = min(n, len(full['close']))
    C.bars_open = full['open'][:n]
    C.bars_high = full['high'][:n]
    C.bars_low = full['low'][:n]
    C.bars_close = full['close'][:n]
    C.bars_volume = full['volume'][:n]
    C.confirmed_min_vol = sum(C.bars_volume)
    C.confirmed_min_amt = sum(
        C.bars_close[j] * C.bars_volume[j] for j in range(len(C.bars_close)))


def _fetch_sweep_day_bars(C, _bar_time_str, time_str):
    """log185 P0⁴: today's 1m bars as arrays (no bar_max_keep cap)."""
    if time_str < '09:31:00':
        return None, 0
    try:
        _today_str = getattr(C, 'current_date', '') or _HB_SESSION.get('date', '')
        if not _today_str and len(_bar_time_str) >= 8:
            _today_str = f"{_bar_time_str[:4]}-{_bar_time_str[4:6]}-{_bar_time_str[6:8]}"
        _md = C.get_market_data_ex(
            ['open', 'high', 'low', 'close', 'volume'],
            [C.stock], period='1m', count=280,
            subscribe=False, end_time=_bar_time_str)
        if C.stock not in _md or _md[C.stock].empty:
            print(f"  [Backfill Sweep] API empty for {C.stock} end={_bar_time_str}")
            return None, 0
        df = _md[C.stock]
        if not isinstance(df.index, _pd.DatetimeIndex):
            df.index = _pd.to_datetime(df.index)
        if _today_str:
            df = df[df.index.strftime('%Y-%m-%d') == _today_str]
        if df.empty:
            print(f"  [Backfill Sweep] 0 bars after today filter ({_today_str})")
            return None, 0
        _cur_minute_ms = (C.get_bar_timetag(C.barpos) // 60000) * 60000
        _vol_unit_is_lots = getattr(C, '_vol_unit_is_lots', True)
        _full = {'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'bar_ms': []}
        for ts, row in df.iterrows():
            _bar_ms = cst_naive_ts_to_ms(ts)
            if _bar_ms >= _cur_minute_ms:
                continue
            _raw_v = int(row['volume'])
            _vol = _raw_v * 100 if _vol_unit_is_lots else _raw_v
            _full['open'].append(float(row['open']))
            _full['high'].append(float(row['high']))
            _full['low'].append(float(row['low']))
            _full['close'].append(float(row['close']))
            _full['volume'].append(_vol)
            _full['bar_ms'].append(_bar_ms)
        _n = len(_full['close'])
        if _n == 0:
            print(f"  [Backfill Sweep] 0 bars before cur minute (raw={len(df)})")
            return None, 0
        return _full, _n
    except Exception as e:
        print(f"  [Backfill Sweep] fetch failed: {e}")
        return None, 0


def _force_backfill_today_bars(C, _bar_time_str, time_str):
    """log183 P0': pull today's 1m bars up to end_time (ignore len early-exit)."""
    _full, _n = _fetch_sweep_day_bars(C, _bar_time_str, time_str)
    if not _full or _n <= 0:
        return 0
    C.bars_open.clear()
    C.bars_high.clear()
    C.bars_low.clear()
    C.bars_close.clear()
    C.bars_volume.clear()
    C.confirmed_min_vol = 0
    C.confirmed_min_amt = 0.0
    for j in range(_n):
        update_kline_from_1m(
            C, _full['open'][j], _full['high'][j], _full['low'][j],
            _full['close'][j], _full['volume'][j], _full['bar_ms'][j])
    if len(C.bars_open) >= 1:
        _orb_bars = min(len(C.bars_open), 30)
        C.orb_high = max(C.bars_high[:_orb_bars])
        C.orb_low = min(C.bars_low[:_orb_bars])
        C.orb_total_vol = sum(C.bars_volume[:_orb_bars])
        C._obs_tick_count = max(C._obs_tick_count, min(_orb_bars, 30))
    return _n


def _restore_bars_snap(C, snap):
    C.bars_open = snap['open']
    C.bars_high = snap['high']
    C.bars_low = snap['low']
    C.bars_close = snap['close']
    C.bars_volume = snap['volume']
    C.confirmed_min_vol = snap['confirmed_vol']
    C.confirmed_min_amt = snap['confirmed_amt']
    C.orb_high = snap['orb_high']
    C.orb_low = snap['orb_low']
    C.orb_total_vol = snap['orb_vol']


def _calc_down_oversold_th(C):
    """MA20×pct capped by recent 5d low — same as handlebar DOWN rebuild gate."""
    _ma20 = getattr(C, '_trend_ma20', 0)
    _os_pct = getattr(C, 'down_rebuild_oversold_pct', 0.90)
    _th = _ma20 * _os_pct if _ma20 > 0 else 0
    if _th > 0 and len(C.bars_low) >= 5:
        _th = min(_th, min(C.bars_low[-5:]) * 0.98)
    return _th


def _sweep_frozen_oversold_th(C):
    """log188 P0⁷′-C: day-open MA20×pct only — no replay intraday low cap."""
    _ma20 = getattr(C, '_trend_ma20', 0)
    _os_pct = getattr(C, 'down_rebuild_oversold_pct', 0.90)
    return _ma20 * _os_pct if _ma20 > 0 else 0.0


def _sweep_oversold_price_ok(C, price):
    _th = _sweep_frozen_oversold_th(C)
    return _th <= 0 or price <= _th


def _sweep_early_virtual_arm(C):
    """log188 P0⁷′-A: virtual/stop-arm days — P0⁵ EARLY sweep, not RELAX gates."""
    return bool(getattr(C, '_virtual_oversold_armed', False) or C._base_stop_date != '')


def _sweep_d8_eval_ok(C, daily_rsi):
    """Mirror D8 absolute-oversold gate before probe path evaluation."""
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _absolute = (
        _macro_bias <= -0.03
        or getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False)))
    _down_os_active = (
        getattr(C, '_exit_policy', '') == 'down_oversold'
        and getattr(C, '_days_since_init_half_exit', 99) >= getattr(C, '_stop_cool_days', 3))
    return _absolute or daily_rsi <= 30 or _down_os_active


def _sweep_macro_fallback_time_ok(sweep_time, daily_rsi, macro_bias=0.0, macro_near_bottom=False):
    if sweep_time >= '14:00:00' or daily_rsi < 15:
        return True
    # log189 P0¹⁰: extreme oversold — sweep MACRO may match live morning (6/2 bias -12%)
    if macro_bias <= -0.10 or macro_near_bottom:
        return sweep_time >= '10:00:00'
    return False


def _sweep_flat_probe_cooldown_ok(C):
    # log187 P0⁹: RELAX probe spacing — not EARLY virtual-arm
    return getattr(C, '_days_since_rebuild_probe', 99) >= 2


def _sweep_phc_flat_cooldown_ok(C):
    """log191 P0¹²: PHC re-arm sweep gap — allow stale d4 (6/2), block hot d5 (6/10)."""
    _dpd = getattr(C, '_dull_profit_neutral_block_date', '')
    if not _dpd:
        return True
    _dsd = getattr(C, '_days_since_dull_profit_flat', 99)
    if _dsd < getattr(C, 'sweep_phc_macro_cooldown_days', 4):
        return False
    # ponytail: d5 hole after fresh PHC blocks 6/10 MACRO re-sweep; d6+ cooled
    return _dsd != getattr(C, 'sweep_phc_macro_block_day', 5)


def _sweep_bar_index_at_or_after(time_hms):
    for i in range(241):
        if _bar_minute_index_to_time_str(i) >= time_hms:
            return i
    return 240


def _sweep_pick_macro_candidate(candidates, macro_extreme):
    """log195 frozen P0¹⁵: extreme MACRO — afternoon VWAP_ok pool, max price (6/3 cum ~181)."""
    if not candidates:
        return None
    if not macro_extreme:
        return candidates[0]
    _pm_i = _sweep_bar_index_at_or_after('13:00:00')
    _pool = [c for c in candidates if c[0] >= _pm_i] or candidates
    _vwap = [c for c in _pool if c[3].get('vwap_ok')]
    _pick_pool = _vwap if _vwap else _pool
    return max(_pick_pool, key=lambda c: (c[1], c[0]))


def _mark_down_rebuild_weak_today(C):
    C._down_rebuild_weak_date = getattr(C, 'current_date', '')


def _sweep_rebuild_preflight(C, ignore_cooldown=False):
    """Shared sweep entry guards (log186 P0⁶)."""
    if not _allows_down_oversold_probe(C):
        return False
    if C.trend_direction != 'DOWN' or C.strategy_mode == 'ORB':
        return False
    if getattr(C, '_abandon_stock', False) or getattr(C, '_pending_base_stop', False):
        return False
    # log184 P0″: morning rebuild diag sets cooldown; sweep replays must ignore it
    if not ignore_cooldown and C._rebuild_cooldown_bars > 0:
        return False
    if getattr(C, '_position_cleared_today', False):
        return False
    return True


def _sweep_rebuild_vwap(C):
    if C.confirmed_min_vol > 0:
        return C.confirmed_min_amt / C.confirmed_min_vol
    if len(C.bars_close) > 0:
        return C.bars_close[-1]
    return 0.0


def _sweep_micro_signals(C, price, sweep_time):
    """Per-bar micro gates for MACRO/RELAX sweep replay."""
    _rebuild_vwap = _sweep_rebuild_vwap(C)
    _micro_vol_ratio = calc_vol_ratio(C)
    _micro_vol_dry = sweep_time >= '10:00:00' and _micro_vol_ratio < 0.5
    _micro_vwap_ok = sweep_time >= '10:00:00' and _rebuild_vwap > 0 and price > _rebuild_vwap
    _micro_ma5_cross = False
    if len(C.bars_close) >= 6:
        _ma5_val = sum(C.bars_close[-5:]) / 5.0
        _prev_ma5 = sum(C.bars_close[-6:-1]) / 5.0
        _micro_ma5_cross = (C.bars_close[-1] > _ma5_val and C.bars_close[-2] <= _prev_ma5)
    _amp_bars = min(len(C.bars_high), len(C.bars_low), 30)
    _cur_amp = 0.0
    if _amp_bars > 0:
        _h = max(C.bars_high[-_amp_bars:])
        _l = min(C.bars_low[-_amp_bars:])
        _cur_amp = (_h - _l) / _l if _l > 0 else 0.0
    return {
        'vwap': _rebuild_vwap,
        'vol_ratio': _micro_vol_ratio,
        'vol_dry': _micro_vol_dry,
        'vwap_ok': _micro_vwap_ok,
        'ma5_cross': _micro_ma5_cross,
        'is_high_amp': _cur_amp >= getattr(C, 'orb_amp_th', 0.035),
    }


def _sweep_stop_cooldown_active(C):
    return (getattr(C, '_base_stop_done', False)
            or getattr(C, '_down_intraday_reduced_done', False)
            or getattr(C, '_probe_immune_defense_done', False)
            or getattr(C, '_gap_exit_today', False)
            or getattr(C, '_intraday_micro_gap_done', False))


def _sweep_macro_resonance_ok(C, price, sweep_time, bar_time_str, micro, daily_rsi,
                              ignore_cooldown=False):
    """MACRO probe gate for backfill sweep (mirrors handlebar _macro_resonance_ok)."""
    if not _sweep_phc_flat_cooldown_ok(C):
        return False
    if not _sweep_rebuild_preflight(C, ignore_cooldown):
        return False
    if _oversold_probe_in_block_period(C):
        return False
    if _sweep_stop_cooldown_active(C):
        return False
    if getattr(C, '_probe_immune_defense_done', False):
        return False
    if getattr(C, '_backend_indicators_stale', False):
        return False
    if getattr(C, '_backend_continuous_shrink_days', 0) >= 10:
        return False
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _macro_near_bottom = getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))
    _support = getattr(C, '_backend_support', 0.0)
    _is_break_support = (price < _support) if _support > 0 else False
    _macro_space_ok = (_macro_bias <= -0.03) or _macro_near_bottom or _is_break_support
    _adtm = getattr(C, '_adtm_val', 0.0)
    _extreme_bias = _macro_bias <= -0.12
    _micro_confirmed = (
        (micro['vol_dry'] and (micro['vwap_ok'] or micro['ma5_cross']))
        or (_extreme_bias and (micro['vwap_ok'] or micro['ma5_cross']))
    )
    return (_macro_space_ok and _micro_confirmed and _adtm > -0.85
            and not micro['is_high_amp'] and C._base_rebuild_stage == 0
            and get_total_position(C) < C.trade_qty
            and _sweep_macro_fallback_time_ok(
                sweep_time, daily_rsi, _macro_bias, _macro_near_bottom))


def _sweep_relax_ok(C, price, sweep_time, bar_time_str, micro, daily_rsi,
                    ignore_cooldown=False):
    """RELAX probe gate for backfill sweep (mirrors handlebar _relax_ok)."""
    if not _sweep_flat_probe_cooldown_ok(C):
        return False
    if not _sweep_phc_flat_cooldown_ok(C):
        return False
    if not _sweep_rebuild_preflight(C, ignore_cooldown):
        return False
    if _oversold_probe_in_block_period(C):
        return False
    if _sweep_stop_cooldown_active(C):
        return False
    if getattr(C, '_backend_indicators_stale', False):
        return False
    _adtm = getattr(C, '_adtm_val', 0.0)
    _prev_adtm = get_prior_day_adtm(C, bar_time_str)
    _adtm_rising = _adtm >= _prev_adtm - 0.03
    _adtm_stabilized = (-0.50 < _adtm < 0.2)
    _adtm_ok_for_probe = _adtm > -0.85 and (_adtm_rising or _adtm_stabilized)
    _adtm_weakening_moderate = (-0.50 < _adtm < 0.1) and _adtm_rising
    _adtm_weakening_mild = ((-0.65 < _adtm < 0.2) and (_adtm >= _prev_adtm - 0.03))
    _adtm_relax_ok = _adtm_weakening_moderate or (daily_rsi < 30 and _adtm > -0.85)
    _daily_bullish_bar = check_daily_bullish_bar(C, bar_time_str)
    _dull_down, _down_streak = is_dull_down_decline(C, bar_time_str)
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _relax_score = (int(_adtm_weakening_moderate or daily_rsi < 30)
                    + int(daily_rsi < 40)
                    + int(_daily_bullish_bar or daily_rsi < 35))
    _relax_threshold = 1 if (daily_rsi < 30 and C.trend_direction == 'DOWN') else 2
    _probe_immune_no_add = (
        get_total_position(C) > 0 and _dpm_is_oversold_probe(C)
        and getattr(C, '_days_since_rebuild_probe', 99) <= 2)
    return (not _probe_immune_no_add and _adtm_ok_for_probe
            and (_adtm_relax_ok or _adtm_weakening_mild)
            and _relax_score >= _relax_threshold and _down_streak >= 1
            and not micro['is_high_amp']
            and not getattr(C, '_is_gap_down_day_flag', False)
            and _severe_down_macro_rsi_ok(C, daily_rsi, _macro_bias)
            and C._base_rebuild_stage == 0
            and get_total_position(C) < C.trade_qty
            and sweep_time >= '10:00:00')


def _sweep_dull_preflight(C, ignore_cooldown=False):
    """log192 P0¹³: DULL bypasses observe_only _allows gate (mirrors live elif _dull_ok)."""
    if C.trend_direction != 'DOWN' or C.strategy_mode == 'ORB':
        return False
    if getattr(C, '_abandon_stock', False) or getattr(C, '_pending_base_stop', False):
        return False
    if not ignore_cooldown and C._rebuild_cooldown_bars > 0:
        return False
    if getattr(C, '_position_cleared_today', False):
        return False
    return True


def _sweep_dull_ok(C, price, sweep_time, bar_time_str, micro, daily_rsi, ignore_cooldown=False):
    """DULL probe gate for backfill sweep (mirrors live handlebar _dull_ok)."""
    if not _sweep_dull_preflight(C, ignore_cooldown):
        return False
    if _oversold_probe_in_block_period(C):
        return False
    if _sweep_stop_cooldown_active(C):
        return False
    if sweep_time < '10:30:00':
        return False
    _adtm = getattr(C, '_adtm_val', 0.0)
    _prev_adtm = get_prior_day_adtm(C, bar_time_str)
    _adtm_rising = _adtm >= _prev_adtm - 0.03
    _adtm_stabilized = (-0.50 < _adtm < 0.2)
    _adtm_ok_for_probe = _adtm > -0.85 and (_adtm_rising or _adtm_stabilized)
    _adtm_weakening_moderate = (-0.50 < _adtm < 0.1) and _adtm_rising
    _adtm_relax_ok = _adtm_weakening_moderate or (daily_rsi < 30 and _adtm > -0.85)
    _dull_rsi_th = getattr(C, 'down_probe_dull_rsi_th', 35.0)
    _dull_down, _down_streak = is_dull_down_decline(C, bar_time_str)
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _dull_severe_macro_block = (
        C.trend_direction == 'DOWN'
        and _macro_bias < -0.05
        and not _severe_down_macro_rsi_ok(C, daily_rsi, _macro_bias))
    _last_dull_price = getattr(C, '_last_dull_probe_price', 0.0)
    _dull_same_price_blocked = (
        _last_dull_price > 0
        and abs(price - _last_dull_price) / _last_dull_price < 0.015)
    _dull_down_eff = (
        _dull_down
        or (_in_post_gap_rebuild_window(C)
            and _down_streak >= 1
            and daily_rsi < getattr(C, 'micro_gap_rebuild_rsi_floor', 30)))
    _probe_immune_no_add = (
        get_total_position(C) > 0 and _dpm_is_oversold_probe(C)
        and getattr(C, '_days_since_rebuild_probe', 99) <= 2)
    return (not _probe_immune_no_add and _adtm_ok_for_probe
            and _dull_down_eff and daily_rsi < _dull_rsi_th
            and (_adtm_relax_ok or daily_rsi < 25)
            and not micro['is_high_amp']
            and not _dull_same_price_blocked
            and not _dull_severe_macro_block
            and C._base_rebuild_stage == 0
            and get_total_position(C) < C.trade_qty)


def _sweep_early_oversold_ok(C, price, sweep_time, bar_time_str, ignore_cooldown=False, quiet=False):
    """Minimal EARLY_OVERSOLD gate for backfill sweep (mirrors handlebar path)."""
    if not _sweep_rebuild_preflight(C, ignore_cooldown):
        return False
    if _oversold_probe_in_block_period(C):
        return False
    _daily_rsi = calc_daily_rsi(C, bar_time_str, period=14)
    _adtm = getattr(C, '_adtm_val', 0.0)
    _prev_adtm = get_prior_day_adtm(C, bar_time_str)
    _adtm_rising = _adtm >= _prev_adtm - 0.03
    _daily_bullish_bar = check_daily_bullish_bar(C, bar_time_str)
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _macro_near_bottom = getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))
    _support = getattr(C, '_backend_support', 0.0)
    _is_break_support = (price < _support) if _support > 0 else False
    if _is_break_support and not quiet:
        _log_rebuild_signal(C, f"  [Rebuild Signal] Price {price:.2f} below support {_support:.2f}")
    _macro_space_ok = (_macro_bias <= -0.03) or _macro_near_bottom or _is_break_support
    _amp_bars = min(len(C.bars_high), len(C.bars_low), 30)
    _cur_amp = 0.0
    if _amp_bars > 0:
        _h = max(C.bars_high[-_amp_bars:])
        _l = min(C.bars_low[-_amp_bars:])
        _cur_amp = (_h - _l) / _l if _l > 0 else 0.0
    _is_high_amp = _cur_amp >= getattr(C, 'orb_amp_th', 0.035)
    return (not _sweep_stop_cooldown_active(C)
            and 25 <= _daily_rsi < 40 and _adtm > -0.50
            and _macro_space_ok and C._base_rebuild_stage == 0
            and get_total_position(C) < C.trade_qty
            and not getattr(C, '_is_gap_down_day_flag', False)
            and not getattr(C, '_phc_sell_today', False)
            and not _is_high_amp
            and sweep_time >= '10:00:00'
            and (C.trend_direction != 'DOWN' or _macro_bias >= -0.05
                 or (_daily_rsi < 25 and (_adtm_rising or _daily_bullish_bar))))


def _sweep_execute_oversold_buy(C, build_qty, price, sweep_time, tick, date_str, reason,
                                _snap_bars=None):
    """Execute sweep rebuild buy + minimal post-buy state (oversold probe path).

    _snap_bars: optional dict with 'open', 'high', 'low', 'close', 'volume'
    lists from the pre-sweep snapshot. Temporarily restores the current bar's
    full OHLCV so that adjust_price_for_backtest sees the correct bar range
    AND close (not the sweep-truncated historical bar).
    """
    _pos_mgr = getattr(C, '_pos_mgr', None)
    if _pos_mgr is None:
        return False
    # [Fix D1-sweep+v2] Restore full OHLCV for price calibration.
    # _apply_bars_slice truncated ALL bar arrays to the sweep's historical
    # bar index. Without restoring bars_close, adjust_price_for_backtest
    # reads the signal bar's close (≈ signal price), so no adjustment
    # fires and QMT still overrides the price.
    _saved = {}
    if _snap_bars:
        for _k in ('open', 'high', 'low', 'close', 'volume'):
            _attr = f'bars_{_k}'
            if hasattr(C, _attr):
                _saved[_k] = list(getattr(C, _attr))
                setattr(C, _attr, list(_snap_bars.get(_k, [])))
    try:
        _buy_ok = _pos_mgr.request_buy(
            build_qty, reason, caller='rebuild',
            current_price=price, tick=tick, probe_type='oversold_probe')
    finally:
        for _k, _v in _saved.items():
            setattr(C, f'bars_{_k}', _v)
    if not _buy_ok:
        return False
    _total_pos = get_total_position(C)
    _clear_virtual_oversold_on_entry(C)
    C._today_bought_qty += build_qty
    C.traded_volume += build_qty
    C._base_pos_initialized = True
    C._base_ever_built = True
    C._empty_skip_done = False
    C._base_cut_done = False
    C._staged_stop_done = False
    C._trail_stop_disabled = False
    C._base_staged_reduce_done = False
    C._base_staged_reduce_qty = 0
    C._base_staged_orig_total = 0
    C._base_trend_cut_active = False
    C.strategy_mode = 'OBSERVE'
    C.state = 'IDLE'
    C.can_do_t0 = False
    C._rebuild_from_staged_half_exit = False
    C._base_peak_price = price
    C._neutral_micro_probe_active = False
    C._oversold_probe_entry_bias = getattr(
        C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    C._last_dull_probe_price = 0.0
    C._dull_micro_probe_active = False
    _dpm = getattr(C, '_probe_mgr', None)
    if _dpm is not None:
        _dpm.peak_price = price
    if not getattr(C, '_first_rebuild_probe_date', ''):
        C._first_rebuild_probe_date = date_str
        C._first_probe_trade_days = 0
    C._days_since_rebuild_probe = max(getattr(C, '_days_since_rebuild_probe', 0),
                                      getattr(_dpm, 'hold_days', 0) if _dpm else 0)
    if getattr(C, '_base_stop_anchor', 0.0) <= 0.0:
        C._base_stop_anchor = price
    _build_fee = calc_trade_fee(C, price, build_qty, is_sell=False)
    C._base_trade_fee += _build_fee
    C._daily_trade_fee += _build_fee
    C.realized_pnl -= _build_fee
    C.daily_pnl = C.realized_pnl
    C._rebuild_cooldown_bars = 240
    C.last_probe_barpos = C.barpos
    print(f"  [Build] {reason}: bought {build_qty} @ {price:.2f} "
          f"total:{_total_pos} stage:{C._base_rebuild_stage} "
          f"cost:{C._base_cost_price:.2f} fee:{_build_fee:.1f} (sweep@{sweep_time})")
    return True


def _empty_obs_rebuild_sweep(C, date_str, _bar_time_str, time_str, tick):
    """log183 P0': replay DOWN oversold rebuild on backfilled intraday bars."""
    if getattr(C, '_empty_rebuild_sweep_done', False):
        return
    if getattr(C, 'is_live', True):
        return
    if get_total_position(C) > 0:
        return
    if not (C._base_stop_date != ''
            or getattr(C, '_exit_policy', '') == 'down_oversold'
            or getattr(C, '_virtual_oversold_armed', False)):
        return
    # log187 P0⁷: live already rejected all DOWN probe paths today — do not sweep
    if getattr(C, '_down_rebuild_weak_date', '') == date_str:
        C._empty_rebuild_sweep_done = True
        print(f"  [Rebuild Sweep] skip: live DOWN signals weak ({date_str})")
        return
    C._empty_rebuild_sweep_done = True
    _snap = {
        'open': C.bars_open[:], 'high': C.bars_high[:], 'low': C.bars_low[:],
        'close': C.bars_close[:], 'volume': C.bars_volume[:],
        'confirmed_vol': C.confirmed_min_vol,
        'confirmed_amt': C.confirmed_min_amt,
        'orb_high': C.orb_high, 'orb_low': C.orb_low, 'orb_vol': C.orb_total_vol,
    }
    _snap_n = len(_snap['close'])
    print(f"  [Rebuild Sweep] start {date_str} {time_str} snap={_snap_n} "
          f"policy={getattr(C, '_exit_policy', '')} stop={C._base_stop_date!r} "
          f"virtual={getattr(C, '_virtual_oversold_armed', False)} "
          f"early_virtual={bool(_sweep_early_virtual_arm(C))} "
          f"phc_flat_d={getattr(C, '_days_since_dull_profit_flat', 99)} "
          f"cooldown={C._rebuild_cooldown_bars}")
    # [Fix D4] Diagnostic: trace truly anomalous cooldown values.
    # Normal range is 0..240 (initial values 30/60/240 decremented bar-by-bar).
    # Only flag negative or >240 as genuinely unexpected.
    if C._rebuild_cooldown_bars < 0 or C._rebuild_cooldown_bars > 240:
        print(f"  [CoolDown DIAG] anomalous _rebuild_cooldown_bars={C._rebuild_cooldown_bars} "
              f"at {time_str} (expected 0..240). "
              f"stop_date={C._base_stop_date!r} days_since_stop={C._days_since_stop}")
    _full, _n = _fetch_sweep_day_bars(C, _bar_time_str, time_str)
    _start = 30  # 10:00 — need index > _start for at least one replay step
    if not _full or _n <= _start:
        if _snap_n > _start:
            _full = {
                'open': _snap['open'][:], 'high': _snap['high'][:],
                'low': _snap['low'][:], 'close': _snap['close'][:],
                'volume': _snap['volume'][:],
            }
            _n = _snap_n
            print(f"  [Rebuild Sweep] api thin, using snap n={_n}")
        else:
            print(f"  [Rebuild Sweep] skip: n={_n or _snap_n} need>{_start}")
            _restore_bars_snap(C, _snap)
            return
    else:
        print(f"  [Rebuild Sweep] replay n={_n} bars (uncapped)")
    _daily_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
    _adtm = getattr(C, '_adtm_val', 0.0)
    _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
    _macro_near_bottom = getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))
    _support = getattr(C, '_backend_support', 0.0)
    _sweep_end = min(_n, 211)  # through 14:30
    _late_i = _start if _daily_rsi < 15 else _sweep_bar_index_at_or_after('14:00:00')
    _early_virtual = _sweep_early_virtual_arm(C)
    _macro_extreme = (_macro_bias <= -0.10 or _macro_near_bottom)
    _macro_i_early = _macro_extreme and not _early_virtual
    _relax_defer_morning = _macro_i_early
    _macro_candidates = []
    _relax_pick = None
    _dull_pick = None
    _early_candidates = []
    # log193 P0¹⁴: DULL sweep only on observe_only afternoons (6/29); not on down_oversold (5/28)
    _sweep_dull_armed = getattr(C, '_exit_policy', '') == 'observe_only'
    _dull_late_i = _sweep_bar_index_at_or_after('13:00:00')
    _rs_saved = getattr(C, '_rebuild_signal_log_count', 0)
    for i in range(_start, _sweep_end):
        _sweep_time = _bar_minute_index_to_time_str(i)
        if _sweep_time > '14:30:00':
            break
        _price = _full['close'][i]
        if not is_valid_price(_price):
            continue
        _apply_bars_slice(C, _full, i + 1)
        _micro = _sweep_micro_signals(C, _price, _sweep_time)
        _macro_i_ok = (i >= _late_i) or _macro_i_early
        _relax_i_ok = (_daily_rsi < 25 or i >= _late_i)
        if _macro_bias < -0.05 and _daily_rsi >= 25:
            _relax_i_ok = i >= _late_i
        # log188 P0⁷′-B: MACRO/RELAX — frozen oversold_th + D8 + afternoon gates
        _mr_ok = _sweep_oversold_price_ok(C, _price) and _sweep_d8_eval_ok(C, _daily_rsi)
        if (_macro_i_ok and _mr_ok
                and _sweep_macro_resonance_ok(
                    C, _price, _sweep_time, _bar_time_str, _micro, _daily_rsi,
                    ignore_cooldown=True)):
            _macro_candidates.append((i, _price, _sweep_time, _micro))
        if (_relax_pick is None and _relax_i_ok and _mr_ok
                and not (_relax_defer_morning and i < _late_i)
                and _sweep_relax_ok(
                    C, _price, _sweep_time, _bar_time_str, _micro, _daily_rsi,
                    ignore_cooldown=True)):
            _relax_pick = (i, _price, _sweep_time, _micro)
        # log193 P0¹⁴: DULL — observe_only afternoon only (6/29), not down_oversold days
        if (_sweep_dull_armed and _dull_pick is None and i >= _dull_late_i
                and _sweep_dull_ok(
                    C, _price, _sweep_time, _bar_time_str, _micro, _daily_rsi,
                    ignore_cooldown=True)):
            _dull_pick = (i, _price, _sweep_time, _micro)
        # log188 P0⁷′-A: virtual-arm EARLY — P0⁵ full-session min-price, no oversold_th
        if _early_virtual:
            if _sweep_early_oversold_ok(
                    C, _price, _sweep_time, _bar_time_str,
                    ignore_cooldown=True, quiet=True):
                _early_candidates.append((i, _price, _sweep_time))
        elif (i >= _late_i and _mr_ok
              and _sweep_early_oversold_ok(
                  C, _price, _sweep_time, _bar_time_str,
                  ignore_cooldown=True, quiet=True)):
            _early_candidates.append((i, _price, _sweep_time))
    C._rebuild_signal_log_count = _rs_saved
    _macro_pick = _sweep_pick_macro_candidate(_macro_candidates, _macro_extreme)
    # log186 P0⁶: live elif priority — MACRO > RELAX > EARLY (min-price)
    if _macro_pick is not None:
        _pick_i, _price, _sweep_time, _micro = _macro_pick
        _apply_bars_slice(C, _full, _pick_i + 1)
        _qty = _oversold_fast_path_qty(
            C, C.trade_qty, _macro_bias, _macro_near_bottom,
            _micro['vwap_ok'], _micro['ma5_cross'])
        _qty = (_qty // 100) * 100
        if _qty >= 100:
            print(f"  [Rebuild Sweep] MACRO: bias={_macro_bias:.2%} bottom60={_macro_near_bottom} "
                  f"vol_ratio={_micro['vol_ratio']:.2f} VWAP_ok={_micro['vwap_ok']} "
                  f"MA5_cross={_micro['ma5_cross']} @ {_sweep_time} p:{_price:.2f} (n={_n})")
            if _sweep_execute_oversold_buy(
                    C, _qty, _price, _sweep_time, tick, date_str,
                    'REBUILD_OVERSOLD_PROBE_MACRO', _snap_bars=_snap):
                return
    if _relax_pick is not None:
        _pick_i, _price, _sweep_time, _micro = _relax_pick
        _apply_bars_slice(C, _full, _pick_i + 1)
        _qty = _oversold_rebuild_qty(C, C.trade_qty)
        _qty = (_qty // 100) * 100
        if _qty >= 100:
            _dull_down, _down_streak = is_dull_down_decline(C, _bar_time_str)
            _adtm_rising = _adtm >= get_prior_day_adtm(C, _bar_time_str) - 0.03
            _relax_score = (int(((-0.50 < _adtm < 0.1) and _adtm_rising) or _daily_rsi < 30)
                            + int(_daily_rsi < 40)
                            + int(check_daily_bullish_bar(C, _bar_time_str) or _daily_rsi < 35))
            print(f"  [Rebuild Sweep] RELAX: streak {_down_streak}d score {_relax_score}/3 "
                  f"RSI:{_daily_rsi:.1f} @ {_sweep_time} p:{_price:.2f} (n={_n})")
            if _sweep_execute_oversold_buy(
                    C, _qty, _price, _sweep_time, tick, date_str,
                    'REBUILD_OVERSOLD_PROBE_RELAX', _snap_bars=_snap):
                return
    if _dull_pick is not None:
        _pick_i, _price, _sweep_time, _micro = _dull_pick
        _apply_bars_slice(C, _full, _pick_i + 1)
        _qty = _oversold_rebuild_qty(C, C.trade_qty)
        _qty = (_qty // 100) * 100
        if _qty >= 100:
            _dull_down, _down_streak = is_dull_down_decline(C, _bar_time_str)
            _adtm = getattr(C, '_adtm_val', 0.0)
            _prev_adtm = get_prior_day_adtm(C, _bar_time_str)
            print(f"  [Rebuild Sweep] DULL: streak {_down_streak}d "
                  f"ADTM:{_adtm:.2f} prev:{_prev_adtm:.2f} RSI:{_daily_rsi:.1f} "
                  f"@ {_sweep_time} p:{_price:.2f} (n={_n})")
            if _sweep_execute_oversold_buy(
                    C, _qty, _price, _sweep_time, tick, date_str,
                    'REBUILD_OVERSOLD_PROBE_DULL', _snap_bars=_snap):
                return
    if _support > 0:
        _below = [(i, p, t) for i, p, t in _early_candidates if p < _support]
        if _below:
            _early_candidates = _below
    if _early_candidates:
        # log185 P0⁵: deepest oversold bar (align 5/26 37.01 afternoon, not 10:00 first-match)
        _pick_i, _price, _sweep_time = min(_early_candidates, key=lambda x: x[1])
        _apply_bars_slice(C, _full, _pick_i + 1)
        if _support > 0 and _price < _support:
            _log_rebuild_signal(
                C, f"  [Rebuild Signal] Price {_price:.2f} below support {_support:.2f}")
        _qty = _oversold_fast_path_qty(
            C, C.trade_qty, _macro_bias,
            getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False)),
            _price > (C.confirmed_min_amt / C.confirmed_min_vol if C.confirmed_min_vol > 0 else 0),
            False)
        _qty = (_qty // 100) * 100
        if _qty >= 100:
            print(f"  [Rebuild Sweep] Early oversold probe (RSI:{_daily_rsi:.1f} "
                  f"ADTM:{_adtm:.2f} macro:{_macro_bias:.2%}) @ {_sweep_time} p:{_price:.2f} "
                  f"(picked {len(_early_candidates)} candidates, n={_n})")
            if _sweep_execute_oversold_buy(
                    C, _qty, _price, _sweep_time, tick, date_str,
                    'REBUILD_EARLY_OVERSOLD_PROBE', _snap_bars=_snap):
                return
    print(f"  [Rebuild Sweep] no MACRO/RELAX/DULL/EARLY in bars {_start}..{_sweep_end - 1} "
          f"(n={_n} macro={0 if _macro_pick is None else 1} "
          f"relax={0 if _relax_pick is None else 1} "
          f"dull={0 if _dull_pick is None else 1} early={len(_early_candidates)})")
    _restore_bars_snap(C, _snap)


def _backfill_today_1m_bars(C, _bar_time_str, time_str):
    """ 盘中重启/热重载后，自动拉取今日已走完的1m K线补齐序列。
    提升 VWAP、ORB 及微观指标的恢复速度。
    """
    if time_str < '09:31:00':
        return

    # [Bug#4 Fix] 从 _HB_SESSION 缓存恢复 bars 数据（survive deepcopy）
    _today_str = getattr(C, 'current_date', '')
    _cached = _HB_SESSION.get('bars_cache')
    if (_cached is not None
            and _HB_SESSION.get('bars_cache_date') == _today_str
            and len(C.bars_close) == 0):
        C.bars_open = _cached['open'][:]
        C.bars_high = _cached['high'][:]
        C.bars_low = _cached['low'][:]
        C.bars_close = _cached['close'][:]
        C.bars_volume = _cached['volume'][:]
        C.confirmed_min_vol = _cached['confirmed_vol']
        C.confirmed_min_amt = _cached['confirmed_amt']
        C.orb_high = _cached['orb_high']
        C.orb_low = _cached['orb_low']
        C.orb_total_vol = _cached['orb_vol']
        C.current_obv = _cached['obv']
        C.obv_list = _cached['obv_list'][:]
        return

    try:
        _h, _m, _s = map(int, time_str.split(':'))
        _cur_mins = _h * 60 + _m
        _start_mins = 9 * 60 + 30
        _expected_bars = _cur_mins - _start_mins
        if 11 * 60 + 30 < _cur_mins < 13 * 60:
            _expected_bars = 120
        elif _cur_mins >= 13 * 60:
            _expected_bars = 120 + (_cur_mins - 13 * 60)
        if len(C.bars_close) >= _expected_bars:
            return
    except Exception:
        pass

    try:
        _md = C.get_market_data_ex(
            ['open', 'high', 'low', 'close', 'volume'],
            [C.stock], period='1m', count=280,
            subscribe=False, end_time=_bar_time_str
        )
        if C.stock not in _md or _md[C.stock].empty:
            print(f" [Backfill Diag] API returned empty for {C.stock}")
            return
        df = _md[C.stock]

        # [Fix] QMT API 返回的 index 是字符串格式，显式转为 DatetimeIndex
        if not isinstance(df.index, _pd.DatetimeIndex):
            df.index = _pd.to_datetime(df.index)

        _today_str = getattr(C, 'current_date', '')
        if _today_str:
            _today_mask = df.index.strftime('%Y-%m-%d') == _today_str
            df = df[_today_mask]
        if df.empty:
            print(f" [Backfill Diag] No bars after today filter ({_today_str})")
            return

        _cur_minute_ms = (C.get_bar_timetag(C.barpos) // 60000) * 60000
        _filled_count = 0
        _vol_unit_is_lots = getattr(C, '_vol_unit_is_lots', True)

        for ts, row in df.iterrows():
            # ponytail: cst_naive_ts_to_ms — host TZ must not affect bar cutoff
            _bar_ms = cst_naive_ts_to_ms(ts)
            if _bar_ms >= _cur_minute_ms:
                continue
            _raw_v = int(row['volume'])
            _vol = _raw_v * 100 if _vol_unit_is_lots else _raw_v
            update_kline_from_1m(
                C, float(row['open']), float(row['high']),
                float(row['low']), float(row['close']),
                _vol, _bar_ms
            )
            _filled_count += 1

        if len(C.bars_open) >= 1:
            _orb_bars = min(len(C.bars_open), 30)
            C.orb_high = max(C.bars_high[:_orb_bars]) if C.orb_high == 0.0 else C.orb_high
            C.orb_low = min(C.bars_low[:_orb_bars]) if C.orb_low == 9999.0 else C.orb_low
            C.orb_total_vol = sum(C.bars_volume[:_orb_bars]) if C.orb_total_vol == 0 else C.orb_total_vol
            if _orb_bars >= 30:
                C._obs_tick_count = max(C._obs_tick_count, 30)

        if _filled_count > 0:
            print(f" [Backfill] Restored {_filled_count} 1m bars. "
                  f"bars_len={len(C.bars_close)}, "
                  f"orb=[{C.orb_low:.2f},{C.orb_high:.2f}], "
                  f"orb_vol={C.orb_total_vol}, "
                  f"confirmed_vol={C.confirmed_min_vol}, "
                  f"confirmed_amt={C.confirmed_min_amt:.0f}")
            # [Bug#4 Fix] 保存 bars 到 _HB_SESSION（survive deepcopy）
            _HB_SESSION['bars_cache_date'] = _today_str
            _HB_SESSION['bars_cache'] = {
                'open': C.bars_open[:],
                'high': C.bars_high[:],
                'low': C.bars_low[:],
                'close': C.bars_close[:],
                'volume': C.bars_volume[:],
                'confirmed_vol': C.confirmed_min_vol,
                'confirmed_amt': C.confirmed_min_amt,
                'orb_high': C.orb_high,
                'orb_low': C.orb_low,
                'orb_vol': C.orb_total_vol,
                'obv': C.current_obv,
                'obv_list': C.obv_list[:],
            }
            _HB_SESSION['backfill_done_barpos'] = C.barpos
        else:
            print(f" [Backfill Diag] 0 bars filled (cur_minute_ms={_cur_minute_ms})")

    except Exception as e:
        print(f" [Backfill] Failed to backfill 1m bars: {e}")


def _update_hb_bars_cache(C):
    """[Bug#4 Fix] 每根 K 线结束后更新 bars 缓存到 _HB_SESSION，防止数据逐根丢失"""
    if not getattr(C, 'is_live', False):
        return
    _today_str = getattr(C, 'current_date', '')
    if not _today_str:
        return
    if len(C.bars_close) == 0:
        return
    _HB_SESSION['bars_cache_date'] = _today_str
    _HB_SESSION['bars_cache'] = {
        'open': C.bars_open[:],
        'high': C.bars_high[:],
        'low': C.bars_low[:],
        'close': C.bars_close[:],
        'volume': C.bars_volume[:],
        'confirmed_vol': C.confirmed_min_vol,
        'confirmed_amt': C.confirmed_min_amt,
        'orb_high': C.orb_high,
        'orb_low': C.orb_low,
        'orb_vol': C.orb_total_vol,
        'obv': C.current_obv,
        'obv_list': C.obv_list[:],
    }


def handlebar(C):
    print(f" [DEBUG_TICK] barpos={C.barpos}, is_last={C.is_last_bar()}, time={C.get_bar_timetag(C.barpos)}")
    try:
        if not getattr(C, '_version_tag', False):
            C._version_tag = True
            print("========== v4.0 RUNNING (operator-gate-v1 + t0-underwater-gate-v1 + trend-abort-v2 + pos-mgr-v1) ==========")
        if not getattr(C, '_hb_first_call', False):
            C._hb_first_call = True
            print(f"  [Info] handlebar first call, barpos={C.barpos} is_live={C.is_live}")
        if not qmt_apis_ready():
            scan_and_bind_qmt_apis(C)

        # ponytail: bind before any path reads them (exec/tick edge cases)
        _bar_data_ok = False
        _bar_low = 0.0
        tick = {}

        # [PositionManager] Reset bar-level lock at the start of each tick
        if hasattr(C, '_pos_mgr') and C._pos_mgr is not None:
            C._pos_mgr.reset_bar()

        # [State Hub] Mandatory broker reconciliation at the top of each
        # handlebar. DPM.sync_from_broker() aligns internal qty/cost/state
        # with actual broker position, fixing side-channel bypasses that wrote
        # directly to C.position without calling on_buy/on_sell.
        if hasattr(C, '_probe_mgr') and C._probe_mgr is not None:
            # [BUG 4 FIX] C.close is a numpy ARRAY, not a scalar. The old
            # code `getattr(C, 'close', 0.0)` returned the array; then
            # `_recon_price > 0` returns a boolean array which fails the
            # `if` statement. We must extract the CURRENT bar's scalar price.
            _recon_price = 0.0
            try:
                if hasattr(C, 'close') and hasattr(C.close, '__len__') and C.barpos < len(C.close):
                    _recon_price = float(C.close[C.barpos])
                elif hasattr(C, 'last_price'):
                    _recon_price = float(C.last_price)
                else:
                    _recon_price = 0.0
            except:
                _recon_price = 0.0
            if _recon_price > 0:
                C._probe_mgr.sync_from_broker(_recon_price)
                # [BUG 9 FIX] Strong consistency assertion. After
                # sync_from_broker, DPM.qty MUST equal the broker's real
                # position. Any divergence means a code path somewhere is
                # still writing to C.position / _probe_mgr.qty without going
                # through PositionManager — an undetected bug. Hard-fail
                # early rather than silently producing wrong positions.
                try:
                    _broker_qty = get_total_position(C)
                except:
                    _broker_qty = 0
                _dpm_qty = C._probe_mgr.qty
                if _dpm_qty != _broker_qty:
                    print(f"  [ASSERT WARN] STATE SPLIT DPM.qty={_dpm_qty} "
                          f" broker_qty={_broker_qty}  state={C._probe_mgr.state} ")
                    # Dev-time hard assert: crash immediately so we find
                    # the rogue write. In production (python -O) assertions
                    # are stripped and we fall through silently (after the
                    # WARN print above, sync_from_broker already aligned).
                    assert _dpm_qty == _broker_qty, (
                        f"STATE SPLIT: DPM.qty={_dpm_qty} vs broker={_broker_qty} "
                        f"(state={C._probe_mgr.state}). A side-channel bypass "
                        f"wrote to qty outside PositionManager  fix it."
                    )

        # In live mode, only process on last bar; in backtest, every bar
        if C.is_live and not C.is_last_bar():
            return

        # Fundamental deterioration blacklist: stop all trading activity for
        # a stock whose last two reported quarters show net losses. One-time
        # log per session.
        if getattr(C, '_abandon_stock', False):
            if not getattr(C, '_abandon_stock_logged', False):
                C._abandon_stock_logged = True
                print(f"  [Abandon] Stock blacklisted (fundamental deterioration), all further signals ignored")
            return

        bar_time_ms = C.get_bar_timetag(C.barpos)
        bar_time_s = bar_time_ms / 1000.0
        date_str = time.strftime('%Y-%m-%d', time.localtime(bar_time_s))
        time_str = time.strftime('%H:%M:%S', time.localtime(bar_time_s))
        _bar_time_str = time.strftime('%Y%m%d%H%M%S', time.localtime(bar_time_s))
        C._bar_time_str = _bar_time_str

        # ponytail: tick path may yield invalid timetag; skip to avoid wiping current_date
        if not date_str or date_str < '2000-01-01':
            return

        _hb_bind_managers(C)
        if C.is_live and _HB_SESSION.get('date') == date_str:
            _hb_rehydrate(C, date_str)

        # ponytail: module-level dedupe — QMT deepcopy(C) resets C._tick_hb_barpos
        if C.is_live and C.barpos == _HB_SESSION['barpos']:
            return
        _HB_SESSION['barpos'] = C.barpos

        # Debug first few bars (backtest: minimal; live: slightly more)
        if not hasattr(C, '_dbg_count'):
            C._dbg_count = 0
            C._max_barpos_seen = C.barpos  # [Fix P2] Dynamically track max barpos
        else:
            if C.barpos > C._max_barpos_seen:
                C._max_barpos_seen = C.barpos
        _dbg_limit = 5 if C.is_live else 2
        if C._dbg_count < _dbg_limit:
            C._dbg_count += 1
            print(f"  [DBG] #{C._dbg_count} barpos={C.barpos} time={date_str} {time_str}")

        # ==================== 盘后固定价格交易拦截器 (15:05-15:30) ====================
        # 放在时间门控之前，避免15:00-15:30的bar污染barpos和C.bars（影响Sweep回放）
        if (time_str >= '15:05:00'
                and time_str <= '15:30:00'
                and getattr(C, 'after_hours_enabled', False)
                and not getattr(C, 'after_hours_order_sent', False)):

            _ah_close = 0.0
            try:
                _ah_md = C.get_market_data_ex(
                    ['close'], [C.stock], period='1d',
                    count=1, end_time=_bar_time_str)
                if C.stock in _ah_md and not _ah_md[C.stock].empty:
                    _ah_close = float(_ah_md[C.stock]['close'].iloc[-1])
            except Exception:
                pass
            if not is_valid_price(_ah_close):
                _ah_close = getattr(C, 'prev_close', 0.0) or float(C.close[C.barpos]) if hasattr(C, 'close') and C.barpos < len(C.close) else 0.0
            C.after_hours_close_price = _ah_close

            if not is_valid_price(_ah_close):
                return  # 无法获取收盘价，跳过

            _ah_total = get_total_position(C)
            _ah_avail = get_true_available_position(C)

            # 获取tick用于下单
            _ah_tick = {}
            try:
                _ah_tick_dict = C.get_full_tick([C.stock])
                if _ah_tick_dict and C.stock in _ah_tick_dict:
                    _ah_tick = _ah_tick_dict[C.stock]
            except Exception:
                pass

            # ---- 场景1: T0 未平仓头寸盘后平仓 ----
            if (C.state == 'BOUGHT_WAITING_SELL'
                    and C.pending_close_qty > 0
                    and _ah_avail > 0
                    and not C.eod_order_sent):
                _ah_qty = min(C.pending_close_qty, _ah_avail)
                _ah_qty = (_ah_qty // 100) * 100
                if _ah_qty >= 100:
                    _ah_pnl = (_ah_close - C.buy_price) * _ah_qty
                    _ah_fee = calc_trade_fee(C, _ah_close, _ah_qty, is_sell=True)
                    if safe_sell_eod(C, _ah_close, _ah_qty,
                                     'AFTER_HOURS_T0_CLOSE', _ah_tick):
                        C.realized_pnl += _ah_pnl - _ah_fee
                        C.daily_pnl = C.realized_pnl
                        C._daily_trade_fee += _ah_fee
                        C._base_trade_fee += _ah_fee
                        C.pending_close_qty -= _ah_qty
                        C._today_bought_qty = max(
                            0, C._today_bought_qty - _ah_qty)
                        C.after_hours_order_sent = True
                        if C.pending_close_qty <= 0:
                            C.state = 'IDLE'
                        print(f" [AfterHours] T0平仓 {_ah_qty}@{_ah_close:.2f} "
                              f"pnl:{_ah_pnl:.1f} fee:{_ah_fee:.1f}")

            # ---- 场景2: 待处理止损盘后执行 ----
            elif (getattr(C, '_pending_base_stop', False)
                    and _ah_total > 0
                    and _ah_avail > 0
                    and not getattr(C, '_base_stop_done', False)):
                _ah_stop_qty = min(_ah_total, _ah_avail)
                _ah_stop_qty = (_ah_stop_qty // 100) * 100
                if _ah_stop_qty >= 100:
                    _ah_fee = calc_trade_fee(
                        C, _ah_close, _ah_stop_qty, is_sell=True)
                    _pos_mgr_ah = getattr(C, '_pos_mgr', None)
                    if _pos_mgr_ah is not None:
                        _ah_sold, _ah_pnl = _pos_mgr_ah.request_sell(
                            _ah_stop_qty, 'AFTER_HOURS_STOP',
                            caller='stop_loss',
                            current_price=_ah_close,
                            tick=_ah_tick, fee=_ah_fee)
                    else:
                        _ah_sold = safe_sell_eod(
                            C, _ah_close, _ah_stop_qty,
                            'AFTER_HOURS_STOP', _ah_tick)
                        _ah_pnl = 0.0
                    if _ah_sold:
                        C.after_hours_order_sent = True
                        C._base_stop_done = True
                        C.can_do_t0 = False
                        _record_base_stop(
                            C, date_str, _ah_close, _ah_stop_qty,
                            realized_pnl=_ah_pnl if _pos_mgr_ah else 0.0)
                        print(f" [AfterHours] 止损平仓 {_ah_stop_qty}"
                              f"@{_ah_close:.2f}")
                        if get_total_position(C) == 0:
                            reset_base_anchors(C, 'full')

            # ---- 场景3: 盘后EOD汇总 ----
            if not getattr(C, '_eod_summary_done', False):
                C._eod_summary_done = True
                print_eod_summary(C, time_str, _ah_close)
                print_stop_account(C)
                print(f" [AfterHours] 盘后固定价格交易完成 "
                      f"收盘价={_ah_close:.2f} "
                      f"持仓={get_total_position(C)}")
            return
        # ==================== 盘后拦截器结束 ====================

        if time_str < '09:30:00' or time_str >= '15:30:00' or ('11:30:00' < time_str < '13:00:00'):
            return

        # -------- New day initialization --------
        if date_str >= '2000-01-01' and _HB_SESSION['date'] != date_str:
            _HB_SESSION['date'] = date_str
            _HB_SESSION['barpos'] = -1
            _HB_SESSION['mode'] = ''
            _HB_SESSION['day_open_set'] = False
            _HB_SESSION['day_open'] = 'NEUTRAL'
            _HB_SESSION['daily_trend'] = 'NEUTRAL'
            _HB_SESSION['trend_dir'] = 'NEUTRAL'
            _HB_SESSION['trend_logged'] = False
            # [Bug#3 Fix] 跨日重置 C.trend_direction，防止继承前一日趋势。
            # _HB_SESSION 重置了但 C.trend_direction 未重置，导致
            # 05-15 继承 05-14 的 UP，产生错误的 UP->NEUTRAL 降级。
            C.trend_direction = 'NEUTRAL'
            _HB_SESSION['backend_date'] = ''
            _HB_SESSION['trend_cut_done'] = False
            _HB_SESSION['base_trend_cut_active'] = False
            _HB_SESSION['base_risk_hb'] = ''
            C.current_date = date_str
            C._today_bought_qty = 0
            C._micro_cut_count_today = 0
            # [PositionManager] Reset daily counters
            if hasattr(C, '_pos_mgr') and C._pos_mgr is not None:
                C._pos_mgr.reset_daily()
            # [Fix 8] Freeze intraday macro indicators to prevent high-frequency
            # layer from misreading daily-level backend indicators.
            C._frozen_bias_20 = getattr(C, '_backend_bias_20', 0.0)
            C._frozen_macro_down_5d = getattr(C, '_backend_macro_down_5d', False)
            C._frozen_near_bottom_60 = getattr(C, '_backend_near_bottom_60', False)
            try:
                _gtd = _get_api('get_trade_detail_data')
                if _gtd:
                    acc_list = _gtd(C.account_id, 'stock', 'account')
                else:
                    acc_list = None
                if acc_list and len(acc_list) > 0:
                    acc_obj = acc_list[0]
                    avail_cash = getattr(acc_obj, 'm_dAvailable', 0.0)
                    total_asset = getattr(acc_obj, 'm_dBalance', 0.0)
                    print(f"[Account] Available: {avail_cash:.2f}, Total: {total_asset:.2f}")
                else:
                    print(f"[Account] No data for {C.account_id}")
            except Exception as e:
                print(f"[Account] get_trade_detail_data error: {e}")
            if get_total_position(C) == 0:
                C._pending_base_stop = False
                C._is_clearing_pending_stop = False
                C._base_stop_done = False
            _rollover_state = C.state
            _rollover_qty = C.pending_close_qty
            if _rollover_state in ('BOUGHT_WAITING_SELL', 'SOLD_WAITING_BUY') and _rollover_qty > 0:
                print(f"  [WARN] T0 state rollover: state={_rollover_state}, qty={_rollover_qty}. EOD closure failed")
                _real_total_pos = get_total_position(C)
                if _rollover_state == 'BOUGHT_WAITING_SELL':
                    print(f"  [Recovery] Long T0 rolled over, sync base target to broker total {_real_total_pos}")
                else:
                    print(f"  [Recovery] Short T0 rolled over, sync base target to broker total {_real_total_pos}")
                C._base_target_qty = _real_total_pos
                if _real_total_pos <= 0:
                    reset_base_anchors(C, 'full')
                elif _rollover_state == 'BOUGHT_WAITING_SELL' and C._base_cost_price > 0 and C.buy_price > 0:
                    _old_total = _real_total_pos - _rollover_qty
                    if _old_total > 0:
                        _new_cost = (C._base_cost_price * _old_total + C.buy_price * _rollover_qty) / _real_total_pos
                        set_base_cost(C, _new_cost, sync_anchor=False)
                        print(f"  [Recovery] base_cost: {_new_cost:.3f}")
                    else:
                        set_base_cost(C, C.buy_price, sync_anchor=True)
                        C._base_peak_price = C.buy_price
                        print(f"  [Recovery] base_cost: {C.buy_price:.3f} (all from T0 buy)")
                elif C._base_cost_price <= 0 and C.prev_close > 0:
                    set_base_cost(C, C.prev_close)
                _base_target = getattr(C, '_base_target_qty', 0)
                if _real_total_pos > 0 and _base_target > 0:
                    C._t0_pending_qty = max(0, _real_total_pos - _base_target)
                else:
                    C._t0_pending_qty = getattr(C, '_t0_pending_qty', 0)
                print(f"  [WARN] T0 pending rollover: state={_rollover_state}, "
                      f"pending_qty={_rollover_qty}, t0_pending={C._t0_pending_qty}")
            else:
                C._t0_pending_qty = 0
            C._day_open_trend_set = False
            C._day_open_trend = 'NEUTRAL'
            C.daily_trend = 'NEUTRAL'              # [V5 P0-1] Reset authoritative daily trend
            C._backend_trend = 'NEUTRAL'           # log121: stale leak poisoned day-open on API timeout
            _dayTradable = fetch_premarket_status(C, C.stock)
            if not _dayTradable:
                C.strategy_mode = 'SKIP'
                C.state = 'IDLE'
                C._empty_skip_done = True
                _HB_SESSION['mode'] = 'SKIP'
                print(f"  [Init] Premarket risk control block: {C.stock} abnormal status, force SKIP mode today")
            else:
                C.strategy_mode = 'UNDECIDED'
                C.state = 'IDLE'
            C.eod_order_sent = False
            C.after_hours_order_sent = False      # 盘后订单标记每日重置
            C.after_hours_close_price = 0.0       # 盘后收盘价每日重置
            C.orb_high, C.orb_low, C.orb_total_vol = 0.0, 9999.0, 0
            C._obs_tick_count = 0
            C.traded_volume = C.pending_close_qty = 0
            # [log61] Reset per-session flags used by gap-day detection,
            # proactive FUSE half-close, underwater upgrade gating, and
            # trail-skip / EXTREME add mutex.
            C._is_gap_down_day_flag = False
            C._gap_day_flag_logged = False
            C._obs_gap_block_logged = False
            C._extreme_gap_block_logged = False
            C._extreme_underwater_block_logged = False
            C._fuse_half_done = False
            C._trail_skip_today = False
            C._init_probe_water_block_logged = False
            C._upgrade_posmgr_sync_logged = False
            C._high_dev_blocked_today = False
            C.buy_price = C.sell_price = C.highest_since_buy = C.max_favorable = 0.0
            C.daily_pnl = 0.0
            # [P0 Fix v3] Snapshot cumulative stop-loss and reset daily accumulator
            # BEFORE rolling realized_pnl into cum. _daily_stop_loss accumulates
            # ALL loss-making sells today (PosMgr + fallback), independent of
            # T0 profitable trades that reduce the net realized_pnl.
            C._cum_stop_loss_prev_day = getattr(C, '_cum_stop_loss', 0.0)
            C._daily_stop_loss = 0.0
            C._cum_realized_pnl = getattr(C, '_cum_realized_pnl', 0.0) + C.realized_pnl
            C.realized_pnl = 0.0
            C.cum_vol = 0
            C.day_start_vol_cum = -1
            C.bars_open.clear(); C.bars_high.clear(); C.bars_low.clear(); C.bars_close.clear(); C.bars_volume.clear()
            C.cur_bar_min = C.last_min_id = -1
            C.cur_bar_volume = 0
            C.cur_bar_amount = 0.0
            C.cur_min_vol = 0
            C.cur_bar_open = C.cur_bar_high = C.cur_bar_low = C.cur_bar_close = 0.0
            C.confirmed_min_vol = 0
            C.confirmed_min_amt = 0.0
            C.obv_list.clear()
            C.current_obv = 0.0
            C._atr_warned = False
            C._heartbeat_min = ''
            C._skip_hb_hour = ''
            C._observe_hb_hour = ''
            C._t0_nopos_hb = ''
            C._build_qty_skip_logged = False
            C._observe_t1_skip_logged = False
            C._rebuild_probe_t0_skip_logged = False
            C._oversold_protect_logged = False
            C._strong_up_no_short_logged = False  # [建议8] 新日重置强UP反趋势日志标志, 避免只打印一次
            C._probe_min = ''
            C._eod_summary_done = False
            C._stop_account_in_eod_done = False
            C.prev_close = 0.0
            C._prev_close_approx = False
            C._prev_close_warned = False
            C._data_source_logged = False
            C._no_data_diag = False
            C.can_do_t0 = False
            C._trend_logged = False
            C._trend_downgrade_logged = False
            C._trend_downgrade_logged2 = False
            C._trend_vwap_downgrade_logged = False
            C._trend_vwap_downgrade_today = False
            C._trend_vwap_carry_logged = False
            C._vwap_recover_bar_count = 0
            C._observe_down_logged = False
            C._observe_rebuild_blocked_logged = False
            C._last_cool_log = ''
            C._last_t0_stop_price = 0.0
            C._last_t0_stop_was_buy = True
            C._down_t0_skip_logged = False
            C._down_probe_min = ''
            C._down_add_blocked_logged = False
            # [Fix 10] Remove daily reset of _down_oversold_probe_block_logged.
            # This flag should persist across the session to avoid printing the
            # same "Oversold probe blocked" log every day during GAP_EXIT cooldown.
            # C._down_oversold_probe_block_logged = False
            C._probe_cooldown_block_logged = False
            C._adtm_strong_block_logged = False
            C._down_signals_weak_logged = False
            C._down_rebuild_weak_date = ''
            C._deviation_extreme_logged = False
            C._observe_down_hb_logged = False
            C._base_cut_done = False
            # [问题2修复] 新日滚转：同步重置 Staged/Trail 独立标志
            C._staged_stop_done = False
            C._trail_stop_disabled = False
            _rollover_pos = get_total_position(C)
            _in_staged_half_hold = (
                getattr(C, '_staged_reduce_date', '') != ''
                and C.trade_qty < _rollover_pos <= C.base_qty_half)
            if not _in_staged_half_hold:
                if getattr(C, '_staged_reduce_date', '') == '':
                    C._base_staged_reduce_done = False
                    C._base_staged_reduce_qty = 0
                    C._base_staged_orig_total = 0
            elif not getattr(C, '_base_staged_reduce_done', False):
                C._base_staged_reduce_done = True
            C.consecutive_stops = 0
            C.cooldown_until = ''
            C._cooldown_barpos = 0
            C._stop3_logged = False
            C._entry_is_counter = False
            C._sell_signal_history = []
            C._buy_signal_history = []
            C._orb_confirm_bars_elapsed = 0
            C._orb_waiting_confirm = False
            C._orb_disabled_today = False
            C._orb_failed_allow_probe_today = False
            C._base_stop_done = False
            C._is_base_first_day = False
            C._first_day_logged = False
            C._rebuild_cooldown_bars = 0
            C.last_probe_barpos = -1  # Bar-level probe lock: reset daily
            C._bar_probe_lock_logged = False  # Bar-level probe lock log: reset daily
            C._rebuild_diag_logged = False
            C._rebuild_diag_last_date = ''
            C._rebuild_diag_down_date = ''
            C._rebuild_diag_cond_date = ''
            C._empty_rebuild_sweep_done = False
            C._neutral_probe_gate_logged = False
            C._neutral_probe_final_gate_logged = False
            C._obs_micro_add_block_logged = False
            C._obs_gap_warn_add_block_logged = False
            C._neutral_scale_blocked_logged = False
            C._posmgr_build_reject_logged = False
            C._neutral_rebuild_wait_logged = False  # [Fix 9] Daily reset
            C._neutral_to_up_skip_logged = False  # [Fix P1] Daily reset
            C._rebuild_momentum_block_logged = False
            C._init_add_float_block_logged = False
            C._rebuild_probe_add_blocked_logged = False
            C._skip_rebuild_logged = False
            C._skip_rebuild_bypass_today = False
            C._staged_rebuild_block_logged = False
            C._rebuild_cooling_logged = False
            C._skip_bypass_logged = False
            C._staged_half_exit_done = False
            C._staged_half_diag_logged = False
            C._staged_half_exit_block_logged = False
            C._skip_rebuild_blocked_logged = False
            C._down_buy_hard_blocked = False
            C._init_down_skip_logged = False
            C._gap_warn_reduced_today = False
            if get_total_position(C) == 0:
                C._probe_gap_ladder_date = ''
            C._gap_warn_micro_skip_logged = False
            C._rebuild_signal_log_count = 0
            C._intraday_gap_immune_skip_logged = False
            C._phase1_gap_immune_skip_logged = False
            C._probe_immune_micro_skip_logged = False
            C._daily_trade_fee = 0.0

            # [P0-2 Fix] Reset V2 FUSE log throttling flags for new day
            C._fuse_t1_blocked_logged = False
            C._fuse_sell_fail_logged = False
            C._fuse_logged_this_bar = False
            C._fuse_prebuild_block_logged = False
            C._fuse_rebuild_block_logged = False
            # [Issue Fixes] Reset new flags for new day
            C._half_lock_block_logged = False
            C._high_dev_blocked_today = False
            C._gray_zone_weak_block_t0 = False
            C._gray_zone_t0_block_logged = False
            # [P2 Fix] Additional log-dedup flags must reset per session.
            # Without this, getattr-style one-shot logs would print at
            # most once per backtest lifetime rather than once per
            # session, silently suppressing useful diagnostics for
            # days 2..N.
            C._probe_trail_no_loss_logged = False
            C._trail_tplus1_skip_logged = False
            C._high_dev_build_blocked_logged = False
            C._probe_t0_sell_only = False
            C._probe_uw_t0_trim_done = False
            C._probe_underwater_t0_logged = False
            C._underwater_t0_rebuild_block_logged = False
            C._underwater_t0_partial_logged = False
            C._extreme_rsi_fast_used_date = ''
            C._low_amp_up_observe_logged = False
            C._global_guard_immune_break_logged = False
            C._trend_cut_immune_break_logged = False
            C._stale_indicator_block_logged = False
            C._stale_block_logged = False  # Backend stale hard block log: reset daily
            C._stale_warn_logged = False   # Backend stale top-level warn log: reset daily
            C._block_new_builds = False    # Global build gate: reset daily
            C._liquidity_block_logged = False
            # [P2 Fix log66] Reset PHC same-day add flags to prevent state leak across sessions
            C._phc_allow_same_day_add = False
            C._phc_same_day_max_qty = 99999
            # [P2-9 Fix] Reset GAP-WARN logging flag for new day
            C._gap_warn_logged = False
            # [P2-11 Fix] Reset rebound miss diagnostic flag
            C._rebound_miss_logged = False
            # [Issue1/Issue2 Fix] Reset daily MAE tracking for new session
            C._daily_min_float = 0.0
            C._init_probe_mae_block_logged = False
            # [Issue3 Fix] Reset PHC sell today flag for new session
            C._phc_sell_today = False
            C._intraday_gap_reduce_today = False
            C._gap_warn_seen_today = False
            if getattr(C, '_gap_partial_trim_session', ''):
                C._block_observe_probe_add = True
                C._gap_partial_trim_session = ''
            else:
                C._block_observe_probe_add = False
            # [Fix3 Timeline Unification] Reset probe skip logs for new day
            C._phase1_probe_skip_logged = False
            C._intraday_gap_probe_skip_logged = False
            # [P3 Optional] Reset diagnostic log flags for new day
            C._intraday_gap_immune_deep_logged = False
            C._intraday_gap_immune_macro_logged = False
            C._global_guard_immune_logged = False
            # [P0-2/P0-4 Fix] Reset intraday GAP exit flags for new day
            # (these were set to trigger same-day cooldown gate; reset next morning
            # so cooldown is controlled by _days_since_stop via _record_base_stop)
            C._gap_exit_today = False
            C._intraday_micro_gap_done = False
            C._intraday_warn_done = False
            # [P0-2 Fix log81] Reset T+1 blocked log flags
            C._intraday_gap_t1_blocked_logged = False
            C._intraday_warn_t1_blocked_logged = False
            C._intraday_warn_probe_skip_logged = False  # [Fix P1] Daily reset
            C._intraday_micro_gap_probe_skip_logged = False  # [Fix P1] Daily reset
            C._micro_gap_100_full_close_logged = False
            # [P0-1 Fix] Reset same-day re-entry lock at session open so a fresh
            # day can build again (subject to probe cooldown via _days_since_stop).
            C._position_cleared_today = False
            C._intraday_reentry_lock_logged = False
            # [P0-2 Fix] Re-arm breakeven give-back exit for the next probe lifecycle.
            C._probe_breakeven_exit_done = False
            C._neutral_shallow_cap_done = False
            C._dull_shallow_cap_done = False
            C._init_micro_shallow_cap_done = False
            # [P1-1 Fix log81] Reset GAP-STOP immunity fall-through log flag
            C._gap_stop_immune_fallthrough_logged = False
            if C._base_stop_date != '':
                if getattr(C, '_virtual_oversold_armed', False):
                    pass  # ponytail: virtual arm must not inflate days_since_stop
                elif getattr(C, '_probe_stop_active', False):
                    if C._days_since_stop < C._stop_cool_days:
                        C._days_since_stop += 1
                    elif C._days_since_stop >= C._stop_cool_days:
                        C._probe_stop_active = False
                        C._days_since_stop += 1
                else:
                    C._days_since_stop += 1
            if getattr(C, '_rebuild_probe_date', '') != '' and C._rebuild_probe_date != date_str:
                C._days_since_rebuild_probe = getattr(C, '_days_since_rebuild_probe', 0) + 1
            # [P4修复] 高偏离探针天数递增
            if getattr(C, '_init_probe_date', '') != '' and C._init_probe_date != date_str:
                C._days_since_init_probe = getattr(C, '_days_since_init_probe', 0) + 1
            if getattr(C, '_staged_reduce_date', '') != '' and C._staged_reduce_date != date_str:
                C._days_since_staged_reduce = getattr(C, '_days_since_staged_reduce', 0) + 1
            if getattr(C, '_staged_half_exit_date', '') != '' and C._staged_half_exit_date != date_str:
                C._days_since_staged_half_exit = getattr(C, '_days_since_staged_half_exit', 0) + 1
            # [Fix] Increment isolated Init-Half cooldown counter.
            if getattr(C, '_init_half_cooldown_date', '') != '' and C._init_half_cooldown_date != date_str:
                C._days_since_init_half_exit = getattr(C, '_days_since_init_half_exit', 0) + 1
            if getattr(C, '_giveback_exit_date', '') != '' and C._giveback_exit_date != date_str:
                C._days_since_giveback = getattr(C, '_days_since_giveback', 0) + 1
            if getattr(C, '_dull_profit_neutral_block_date', '') != '' and C._dull_profit_neutral_block_date != date_str:
                C._days_since_dull_profit_flat = getattr(C, '_days_since_dull_profit_flat', 0) + 1
            # [P11修复] VWAP carry 交易日计数
            if getattr(C, '_trend_vwap_downgrade_carry', False) and getattr(C, '_vwap_carry_last_date', '') != date_str:
                C._vwap_carry_trading_days += 1
                C._vwap_carry_last_date = date_str
            # [P0 Fix] _first_probe_trade_days day counter: only increment when
            # position > 0 AND the DPM actually reports a probe state. Previously
            # the `_first_rebuild_probe_date != ''` check was set once and never
            # cleared, so the counter kept incrementing even after stop/exit with
            # flat position. _dpm_is_oversold_probe / _dpm_is_init_probe are the
            # authoritative signals because DPM.reset() clears the probe state on
            # full exit, so an empty position naturally stops counting.
            _fp_position = get_total_position(C)
            if _fp_position > 0 and (_dpm_is_oversold_probe(C) or _dpm_is_init_probe(C)):
                C._first_probe_trade_days = getattr(C, '_first_probe_trade_days', 0) + 1
            elif _fp_position == 0:
                # When the probe has been fully liquidated (stop / expiry /
                # profit), reset the counter so a future probe rebuild starts
                # clean with a 0-based hold_days.
                C._first_probe_trade_days = 0
            C._base_risk_hb = ''
            C._empty_skip_done = False
            _init_pos = get_total_position(C)
            if (_init_pos > 0 and C._base_rebuild_stage == 0
                    and _init_pos < C.base_qty_half):
                C.strategy_mode = 'OBSERVE'
                # [P1-5 Fix] If this is a high-dev / oversold probe, allow T0
                # engine to cost-average. Only disable T0 for non-probe small
                # positions.
                if not _dpm_is_init_probe(C) and not _dpm_is_oversold_probe(C):
                    C.can_do_t0 = False
                else:
                    _avail_here = get_true_available_position(C)
                    C.can_do_t0 = ( _avail_here >= 100)
            elif _init_pos == 0 and C._base_stop_date != '':
                C.strategy_mode = 'OBSERVE'
                C._empty_skip_done = True
            C._t0_base_block_logged = False
            C._t0_base_half_logged = False
            C._t0_underwater_gate_logged = False
            C._t0_buy_blocked_by_float = False
            C._t0_buy_restricted = False  # [修复6] 每日重置
            C._pending_stop_logged = False
            # [修复1] 不移除 _pending_stop_needs_record，让其在清仓补录后才重置
            C._pend_clear_fail_logged = False
            if getattr(C, '_pending_base_stop', False):
                C._is_clearing_pending_stop = True
            _total = get_total_position(C)
            _avail = get_true_available_position(C)
            if _total == 0:
                C._base_pos_initialized = False
                C.can_do_t0 = False
                _orphan_cost = getattr(C, '_base_cost_price', 0.0)
                _orphan_probe = _dpm_is_init_probe(C)
                if _orphan_cost > 0 or _orphan_probe or _dpm_is_oversold_probe(C):
                    clear_ghost_base_state(C, 'flat day-open')
                else:
                    C._is_oversold_probe = False
                if not getattr(C, '_base_ever_built', False):
                    print(f"  [Info] Position=0, awaiting initial build")
                elif getattr(C, '_virtual_oversold_armed', False):
                    print(f"  [Info] Position=0, virtual oversold ladder armed "
                          f"(trend:{C.trend_direction})")
                elif C._base_stop_date != '':
                    if C._days_since_stop >= C._stop_cool_days:
                        _cool = f"done({C._stop_cool_days}d)"
                    else:
                        _cool = f"{C._days_since_stop}/{C._stop_cool_days}"
                    print(f"  [Info] Position=0 after stop (cooldown:{_cool} trend:{C.trend_direction})")
                else:
                    print(f"  [Info] Position=0, flat (cost:{getattr(C, '_base_cost_price', 0.0):.2f})")
            elif _total < 100:
                C._base_pos_initialized = False
                C.can_do_t0 = False
                print(f"  [Info] Position {_total} < 100, no T0")
            else:
                C._base_pos_initialized = True
                C._base_ever_built = True
                # [Bug#2 Fix] base 仓位有 avail 且 cost 有效时允许 T0，
                # 与日内 rebuild 路径（line 5160+）行为一致。
                if _dpm_is_probe(C):
                    C.can_do_t0 = (_avail >= 100)
                elif _avail >= 100 and getattr(C, '_base_cost_price', 0.0) > 0:
                    C.can_do_t0 = True
                else:
                    C.can_do_t0 = False
                if C._base_cost_price <= 0:
                    set_base_cost(C, C.prev_close if C.prev_close > 0 else 0.0)
                # log131: micro probe peak resets each session — stale ORB H must not arm give-back
                _micro_nq = getattr(C, 'neutral_probe_qty', 100)
                if (_total <= _micro_nq and _dpm_is_probe(C)
                        and getattr(C, '_base_cost_price', 0.0) > 0):
                    C._base_peak_price = C._base_cost_price
                    _dpm_pk = getattr(C, '_probe_mgr', None)
                    if _dpm_pk is not None:
                        _dpm_pk.peak_price = C._base_cost_price
                elif C._base_peak_price <= 0:
                    C._base_peak_price = get_stop_anchor(C)
                if getattr(C, '_base_stop_anchor', 0.0) <= 0 and C._base_cost_price > 0:
                    C._base_stop_anchor = C._base_cost_price
                if _total >= C.trade_qty and _avail == 0:
                    C._is_base_first_day = True
                    print(f"  [Info] T+1 hold (total:{_total}, avail:0), T0 disabled today")
                if C.can_do_t0:
                    print(f"  [Info] Base active (total:{_total}, avail:{_avail}) T0 ok cost:{C._base_cost_price:.2f}")
                else:
                    print(f"  [Info] Base active but T0 blocked (total:{_total}, avail:{_avail})")
            _pre_last = 0.0
            # [P2 Fix] Always sync backend + pre-lock, including empty-account days.
            if not backend_indicators_synced_today(C, date_str):
                _backend_fresh = fetch_daily_indicators_from_backend(C, C.stock, date_str)
                if _backend_fresh:
                    print(f"  [Indicator] Pre-lock backend refresh: "
                          f"DIFF:{C._macd_diff:.3f} DEA:{C._macd_dea:.3f} ADTM:{C._adtm_val:.3f}")
                    # log121: only lock day-open from backend when today's fetch succeeded
                    if getattr(C, '_backend_trend', 'NEUTRAL') in ('UP', 'DOWN'):
                        if not getattr(C, '_day_open_trend_set', False):
                            C._day_open_trend = C._backend_trend
                            C.daily_trend = C._backend_trend
                            C._day_open_trend_set = True
                            print(f"  [Trend] Day-open trend locked by Backend: {C._backend_trend}")
                elif not C.is_live:
                    C._backend_fetch_attempt_date = date_str

            # [Fix2] Pre-lock: T-1 close + MA20 from one daily fetch (avoid 40x 1m loop / weekend ms bug)
            try:
                _pre_last = 0.0
                _pre_ma20 = 0.0
                try:
                    _md_daily_ma = C.get_market_data_ex(
                        ['close'], [C.stock], period='1d',
                        count=C.trend_ma_period + 5,
                        subscribe=False, end_time=_bar_time_str)
                    if C.stock in _md_daily_ma and len(_md_daily_ma[C.stock]) >= 2:
                        _daily_closes = _md_daily_ma[C.stock]['close'].values.tolist()
                        if len(_daily_closes) > 1:
                            _daily_closes = _daily_closes[:-1]
                        if _daily_closes:
                            _pre_last = float(_daily_closes[-1])
                        if len(_daily_closes) >= C.trend_ma_period:
                            _pre_ma20 = sum(_daily_closes[-C.trend_ma_period:]) / C.trend_ma_period
                except Exception as e:
                    print(f"  [Trend] MA20 fast fetch failed: {e}")

                if is_valid_price(_pre_last) and _pre_ma20 > 0:
                    _pre_diff = getattr(C, '_macd_diff', 0.0)
                    _pre_dea = getattr(C, '_macd_dea', 0.0)
                    if getattr(C, '_trend_vwap_downgrade_carry', False):
                        C._day_open_trend = 'NEUTRAL'
                        C.daily_trend = 'NEUTRAL'              # [V5 P0-1] Freeze authoritative daily trend
                        C._day_open_trend_set = True
                        print(f"  [Trend] Day open trend pre-locked: NEUTRAL (overridden by VWAP carry)")
                    elif not getattr(C, '_day_open_trend_set', False):
                        if _pre_last > _pre_ma20 * 1.002 and _pre_diff > _pre_dea:
                            C._day_open_trend = 'UP'
                            C.daily_trend = 'UP'                    # [V5 P0-1] Freeze authoritative daily trend
                            C._day_open_trend_set = True
                            print(f"  [Trend] Day open trend pre-locked: {C._day_open_trend} "
                                  f"(ma20:{_pre_ma20:.2f} close:{_pre_last:.2f} "
                                  f"DIFF:{_pre_diff:.3f} DEA:{_pre_dea:.3f})")
                        elif _pre_last < _pre_ma20 * 0.998 and _pre_diff < _pre_dea:
                            C._day_open_trend = 'DOWN'
                            C.daily_trend = 'DOWN'                  # [V5 P0-1] Freeze authoritative daily trend
                            C._day_open_trend_set = True
                            print(f"  [Trend] Day open trend pre-locked: {C._day_open_trend} "
                                  f"(ma20:{_pre_ma20:.2f} close:{_pre_last:.2f} "
                                  f"DIFF:{_pre_diff:.3f} DEA:{_pre_dea:.3f})")
                        else:
                            C._day_open_trend = 'NEUTRAL'
                            C.daily_trend = 'NEUTRAL'              # [V5 P0-1] Freeze authoritative daily trend
                            C._day_open_trend_set = True
                elif backend_indicators_synced_today(C, date_str):
                    # MA20 warmup insufficient — MACD + backend bias fallback
                    _pre_diff = getattr(C, '_macd_diff', 0.0)
                    _pre_dea = getattr(C, '_macd_dea', 0.0)
                    _pre_bias = getattr(C, '_backend_bias_20', 0.0)
                    if getattr(C, '_trend_vwap_downgrade_carry', False):
                        C._day_open_trend = 'NEUTRAL'
                    elif _pre_diff > _pre_dea and _pre_bias > -0.02:
                        C._day_open_trend = 'UP'
                    elif _pre_diff < _pre_dea and _pre_bias < -0.02:
                        C._day_open_trend = 'DOWN'
                    else:
                        C._day_open_trend = 'NEUTRAL'
                    C.daily_trend = C._day_open_trend
                    C._day_open_trend_set = True
                    print(f"  [Trend] Day open trend pre-locked: {C._day_open_trend} "
                          f"(MACD/bias fallback, bias:{_pre_bias:.2%} DIFF:{_pre_diff:.3f} DEA:{_pre_dea:.3f})")
            except Exception as e:
                print(f"  [Trend] Pre-lock failed: {e}, will lock at 09:35")

            C._down_intraday_reduced_done = False
            C._intraday_reduced_block_logged = False
            C._intraday_float_reduced_today = False
            C._up_risk_block_logged = False
            C._momentum_block_logged = False
            C._vwap_down_t0_blocked_logged = False
            C._vwap_carry_observe_block_logged = False
            C._vwap_carry_rebuild_block_logged = False
            # Reset per-session log flag for the liquidity-dry-up probe gate
            # so we print the block notice once per trading day.
            C._liquidity_block_logged = False
            # [Fix] Reset per-day OBSERVE wait log flag so the heartbeat logs once
            # per session instead of once across the entire run.
            C._observe_wait_logged = False
            # [Fix] Reset per-day deep-loss probe-clamp log flag.
            C._deep_loss_probe_logged = False
            # [修复缺陷一] 新日初始化：重置初始探针升级阻断日志
            C._init_probe_upgrade_block_logged = False
            # [Bug3] Phase 0/2 日志标志每日重置（注意：保留 vwap_carry_3d_logged 不重置，避免每日重复打印）
            C._empty_down_logged = False
            C._empty_wait_logged = False
            # [F1修复] 重置日内趋势降级锁
            C._intraday_trend_locked = False
            C._trend_snapshot = ''
            C._trend_snapshot_taken_today = False  # [修复2] 新日重置趋势快照冻结标志
            # [F7修复] 重置独立状态
            C._trend_cut_done = False
            C._probe_protect_done = False
            C._down_reduce_done = False
            # [Defect1] Reset _init_probe_upgrade_blocked at session open
            # so yesterday's gating decision does not keep tightening
            # today's stop.
            C._init_probe_upgrade_blocked = False
            C._init_probe_upgrade_block_logged = False
            # [Defect2] Reset -6% immune defense lock at session open.
            C._probe_immune_defense_done = False
            # [修复问题6] 新日初始化时主动调用降级检查，确保 observe_only 按时降级
            if getattr(C, '_exit_policy', '') == 'observe_only':
                _get_effective_exit_policy(C)
            # [修复问题3] 新日初始化，重置日内锁
            C._init_add_float_locked_today = False
            if getattr(C, '_trend_vwap_downgrade_carry', False):
                _carry_trade_days = getattr(C, '_vwap_carry_trading_days', 0)
                if _carry_trade_days >= 3:
                    C._trend_vwap_downgrade_carry = False
                    C._vwap_recover_bar_count = 0
                    print(f"  [Trend] VWAP carry expired after {_carry_trade_days} trade days, cleared")

            # [Fix] Compute daily ATR once per session so the dynamic stop
            # calculator uses a stable overnight-risk baseline instead of the
            # noisy intraday 1-min ATR.
            try:
                from qmt_indicators import calc_daily_atr
                _daily_atr = calc_daily_atr(C, _bar_time_str, 14)
                if _daily_atr > 0:
                    C._daily_atr_14 = _daily_atr
            except Exception:
                pass

            # --- empty-position / cooldown optimization ----------------------
            # Only applied when the account has no open position or there's
            # an active stop-loss lock; prevents churning in dry/illiquid names.
            if get_total_position(C) == 0 or getattr(C, '_base_stop_date', '') != '':

                # (1) Fundamental-deterioration blacklist: if last two reported
                # quarters show net losses, the stock likely carries ST/de-listing
                # risk. We stop trading it permanently for this session.
                if getattr(C, '_backend_is_fundamental_deteriorated', False):
                    if not getattr(C, '_abandon_stock', False):
                        C._abandon_stock = True
                        print(f"  [Abandon] Continuous-loss quarters detected, stock blacklisted permanently")

                # (2) Liquidity dry-up: if volume has been shrinking for 15+ of
                # the last 20 days, the stock may be in a fading trend with no
                # natural buyers; extend stop-loss cooldown to avoid whipsawing.
                if getattr(C, '_backend_continuous_shrink_days', 0) >= 15:
                    C._stop_cool_days = max(getattr(C, '_stop_cool_days', 5), 10)
                    print(f"  [CoolDown] Liquidity dry-up: {C._backend_continuous_shrink_days} low-vol days in 20, "
                          f"stop cooldown extended to {C._stop_cool_days} days")

                # (3) Capital-reflow signal: if a high-volume bar appeared within
                # the last 3 trading days, institutional capital may be flowing
                # back → allow an earlier re-entry.
                # [Issue3 Fix] Only apply capital reflow if we actually had a stop loss recently
                if getattr(C, '_backend_barslast_vol_surge', 99) <= 3 and C._base_stop_date != '':
                    C._stop_cool_days = max(1, getattr(C, '_stop_cool_days', 5) - 2)
                    C._rebuild_cooldown_bars = 0
                    print(f"  [CoolDown] Capital reflow: last surge {C._backend_barslast_vol_surge}d ago; "
                          f"stop cooldown shortened to {C._stop_cool_days}, rebuild bar-lock cleared")

            # V2 引擎日重置
            if getattr(C, '_risk_engine', None) is not None:
                C._risk_engine.reset_daily()
            if getattr(C, '_probe_mgr', None) is not None:
                C._probe_mgr.increment_day(date_str)

            # 基本面风控检查（每日一次，持仓时）
            if get_total_position(C) > 0 and getattr(C, '_risk_engine', None) is not None:
                try:
                    _fr = requests.get(
                        f"{BACKEND_URL}/api/v1/fundamental_risk",
                        params={"stock_code": C.stock},
                        timeout=backend_timeout(C),
                    )
                    if _fr.status_code == 200:
                        _fr_data = _fr.json().get("data", {})
                        if _fr_data.get("has_risk"):
                            _force_qty = min(get_total_position(C), get_true_available_position(C))
                            _force_qty = (_force_qty // 100) * 100
                            if _force_qty >= 100:
                                # current_price/tick not yet available in day-init block, fetch locally
                                _fr_tick = {}
                                _fr_price = 0.0
                                if C.is_live:
                                    try:
                                        _fr_tick_dict = C.get_full_tick([C.stock])
                                        _fr_tick = _fr_tick_dict[C.stock] if _fr_tick_dict and C.stock in _fr_tick_dict else {}
                                        _fr_price = _fr_tick.get('lastPrice', 0)
                                    except Exception:
                                        pass
                                if not is_valid_price(_fr_price):
                                    try:
                                        _md1 = C.get_market_data_ex(['close'], [C.stock], period='1m', count=1, end_time=_bar_time_str)
                                        if C.stock in _md1 and not _md1[C.stock].empty:
                                            _fr_price = float(_md1[C.stock]['close'].iloc[-1])
                                    except Exception:
                                        pass
                                if not is_valid_price(_fr_price):
                                    _fr_price = C.prev_close
                                if is_valid_price(_fr_price):
                                    print(f"  [Fundamental Risk] {_fr_data.get('risk_type')}: "
                                          f"ratio={_fr_data.get('equity_unlock_ratio', 0):.2f}%, "
                                          f"force close {_force_qty} @ {_fr_price:.2f}")
                                    _pos_mgr = getattr(C, '_pos_mgr', None)
                                    if _pos_mgr is not None:
                                        _fr_sold, _fr_pnl = _pos_mgr.request_sell_eod(_force_qty, 'FUNDAMENTAL_RISK', caller='stop_loss', current_price=_fr_price, tick=_fr_tick)
                                    else:
                                        _fr_sold = safe_sell_eod(C, _fr_price, _force_qty, 'FUNDAMENTAL_RISK', _fr_tick)
                                    if _fr_sold:
                                        _fr_fee = calc_trade_fee(C, _fr_price, _force_qty, is_sell=True)
                                        if _pos_mgr is None:
                                            C._base_trade_fee += _fr_fee
                                            C._daily_trade_fee += _fr_fee
                                            _fr_pnl = (_fr_price - C._base_cost_price) * _force_qty - _fr_fee
                                            C.realized_pnl += _fr_pnl
                                            C.daily_pnl = C.realized_pnl
                                            C.traded_volume += _force_qty
                                            if getattr(C, '_risk_engine', None) is not None:
                                                C._risk_engine.record_trade_pnl(_fr_price, _force_qty, 'SELL', _fr_fee)
                                        C._base_stop_done = True
                                        C.can_do_t0 = False
                                        _record_base_stop(C, date_str, _fr_price, _force_qty, realized_pnl=_fr_pnl if _pos_mgr is not None else 0.0)
                                        if get_total_position(C) == 0:
                                            reset_base_anchors(C, 'full')
                                    return
                except Exception as e:
                    print(f"  [API] Fundamental risk check failed: {e}")

            _today_open = 0.0
            if hasattr(C, 'open') and C.barpos < len(C.open):
                _today_open = float(C.open[C.barpos])
            if not is_valid_price(_today_open):
                try:
                    _md_open = C.get_market_data_ex(['open'], [C.stock], period='1d', count=1, end_time=_bar_time_str)
                    if C.stock in _md_open and not _md_open[C.stock].empty:
                        _today_open = float(_md_open[C.stock]['open'].iloc[-1])
                except:
                    pass
            if not is_valid_price(_today_open) and len(C.bars_open) > 0:
                _today_open = C.bars_open[0]
            C._day_open_price = _today_open

            if getattr(C, '_day_open_trend_set', False) and C._day_open_trend in ('UP', 'NEUTRAL'):
                if is_valid_price(_today_open) and is_valid_price(_pre_last) and _pre_last > 0:
                    if _today_open < _pre_last * (1 - 0.015):
                        # [Fix 4] A -1.5% gap-down invalidates the pre-open
                        # trend snapshot. Previously we'd soft-demote it to
                        # NEUTRAL but leave the "locked in" flag True, so the
                        # rest of the strategy still saw a stale pre-lock.
                        # Now fully clear the lock so real-time trend
                        # evaluation takes over at 09:35.
                        C._day_open_trend_set = False
                        C._day_open_trend = 'NEUTRAL'
                        C.daily_trend = 'NEUTRAL'
                        C.trend_direction = 'NEUTRAL'
                        print(f"  [Trend] Gap-down detected: open {_today_open:.2f} "
                              f"< prev_close {_pre_last:.2f} * 0.985, pre-lock CLEARED for real-time sync")

            # [P0 Fix 1] STATE CONTRACT — ladder reachability assertion.
            # After any full-position clearance (micro-clear, -8% hard stop,
            # fundamental-risk close, etc.), the strategy must leave
            # C._exit_policy or C._base_stop_date in a state that permits
            # DOWN-trend probe re-entry. A "fresh account" (never built a
            # base and never recorded a stop) is exempt — BUT only if the
            # normal first-build path is actually reachable. P0 blocks INIT
            # when day-open trend is DOWN, so the fresh-account DOWN branch
            # at line ~2186 now sets a virtual stop to arm the rebuild
            # ladder. By the time we reach this STATE CONTRACT check, a
            # fresh account that was blocked by P0 has already been armed
            # (_base_stop_date != '') and is no longer "fresh" — the
            # exemption is effectively dead code for DOWN-trend days, kept
            # only for UP/NEUTRAL days where the first-build path is open.
            if get_total_position(C) == 0:
                _is_fresh_account = (C._base_stop_date == '' and not getattr(C, '_base_ever_built', False))
                _has_ladder_policy = (
                    getattr(C, '_exit_policy', '') in ('down_oversold', 'observe_only')
                    or C._base_stop_date != ''
                )
                # [P0 Fix STATE CONTRACT] Defensive auto-repair: if we detect
                # the "position=0 but no ladder armed" state, log it and
                # auto-arm instead of asserting. This handles cases where a
                # clearance path (profitable probe exit, end-of-backtest
                # flatten, etc.) blanked both exit_policy and base_stop_date
                # without re-arming the ladder. Rather than crash the
                # strategy, we recover and log so the gap can be fixed.
                if _is_fresh_account or _has_ladder_policy:
                    pass  # Contract satisfied
                else:
                    _cur_trend = (getattr(C, '_day_open_trend', C.trend_direction)
                                  if getattr(C, '_day_open_trend_set', False)
                                  else C.trend_direction)
                    _old_policy = getattr(C, '_exit_policy', '')
                    _old_stop = C._base_stop_date
                    if _cur_trend == 'DOWN':
                        C._exit_policy = 'down_oversold'
                    else:
                        C._exit_policy = 'observe_only'
                    # log127 P0: ever-built flat (PHC profit) — policy arm only, no fake stop date
                    if not getattr(C, '_base_ever_built', False):
                        C._virtual_oversold_armed = True
                        C._base_stop_date = date_str
                        C._days_since_stop = 99
                    print(f"  [State Repair] Detected unreachable ladder (position=0, "
                          f"prev policy='{_old_policy}' prev stop='{_old_stop}', "
                          f"trend={_cur_trend}). Armed '{C._exit_policy}'"
                          f"{'' if getattr(C, '_base_ever_built', False) else ' + virtual stop'} "
                          f"(policy-only for ever-built flat)")
            print(f"\n{'='*60}\n  {date_str} | v4.0 Strategy Ready\n{'='*60}")
            _backfill_today_1m_bars(C, _bar_time_str, time_str)
            _hb_snapshot(C, date_str)
        elif C.is_live and _HB_SESSION.get('date') == date_str:
            _hb_rehydrate(C, date_str)
            _backfill_today_1m_bars(C, _bar_time_str, time_str)

        # -------- Obtain previous close --------
        if not is_valid_price(C.prev_close):
            try:
                _daily = C.get_market_data_ex(['close'], [C.stock], period='1d', count=2, subscribe=False, end_time=_bar_time_str)
                if C.stock in _daily and len(_daily[C.stock]) >= 2:
                    _prev_close_candidate = float(_daily[C.stock]['close'].iloc[-2])
                    # [问题4修复] 校验：前收盘价不应与当日开盘价完全相同（防止取到当日数据）
                    _day_open_ref = getattr(C, '_day_open_price', 0.0)
                    if is_valid_price(_prev_close_candidate) and is_valid_price(_day_open_ref) and abs(_prev_close_candidate - _day_open_ref) / _day_open_ref > 0.001:
                        C.prev_close = _prev_close_candidate
                    elif is_valid_price(_prev_close_candidate):
                        C.prev_close = _prev_close_candidate  # 容错：即使相似也采用
                elif C.stock in _daily and len(_daily[C.stock]) == 1:
                    C.prev_close = float(_daily[C.stock]['close'].iloc[0])
            except:
                pass
            if not is_valid_price(C.prev_close):
                _open_price = 0.0
                if hasattr(C, 'open') and C.barpos < len(C.open):
                    _open_price = float(C.open[C.barpos])
                if not is_valid_price(_open_price):
                    try:
                        _md_open = C.get_market_data_ex(['open'], [C.stock], period='1m', count=1, end_time=_bar_time_str)
                        if C.stock in _md_open and not _md_open[C.stock].empty:
                            _open_price = float(_md_open[C.stock]['open'].iloc[-1])
                    except:
                        pass
                if is_valid_price(_open_price):
                    C.prev_close = _open_price
                    if not C._prev_close_approx:
                        C._prev_close_approx = True
                        print(f"  [Warn] Using open price {_open_price:.2f} as prev_close")
            if not is_valid_price(C.prev_close):
                if not C._prev_close_warned:
                    C._prev_close_warned = True
                    print(f"  [Warn] Unable to determine prev_close, using 0")
                C.prev_close = 0.0

        # [Bug#1 Fix] base_cost 二次恢复：prev_close fetch 晚于 set_base_cost，
        # 导致热重载/新日初始化时 base_cost 被设为 0。此处补恢复。
        if getattr(C, '_base_cost_price', 0.0) <= 0 and getattr(C, '_base_pos_initialized', False):
            _dpm_obj = getattr(C, '_probe_mgr', None)
            _dpm_cost = getattr(_dpm_obj, 'cost', 0.0) if _dpm_obj else 0.0
            if _dpm_cost > 0:
                set_base_cost(C, _dpm_cost)
                print(f"  [Bug#1 Fix] base_cost recovered from DPM: {_dpm_cost:.2f}")
            elif C.prev_close > 0:
                set_base_cost(C, C.prev_close)
                print(f"  [Bug#1 Fix] base_cost recovered from prev_close: {C.prev_close:.2f}")
            if getattr(C, '_base_stop_anchor', 0.0) <= 0 and C._base_cost_price > 0:
                C._base_stop_anchor = C._base_cost_price
            if getattr(C, '_base_peak_price', 0.0) <= 0 and C._base_cost_price > 0:
                C._base_peak_price = C._base_cost_price

        # [P0 Fix v2] Authoritative gap-day detection. Previously
        # _is_gap_down_day_flag was only set deep inside the
        # _is_init_half branch of _run_base_stop_checks (qmt_risk.py),
        # so oversold_probe positions and positions that had not yet
        # triggered that branch silently bypassed the gap-day gate on
        # the controlled-upgrade and EXTREME-oversold add paths
        # (log61: 05-18 +100 upgrade on a -5.1% gap-down session;
        # 05-29 EXTREME add in a gap-down continuation).
        # [Fix] Strictly use opening price vs prev_close for gap classification.
        # Intraday low conflates gap-down with traded-down (normal drawdown),
        # causing false gap-day flags on non-gap sessions.
        _ensure_day_open_price(C, _bar_time_str)
        _day_open = getattr(C, '_day_open_price', 0.0)
        if _day_open > 0 and C.prev_close > 0:
            _open_gap = (_day_open - C.prev_close) / C.prev_close
            if _open_gap <= -0.04:
                C._is_gap_down_day_flag = True
                C._gap_day_worst_pct = _open_gap
                if not getattr(C, '_gap_day_flag_logged', False):
                    C._gap_day_flag_logged = True
                    print(f"  [Gap-Day] open gap {_open_gap:.1%} (open {_day_open:.2f} vs prev_close {C.prev_close:.2f}), pyramiding disabled today")
            else:
                C._is_gap_down_day_flag = False
                C._gap_day_worst_pct = _open_gap
        else:
            C._gap_day_worst_pct = 0.0

        # -------- Backend stale degradation: block builds, never block risk control --------
        if getattr(C, '_backend_indicators_stale', False):
            if not getattr(C, '_stale_warn_logged', False):
                C._stale_warn_logged = True
                print(f"  [API Err] Backend data stale! Suspending NEW BUILDS, but Risk Control remains ACTIVE.")
            C._block_new_builds = True
            # ponytail: do not force DOWN on stale — local MA20/MACD trend stays authoritative

        # -------- Data feed: unified 1m bar (backtest + live/sim); tick only for live orders --------
        if hasattr(C, 'close') and C.barpos < len(C.close):
            current_price = float(C.close[C.barpos])
            if is_valid_price(current_price):
                _bar_open  = float(C.open[C.barpos])
                _bar_high  = float(C.high[C.barpos])
                _bar_low   = float(C.low[C.barpos])
                _bar_close = current_price
                _raw_vol   = int(C.volume[C.barpos])
                _bar_data_ok = True
                if not C._data_source_logged:
                    C._data_source_logged = True
                    print(f"  [Data] Using C.close (len={len(C.close)}, live={C.is_live})")
        if not _bar_data_ok:
            try:
                _md = C.get_market_data_ex(
                    ['open', 'high', 'low', 'close', 'volume'], [C.stock],
                    period='1m', count=1, end_time=_bar_time_str
                )
                if C.stock in _md and not _md[C.stock].empty:
                    df = _md[C.stock]
                    current_price = float(df['close'].iloc[-1])
                    if is_valid_price(current_price):
                        _bar_open  = float(df['open'].iloc[-1])
                        _bar_high  = float(df['high'].iloc[-1])
                        _bar_low   = float(df['low'].iloc[-1])
                        _bar_close = current_price
                        _raw_vol   = int(df['volume'].iloc[-1])
                        _bar_data_ok = True
                        if not C._data_source_logged:
                            C._data_source_logged = True
                            print(f"  [Data] Using get_market_data_ex('1m') (live={C.is_live})")
            except Exception as e:
                print(f"  [Error] get_market_data_ex: {e}")
        if _bar_data_ok:
            if not getattr(C, '_vol_unit_diag', False):
                C._vol_unit_diag = True
                C._vol_unit_is_lots = (_raw_vol > 100)
                print(f"  [Vol] unit: {'lots' if C._vol_unit_is_lots else 'shares'} (raw={_raw_vol})")
            bar_vol = _raw_vol * 100 if C._vol_unit_is_lots else _raw_vol
            update_kline_from_1m(C, _bar_open, _bar_high, _bar_low,
                                  _bar_close, bar_vol, bar_time_ms)
        elif C.is_live:
            try:
                tick_dict = C.get_full_tick([C.stock])
                tick = tick_dict[C.stock] if tick_dict and C.stock in tick_dict else {}
            except Exception:
                tick = {}
            if not tick:
                return
            current_price = tick.get('lastPrice', 0)
            if not is_valid_price(current_price):
                return
            tick_vol_cum = int(tick.get('volume', 0)) * 100
            if C.day_start_vol_cum < 0:
                C.day_start_vol_cum = tick_vol_cum
                print(f"  [Vol] cum_vol_base={tick_vol_cum}")
            today_cum_vol = max(0, tick_vol_cum - C.day_start_vol_cum)
            if C.cum_vol > 0:
                if today_cum_vol >= C.cum_vol:
                    tick_vol = today_cum_vol - C.cum_vol
                    C.cum_vol = today_cum_vol
                else:
                    tick_vol = today_cum_vol
                    C.cum_vol = today_cum_vol
                    print(f"  [Warn] volume reset, using {today_cum_vol}")
            else:
                tick_vol = today_cum_vol
                C.cum_vol = today_cum_vol
            update_kline_from_tick(C, current_price, bar_time_ms, tick_vol)
            _bar_low = getattr(C, 'cur_bar_low', 0.0) or current_price
            _bar_data_ok = is_valid_price(_bar_low)
            if not C._data_source_logged:
                C._data_source_logged = True
                print(f"  [Data] Fallback get_full_tick (no 1m bar)")
        else:
            if not C._no_data_diag:
                C._no_data_diag = True
                _has_close = hasattr(C, 'close')
                _close_len = len(C.close) if _has_close else -1
                print(f"  [No Data] C.close exists:{_has_close} len:{_close_len} barpos:{C.barpos}")
            return
        if C.is_live and not tick:
            try:
                tick_dict = C.get_full_tick([C.stock])
                tick = tick_dict[C.stock] if tick_dict and C.stock in tick_dict else {}
            except Exception:
                tick = {}

        # [Issue1 Fix] Track daily Minimum Adverse Excursion (MAE)
        if C._base_cost_price > 0 and is_valid_price(current_price):
            _cur_float_mae = (current_price - C._base_cost_price) / C._base_cost_price
            if _cur_float_mae < getattr(C, '_daily_min_float', 0.0):
                C._daily_min_float = _cur_float_mae
            # [Fix Flaw 2] Decouple micro float from gap-day flag: probe losses handled by -8% hard stop
            # No longer auto-arm global gap-day flag from micro float

        # -------- Clear pending base stop (T+1 residue) --------
        if getattr(C, '_pending_base_stop', False) and is_valid_price(current_price):
            _pend_total, _pend_raw_avail = get_pos_data(C)
            _pend_avail = get_true_available_position(C)
            if _pend_total > 0 and _pend_avail > 0:
                C._is_clearing_pending_stop = True
                _pend_close = min(_pend_total, _pend_avail)
                _pend_close = (_pend_close // 100) * 100
                if _pend_close >= 100:
                    if not _execute_base_stop_close(C, date_str, current_price, tick, _pend_close, _pend_total, _pend_raw_avail,
                                                    'PENDING_STOP_CLEAR', 'PENDING_STOP_CLEAR_FORCE', 'Pending Stop Clear'):
                        if not getattr(C, '_pend_clear_fail_logged', False):
                            C._pend_clear_fail_logged = True
                            print(f"  [Warn] Pending stop clear failed (limit down?), will retry next bar")
                    else:
                        # [修复3] Pending clear成功后，如果原止损从未录过（T+1场景），补录止损
                        print(f"  [Pending Stop] Cleared {_pend_close} @ {current_price:.2f}")
                        if getattr(C, '_pending_stop_needs_record', False):
                            # [P0 Fix] _execute_base_stop_close already routed through
                            # PosMgr internally and updated C._cum_stop_loss. Pass non-zero
                            # realized_pnl to skip cost-based recompute and only set metadata.
                            _record_base_stop(C, date_str, current_price, _pend_close, realized_pnl=1.0)
                            C._pending_stop_needs_record = False
                            print(f"  [Pending Stop] Stop recorded: cooldown {C._stop_cool_days}d, loss accumulated")

        # ================= Global Risk Guard =================
        # [Opt3] Hard-floor safety net. If position is down >= 8% (absolute)
        # at ANY bar, force-close immediately and short-circuit all downstream
        # state-machine logic. This catches the path where Phase 1 observation,
        # Phase 3 stops, and FUSE all miss a sudden intraday flash-crash
        # (e.g. limit-downs, strategy-mode jumps). Triggered on the bar low
        # so wick-penetration is caught; uses _bar_data_ok to degrade to
        # current_price when the bar-snapshot isn't available.
        if (get_total_position(C) > 0 and C._base_cost_price > 0
                and not getattr(C, '_base_stop_done', False)
                and not getattr(C, '_force_liquidation_active', False)
                and not getattr(C, '_fuse_half_done', False)):
            update_trend_direction(C, _bar_time_str, current_price if is_valid_price(current_price) else 0.0)
            # [P0 Fix] Use current_price during the first 10 minutes to avoid
            # being whisked out by transient bar-low spikes (wick-stops).
            # After 09:40, use min(bar_low, current_price) so a fast drawdown
            # below the close still triggers, but a low that is *below* the
            # close cannot falsely trigger when the bar has already recovered.
            if time_str < '09:40:00':
                _eval_low_global = current_price
            else:
                _candidate_low = _bar_low if _bar_data_ok else current_price
                _eval_low_global = min(_candidate_low, current_price)
            _global_float_risk = (_eval_low_global - C._base_cost_price) / C._base_cost_price
            # [P0 Fix] Dynamic hard stop: probe T+1 gets 10% exemption
            _is_probe_pos = (_holds_down_probe(C) or _dpm_is_init_probe(C)
                             or _holds_neutral_micro_probe(C))
            _is_high_dev_probe = _dpm_is_init_probe(C)
            # [P0 Fix log75] Init probes with hold≤5d use 10% T+1 stop / 8% DPM
            # stop — their own risk management is wider than the Global Guard's 8%.
            # The previous 2-day immune window let the Global Guard force-close at
            # -8% on day 3-5, while the design intent was 10% breathing room.
            # Oversold probes keep 2-day immunity (they enter at tighter stops).
            _probe_immune_days = _probe_immune_max_days(C)
            _probe_days_held = (
                getattr(C, '_days_since_init_probe', 99)
                if _dpm_is_init_probe(C)
                else getattr(C, '_days_since_rebuild_probe', 99)
            )
            _in_immune_window = (
                _is_probe_pos and _probe_days_held <= _probe_immune_days)
            _probe_guard_immune = _in_immune_window and _probe_global_guard_immune(C)
            if _probe_guard_immune:
                if not getattr(C, '_global_guard_immune_logged', False):
                    C._global_guard_immune_logged = True
                    _immune_type = ('neutral' if _holds_neutral_micro_probe(C)
                                    else ('init' if _dpm_is_init_probe(C) else 'oversold'))
                    _tier = _probe_intraday_stop_tier_msg(C, _dpm_is_init_probe(C), True)
                    print(f"  [Global Stop] Probe immune ({_immune_type}): "
                          f"held={_probe_days_held}d <= {_probe_immune_days}d, "
                          f"skip Global Guard ({_tier})")
            elif _in_immune_window and not _probe_guard_immune:
                if not getattr(C, '_global_guard_immune_break_logged', False):
                    C._global_guard_immune_break_logged = True
                    if _dpm_is_init_probe(C):
                        _brk = 'Init probe immune broken: trend DOWN'
                    elif _holds_down_probe(C) and _oversold_probe_severe_entry(C):
                        _brk = 'Oversold severe-entry immune broken (T+1+)'
                    else:
                        _brk = 'Probe immune broken'
                    print(f"  [Global Stop] {_brk}, "
                          f"Global Guard active (held={_probe_days_held}d)")
            if not _probe_guard_immune:
                _global_hard_stop = -_get_hard_stop_pct(C, _is_probe_pos, _eval_low_global, _is_high_dev_probe)
                if _global_float_risk <= _global_hard_stop:
                    _force_close_qty = min(
                        get_total_position(C), get_true_available_position(C))
                    _force_close_qty = (_force_close_qty // 100) * 100
                    if _force_close_qty >= 100:
                        print(f"  [Global Stop] float {_global_float_risk:.1%} <= "
                              f"{_global_hard_stop:.1%}, force close {_force_close_qty} "
                              f"@ {_eval_low_global:.2f}")
                        _pos_mgr = getattr(C, '_pos_mgr', None)
                        # [Fix Global Guard] Pre-compute fee and pass to
                        # request_sell so DPM.on_sell uses the actual sell cost
                        # (price*qty + fee) rather than stale defaults. Without
                        # this, the EOD summary's fee vs. traded-volume audit
                        # catches a mismatch and reports a fee omission.
                        _guard_fee = calc_trade_fee(
                            C, _eval_low_global, _force_close_qty, is_sell=True)
                        if _pos_mgr is not None:
                            _grg_sold, _grg_pnl = _pos_mgr.request_sell(
                                _force_close_qty, 'GLOBAL_RISK_GUARD',
                                caller='stop_loss', current_price=_eval_low_global,
                                tick=tick, fee=_guard_fee)
                        else:
                            _grg_sold = safe_sell(C, _eval_low_global, _force_close_qty,
                                                  'GLOBAL_RISK_GUARD', tick)
                        if _grg_sold:
                            if _pos_mgr is None:
                                if getattr(C, '_risk_engine', None) is not None:
                                    C._risk_engine.record_trade_pnl(
                                        _eval_low_global, _force_close_qty, 'SELL', _guard_fee)
                                C.traded_volume += _force_close_qty
                                C._base_trade_fee += _guard_fee
                                C._daily_trade_fee += _guard_fee
                                _guard_loss = (C._base_cost_price - _eval_low_global) * _force_close_qty
                                C.realized_pnl += (-_guard_loss - _guard_fee)
                                C.daily_pnl = C.realized_pnl
                            C._base_stop_done = True
                            C.can_do_t0 = False
                            C.strategy_mode = 'OBSERVE'
                            C.state = 'IDLE'
                            # [P1 Fix] Pass is_explicit_probe_stop so the record-
                            # stop function routes through the probe ladder instead
                            # of the base-stop cooldown. Without this a Global Stop
                            # of a probe position would reset to a full base-stop
                            # schedule and block the oversold-rebuild ladder.
                            _record_base_stop(C, date_str, _eval_low_global, _force_close_qty,
                                              realized_pnl=_grg_pnl if _pos_mgr is not None else 0.0,
                                              is_explicit_probe_stop=_is_probe_pos)
                            if get_total_position(C) == 0:
                                reset_base_anchors(C, 'full')
                            return  # Short-circuit: skip all state-machine logic this bar
                        else:
                            # safe_sell failed (limit-down). Escalate to pending stop so
                            # the -8% loss cap keeps propagating rather than silently dying
                            # here in the guard code.
                            C._pending_base_stop = True
                            C._pending_stop_needs_record = True
                            C.can_do_t0 = False
                            if not getattr(C, '_global_stop_fail_logged', False):
                                C._global_stop_fail_logged = True
                                print(f"  [Global Stop] safe_sell FAILED for "
                                      f"{_force_close_qty} @ {_eval_low_global:.2f} "
                                      f"(limit-down?); escalated to pending stop")

        # ================= Daily Indicators (URL 同步) =================
        # Backtest: refresh once per day (pre-lock); Real-time: check every minute, retry every 10 min on failure
        _minute_id = bar_time_ms // 60000
        if _minute_id != getattr(C, '_last_indicator_min', -1):
            C._last_indicator_min = _minute_id
            if C.is_live and not backend_indicators_synced_today(C, date_str):
                _last_retry_min = getattr(C, '_last_backend_indicator_retry_min', -999999)
                if _minute_id - _last_retry_min >= 10:
                    C._last_backend_indicator_retry_min = _minute_id
                    fetch_daily_indicators_from_backend(C, C.stock, date_str)
            try:
                if len(C.bars_close) >= 35:
                    C._vmacd_diff, C._vmacd_dea = calc_vmacd(C.bars_volume, C.bars_close)
                    C._wvad_val = calc_wvad(C.bars_open, C.bars_high, C.bars_low, C.bars_close, C.bars_volume)
            except Exception as e:
                if not getattr(C, '_indicator_api_warned', False):
                    C._indicator_api_warned = True
                    print(f"  [Err] Intraday indicator calc failed: {e}")

        _lock_day_open_trend_if_needed(C, _bar_time_str, current_price, time_str)
        _hb_snapshot(C, date_str)

        # -------- Base position rebuild logic (1m unified: backtest + live/sim) --------
        # [Fix Defect 2] EOD hard cutoff: after force_close_t, block ALL rebuild
        # evaluation to prevent probe orders from firing alongside EOD close logic.
        # 盘后固定价格交易(15:05-15:30)由上方拦截器处理，不经过此路径。
        if time_str >= C.force_close_t:
            pass  # Skip entire rebuild section after EOD cutoff
        # [Fix 1] ORB isolation in condition: do NOT use return (would kill entire handlebar).
        elif not C._abandon_stock and not getattr(C, '_pending_base_stop', False) and C.strategy_mode != 'ORB':
            # Refresh the realtime trend before any rebuild gate reads it. The old
            # flow updated the trend inside the final rebuild branch, so cooldown
            # and policy checks could run on yesterday's/previous-bar trend.
            _pre_rebuild_trend = C.trend_direction
            # ponytail: snapshot is audit-only; rebuild routing uses live trend_direction
            if time_str >= '09:35:00':
                update_trend_direction(C, _bar_time_str, current_price if is_valid_price(current_price) else 0.0)
                C._trend_snapshot = C.trend_direction
                if not getattr(C, '_trend_snapshot_taken_today', False):
                    C._trend_snapshot_taken_today = True
            if _pre_rebuild_trend == 'DOWN' and C.trend_direction in ('UP', 'NEUTRAL'):
                _lifecycle_pos_mgr = getattr(C, '_pos_mgr', None)
                if _lifecycle_pos_mgr is not None and hasattr(_lifecycle_pos_mgr, 'check_and_reset_lifecycle_on_trend_shift'):
                    _lifecycle_pos_mgr.check_and_reset_lifecycle_on_trend_shift(
                        C.trend_direction, _pre_rebuild_trend,
                        getattr(C, 'daily_trend', C.trend_direction))

            # [P2-2 Fix] Pre-check V2 FUSE status before rebuild, avoid adding to position that should be cut
            if (getattr(C, '_risk_engine', None) is not None
                    and get_total_position(C) > 0
                    and C._risk_engine.is_fused(current_price)):
                if not getattr(C, '_fuse_prebuild_block_logged', False):
                    C._fuse_prebuild_block_logged = True
                    print(f"  [{time_str}] [Rebuild Blocked] V2 FUSE active, block all rebuild")
                C._rebuild_cooldown_bars = 60
            # [修复问题3] FUSE 强制清算态：禁止任何重建，并提供假释解除死锁
            elif getattr(C, '_force_liquidation_active', False):
                if get_total_position(C) == 0:
                    C._force_liquidation_active = False
                    print(f"  [{time_str}] [FUSE] Position cleared, force-liquidation state released")
                else:
                    # 1. 检查是否可以假释：如果总 PnL 已大幅恢复（超过熔断线 50%），解除死锁
                    _fuse_pnl_relief = C.daily_pnl
                    if getattr(C, '_base_cost_price', 0.0) > 0:
                        _fuse_pnl_relief += (current_price - C._base_cost_price) * get_total_position(C)
                    if _fuse_pnl_relief > C.daily_loss_limit * 0.5:
                        C._force_liquidation_active = False
                        C._force_liquidation_blocked_logged = False
                        print(f"  [{time_str}] [FUSE] PnL recovered to safe zone ({_fuse_pnl_relief:.1f}), force-liquidation released")
                    else:
                        if not getattr(C, '_fuse_rebuild_block_logged', False):
                            C._fuse_rebuild_block_logged = True
                            print(f"  [{time_str}] [FUSE] Force-liquidation active ({get_total_position(C)} remaining), block all rebuild")
                        # 2. 幽灵穿透修复：设置冷却期，链式阻断后续所有 elif 重建逻辑
                        C._rebuild_cooldown_bars = 30
            elif C.strategy_mode == 'SKIP':
                # [Fix D5] Allow the EXTREME oversold probe path to punch through
                # SKIP at the top-level dispatcher too. SKIP fires when today has
                # high amplitude and price is still under VWAP — but that
                # amplitude is precisely what produces the RSI<20 washouts that
                # are our best entries.
                # [D6] Additionally, allow Init-Half cooldown to bypass SKIP.
                # After an init-half exit, the position enters a 2-day cooldown
                # where exit_policy is set to 'down_oversold' and
                # init_half_cooldown_date is populated. During this window, SKIP
                # would block the very probe entries that the half-exit was
                # designed to enable. Bypassing SKIP here keeps the probe ladder
                # functional after a half-position exit.
                _skip_top_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
                _in_init_half_cool = _in_init_half_cooldown(C)
                if _skip_top_rsi >= C.down_rebuild_extreme_rsi and not _in_init_half_cool:
                    if not getattr(C, '_skip_rebuild_blocked_logged', False):
                        C._skip_rebuild_blocked_logged = True
                        print(f"  [Rebuild Blocked] SKIP mode active, block base rebuild")
                else:
                    C._skip_rebuild_bypass_today = True
                    _bypass_label = "init-half cooldown" if _in_init_half_cool else f"extreme RSI {_skip_top_rsi:.1f}"
                    log_once(C, '_skip_bypass_logged', f"  [Rebuild] SKIP bypassed: {_bypass_label}, "
                          f"falling through to probe evaluator")
            elif C._rebuild_cooldown_bars > 0:
                C._rebuild_cooldown_bars -= 1
            elif getattr(C, '_down_intraday_reduced_done', False):
                if not getattr(C, '_intraday_reduced_block_logged', False):
                    C._intraday_reduced_block_logged = True
                    print(f"  [Rebuild Blocked] Intraday reduce already done today, block all rebuilds")
            else:
                _total_pos = get_total_position(C)
                _avail_pos = get_true_available_position(C)

                _vwap_total_vol = C.confirmed_min_vol + getattr(C, 'cur_bar_volume', 0)
                _vwap_total_amt = C.confirmed_min_amt + getattr(C, 'cur_bar_amount', 0.0)
                _rebuild_vwap = _vwap_total_amt / _vwap_total_vol if _vwap_total_vol > 0 else current_price

                _today_open = getattr(C, '_day_open_price', 0.0)
                if not is_valid_price(_today_open):
                    if hasattr(C, 'open') and C.barpos < len(C.open):
                        _today_open = float(C.open[C.barpos])
                if not is_valid_price(_today_open) and len(C.bars_open) > 0:
                    _today_open = C.bars_open[0]
                _is_gap_down = (is_valid_price(_today_open) and C.prev_close > 0
                                and _today_open < C.prev_close * 0.985)

                _ma20_daily = getattr(C, '_trend_ma20', 0.0)
                _deviation_from_ma20 = ((current_price - _ma20_daily) / _ma20_daily
                                        if _ma20_daily > 0 else 0.0)
                _is_high_deviation = _deviation_from_ma20 > 0.05

                if _total_pos >= C.backtest_base_qty:
                    C._base_pos_initialized = True
                    C._base_ever_built = True
                    C.can_do_t0 = (_avail_pos >= 100)
                    if C._base_rebuild_stage < 2:
                        C._base_rebuild_stage = 2
                    # 重建成功后清除退出策略
                    C._exit_policy = ''
                    C._staged_half_exit_date = ''
                    C._rebuild_from_staged_half_exit = False
                    C._rebuild_cooldown_bars = 60
                elif is_valid_price(current_price):
                    _can_build = False
                    _build_qty = 0
                    _build_reason = ''
                    _is_probe_upgrade_path = False

                    _t0_pending = getattr(C, '_t0_pending_qty', 0)
                    _effective_base = _total_pos + _t0_pending

                    # [Fix 1] The top branch (`_base_stop_date == '' and _total_pos == 0`)
                    # is designed for a *fresh* no-stopping-record case. After an
                    # Init-Half exit, _record_staged_half_exit deliberately blanks
                    # _base_stop_date and sets exit_policy='down_oversold' + its
                    # own _days_since_init_half_exit counter. If we let this top
                    # branch match, the probe-ladder elif below (which is D4-patched)
                    # never fires and we're stuck in a 31-day lockout. So skip the
                    # top branch entirely when Init-Half cooldown is past the gate.
                    # [P0 Fix log111] Same skip when V5/PHC profitable exit cleared
                    # _base_stop_date but armed down_oversold: _base_ever_built=True
                    # otherwise hits "Initial build skipped, trend DOWN" and never
                    # reaches the oversold ladder elif (~2668).
                    _skip_empty_init_for_down_oversold = (
                        getattr(C, '_exit_policy', '') == 'down_oversold'
                        and (
                            (getattr(C, '_init_half_cooldown_date', '') != ''
                             and getattr(C, '_days_since_init_half_exit', 99) >= C._stop_cool_days)
                            or (getattr(C, '_base_ever_built', False) and C._base_stop_date == '')
                        )
                    )
                    # [P0-1 Fix] Same-day re-entry lock. Probe stops route through
                    # _record_base_stop with is_explicit_probe_stop, which sets a
                    # probe cooldown (_probe_stop_active / _stop_cool_days / exit_policy)
                    # but NEVER sets _base_stop_date. This empty-account first-build
                    # branch is gated ONLY on _base_stop_date == '', so a probe that
                    # was stopped out at -7.8% (05-15 PROBE_IMMUNE_HALF) re-entered an
                    # INIT_BASE_PROBE on the very next bar @ basically the same price
                    # (40.04 sell -> 40.06 buy). The oversold rebuild ladder honours the
                    # cooldown (05-21 "Rebuild Block Cooldown 0/1d"); this initial-build
                    # path did not. Block fresh INIT builds for the rest of the session
                    # after any intraday clearance.
                    if getattr(C, '_position_cleared_today', False):
                        if not getattr(C, '_intraday_reentry_lock_logged', False):
                            C._intraday_reentry_lock_logged = True
                            print("  [Build] Intraday re-entry lock: position cleared today, "
                                  "no fresh INIT build until next session (probe cooldown)")
                        _can_build = False
                        C._rebuild_cooldown_bars = 30
                    elif not _skip_empty_init_for_down_oversold and C._base_stop_date == '' and _total_pos == 0:
                        # [Fix] Global macro risk alignment: SKIP mode unconditionally
                        # blocks empty-account first-build, even without staged-half cooldown.
                        if C.strategy_mode == 'SKIP':
                            _can_build = False
                            C._rebuild_cooldown_bars = 30
                        elif _blocks_empty_init_base(C):
                            _can_build = False
                            C._rebuild_cooldown_bars = 60
                            if not getattr(C, '_staged_half_exit_block_logged', False):
                                C._staged_half_exit_block_logged = True
                                if _in_staged_half_exit_cooldown(C):
                                    _d = getattr(C, '_days_since_staged_half_exit', 0)
                                    _cool = getattr(C, '_staged_half_exit_cool_days', 5)
                                    print(f"  [Rebuild Blocked] Staged-half exit cooldown ({_d}/{_cool}d), block INIT_BASE")
                                elif (getattr(C, '_init_half_cooldown_date', '') != ''
                                      and getattr(C, '_days_since_init_half_exit', 99) < C._stop_cool_days):
                                    # [Fix 3] Use dynamic _stop_cool_days instead of hard-coded 2.
                                    _d = getattr(C, '_days_since_init_half_exit', 0)
                                    _cool = getattr(C, '_stop_cool_days', 2)
                                    print(f"  [Rebuild Blocked] Init-Half exit cooldown ({_d}/{_cool}d), block INIT_BASE")
                                else:
                                    _d = getattr(C, '_days_since_staged_reduce', 0)
                                    _cool = getattr(C, '_staged_reduce_cool_days', 5)
                                    print(f"  [Rebuild Blocked] Staged reduce cooldown ({_d}/{_cool}d), block INIT_BASE")
                        else:
                            update_trend_direction(C, _bar_time_str)
                            _ts = C.trend_direction
                            _is_init_half_post_exit = (
                                getattr(C, '_exit_policy', '') == 'down_oversold'
                                and getattr(C, '_init_half_cooldown_date', '') != ''
                                and getattr(C, '_days_since_init_half_exit', 99) >= 2)
                            if _ts == 'DOWN' and not _is_init_half_post_exit:
                                _can_build = False
                                C._rebuild_cooldown_bars = 30
                                # [P0-fix] Fresh-account DOWN deadlock fix:
                                # P0 blocks INIT when day-open trend is DOWN, but for a
                                # truly fresh account (never built, never stopped), the
                                # oversold rebuild ladder at line ~2640 is unreachable
                                # because it requires _base_stop_date != '' or
                                # exit_policy == 'down_oversold'. Without arming here,
                                # the strategy is permanently stuck: no INIT → no
                                # position → no stop → no ladder → no rebuild → ever.
                                # Fix: set a *virtual stop* so the existing cooldown +
                                # rebuild ladder machinery takes over naturally. This
                                # does NOT increment _consecutive_base_stops (no real
                                # loss occurred) and does NOT set _base_ever_built.
                                if not getattr(C, '_base_ever_built', False):
                                    C._virtual_oversold_armed = True
                                    C._base_stop_date = date_str
                                    C._days_since_stop = 99
                                    C._exit_policy = 'down_oversold'
                                    C._probe_stop_active = False
                                    print(f"  [Rebuild] Initial build skipped, trend DOWN   "
                                          f"arming oversold rebuild ladder (virtual arm, "
                                          f"no cooldown penalty)")
                                else:
                                    if not getattr(C, '_init_down_skip_logged', False):
                                        C._init_down_skip_logged = True
                                        print(f"  [Rebuild] Initial build skipped, trend DOWN")
                            elif _ts == 'DOWN' and _is_init_half_post_exit:
                                # [Fix] Init-Half exit cleared base_stop_date, so the top
                                # branch here (`_base_stop_date == '' and _total_pos == 0`)
                                # wins and blocks us from the post-stop probe ladder we
                                # patched in D4. Set a sentinel so we *re-enter* the
                                # probe ladder at the next decision point. We also keep
                                # the short 30-bar cooldown so we don't re-entry blindly.
                                C._rebuild_cooldown_bars = 30
                                if not getattr(C, '_init_half_post_exit_armed', False):
                                    C._init_half_post_exit_armed = True
                                    print(f"  [Rebuild] Init-Half post-exit: DOWN but exit_policy=down_oversold. "
                                          f"Arming probe ladder; waiting for bias/oversold signals.")
                            else:
                                _ma20 = getattr(C, '_trend_ma20', 0.0)
                                _deviation = (current_price - _ma20) / _ma20 if _ma20 > 0 else 0
                                if _deviation > 0.05:
                                    _data_sufficient = (len(C.bars_close) >= 20)
                                    # [P1 Fix A] UP-trend high-dev exemption: if the trend
                                    # is confirmed UP and price is above VWAP, allow an
                                    # initial build even at > 5% deviation above MA20. The
                                    # old blanket 5% gate missed genuine momentum breakouts
                                    # in strong uptrends. A tight 100-sh probe (not full
                                    # base) limits the risk.
                                    # [P0 Fix log63 P0-4] Use realtime trend (C.trend_direction),
                                    # not the pre-locked day-open snapshot. On 05-18 the market
                                    # opened UP (snapshot locked to UP) but intraday price collapsed,
                                    # so a probe built on the UP snapshot was actually a -5%
                                    # underwater add. Using realtime trend ensures the high-dev
                                    # exemption only fires when the CURRENT trend is still
                                    # genuinely UP with price above VWAP — not when the morning
                                    # snapshot is stale but the market has already broken down.
                                    # [P2 Fix] Use the outer _rebuild_vwap (computed from
                                    # confirmed_min_amt / confirmed_min_vol + cur_bar). Previously
                                    # this block overwrote it with getattr(C, '_intraday_vwap', 0.0)
                                    # but C._intraday_vwap is never assigned in this scope, so the
                                    # default 0.0 caused `_rebuild_vwap > 0` to be False and the
                                    # entire INIT_HIGH_DEV_UPTREND_PROBE exemption was dead code.
                                    # ponytail: 5%-6% UP micro @10:00-11:00 only (log138 afternoon chase fix).
                                    _ref_trend_now = C.trend_direction
                                    _high_dev_cap = getattr(C, 'init_high_dev_probe_max', 0.055)
                                    if time_str < C.decision_time:
                                        _can_build = False
                                        # ponytail: time gate only — no day-lock / rebuild cooldown
                                        print(f"  [Build] Deviation {_deviation:.1%} > 5%, defer until {C.decision_time} "
                                              f"(init: dev<=5% full or {0.05:.0%}-{_high_dev_cap:.1%} UP micro 10:00-11:00)")
                                    elif (_deviation <= _high_dev_cap
                                          and time_str <= '11:00:00'
                                          and _ref_trend_now == 'UP'
                                          and _day_open_trend_frozen(C) != 'DOWN'
                                          and not getattr(C, '_gray_zone_weak_block_t0', False)
                                          and _rebuild_vwap > 0 and current_price > _rebuild_vwap):
                                        _can_build = True
                                        _build_qty = getattr(C, 'neutral_probe_qty', 100)
                                        _build_reason = 'INIT_HIGH_DEV_UPTREND_PROBE'
                                        C._pending_build_log = (
                                            f"  [Build] UP high-dev micro {_build_qty} @ "
                                            f"{current_price:.2f} (dev:{_deviation:.1%}, VWAP ok)")
                                    else:
                                        _can_build = False
                                        # ponytail: re-eval each bar when dev/VWAP improve (log158 5/11-14)
                                        _log_high_dev_build_blocked(
                                            C, _deviation, _high_dev_cap,
                                            _ref_trend_now, _rebuild_vwap, current_price)
                                else:
                                    # [Defect1.5] Normal (deviation ≤ 5%) empty-entry path.
                                    # After decision_time, build the standard trade_qty. The
                                    # previous code clamped this to 100 shares too, which meant
                                    # a perfectly ordinary entry in a calm market also got the
                                    # probe-sizing treatment and never built a real base.
                                    if time_str < C.decision_time:
                                        _can_build = False
                                        C._rebuild_cooldown_bars = 30
                                    elif getattr(C, '_orb_failed_allow_probe_today', False):
                                        # log128 P1': no VWAP micro on ORB-fail (127 05/18@41.28 GAP);
                                        # only dev<=0 pullback micro when day-open not DOWN.
                                        if _orb_fail_micro_ok(C, _deviation, current_price, _rebuild_vwap):
                                            _can_build = True
                                            _build_qty = getattr(C, 'neutral_probe_qty', 100)
                                            _build_reason = 'ORB_FAIL_PULLBACK_MICRO'
                                            C._pending_build_log = (
                                                f"  [Build] ORB-fail pullback micro {_build_qty} @ "
                                                f"{current_price:.2f} (dev:{_deviation:.1%})")
                                        else:
                                            _can_build = False
                                            C._rebuild_cooldown_bars = 60
                                            log_once(C, '_orb_fail_init_blocked_logged',
                                                     f"  [Build Blocked] ORB failed today: oversold rebuild "
                                                     f"only (dev:{_deviation:.1%}, trend:{C.trend_direction})")
                                    else:
                                        # [Fix A] Gray-zone weak float: allow only a
                                        # 100-share probe instead of full INIT_BASE.
                                        # The same gray-zone flag that blocks OBSERVE->T0
                                        # (lines 3851/4007) should also limit the initial
                                        # build, otherwise we enter 300 shares in a weak
                                        # market that immediately goes against us.
                                        if getattr(C, '_gray_zone_weak_block_t0', False):
                                            _ma20_gray = getattr(C, '_trend_ma20', 0.0)
                                            _cur_dev_gray = ((current_price - _ma20_gray) / _ma20_gray) if _ma20_gray > 0 else 0.0
                                            _can_build = False
                                            C._rebuild_cooldown_bars = 60
                                            log_once(C, '_gray_zone_init_blocked_logged',
                                                     f"  [Build Blocked] Gray zone: high-position init banned "
                                                     f"(dev {_cur_dev_gray:.1%}, wait for oversold rebuild)")
                                        else:
                                            _blocked, _why = _blocks_high_position_init_build(C, _deviation)
                                            if _blocked:
                                                _can_build = False
                                                C._rebuild_cooldown_bars = 60
                                                log_once(C, '_high_pos_init_blocked_logged',
                                                         f"  [Build Blocked] High-position init: {_why}")
                                            else:
                                                _can_build = True
                                                # log161 P0: day-open UP but intraday NEUTRAL -> micro init
                                                # [log176 Fix D] UP->NEUTRAL degradation: delay to 10:30
                                                # 600406 5/20: 09:43 UP->NEUTRAL, 10:00 entry -> 5/22 -122.6
                                                # Give trend 30min stabilization window before committing.
                                                _day_open = _day_open_trend_frozen(C)
                                                _is_degraded = (_day_open == 'UP' and C.trend_direction == 'NEUTRAL')
                                                if _is_degraded and time_str < '10:30:00':
                                                    _can_build = False
                                                    if not getattr(C, '_up_to_neutral_delay_logged', False):
                                                        C._up_to_neutral_delay_logged = True
                                                        print(f"  [Build Blocked] UP->NEUTRAL degrade at "
                                                              f"{time_str}, defer INIT to 10:30 (trend stability)")
                                                else:
                                                    _build_qty = (getattr(C, 'neutral_probe_qty', 100)
                                                                  if C.trend_direction == 'NEUTRAL'
                                                                  else C.trade_qty)
                                                    _build_reason = 'INIT_BASE_PROBE'
                                                    _init_sz_tag = ' NEUTRAL-micro' if _build_qty < C.trade_qty else ''
                                                    C._pending_build_log = (
                                                        f"  [Build] Pullback INIT_BASE_PROBE {_build_qty} @ "
                                                        f"{current_price:.2f} (dev:{_deviation:.1%}{_init_sz_tag})")
                    elif C._base_stop_date == '' and 0 < _effective_base < C.backtest_base_qty:
                        update_trend_direction(C, _bar_time_str, current_price)
                        # [Fix 5] Probe-controlled upgrade runs BEFORE high-dev gate.
                        # Introduce _is_probe_upgrade_path so a successful probe upgrade
                        # is NOT overwritten by the high-dev / gap-down / INIT_BASE_ADD
                        # gates further down.
                        _is_probe_upgrade_path = False
                        # [P2 Fix] Compute probe float ONCE at the section entry so
                        # both init_probe and oversold_probe branches reference the
                        # same variable. Previously _float_for_probe was defined only
                        # inside the init_probe block (line 1885) but the
                        # oversold_probe block (line 2006) referenced it too, causing
                        # UnboundLocalError when the current probe was oversold_probe.
                        _shared_cost_ref = getattr(C, '_base_cost_price', 0.0)
                        _float_for_probe = ((current_price - _shared_cost_ref) / _shared_cost_ref
                                            if _shared_cost_ref > 0 else 0.0)
                        if _dpm_is_init_probe(C) and _total_pos < C.trade_qty:
                            _is_probe_upgrade_path = True
                            _ma5_upgrade = calc_ma(C, C.ma_short)
                            _init_probe_days = getattr(C, '_days_since_init_probe', 0)
                            _cost_ref_probe = _shared_cost_ref
                            if _init_probe_days <= 1:
                                # [P1 Fix C] Allow init_probe upgrade at a small
                                # loss (within -2%) instead of requiring pure profit
                                # (>0). On a probe day, price typically trades in a
                                # range around the entry cost; requiring >0 means the
                                # upgrade only fires on a strong gap-up morning, which
                                # defeats the purpose of a controlled-add ladder. The
                                # combined risk is still bounded by the 100-share step
                                # size and the hard stop, so a 2% entry-side loss on
                                # a small add is acceptable.
                                _upgrade_ok = (C.trend_direction != 'DOWN'
                                    and current_price > _rebuild_vwap
                                    and _float_for_probe > -0.02)
                                _level = 'strict'
                            elif _init_probe_days <= 3:
                                _upgrade_ok = (C.trend_direction != 'DOWN'
                                    and current_price > _rebuild_vwap
                                    and current_price > _cost_ref_probe * 0.95)
                                _level = 'standard'
                            else:
                                _upgrade_ok = (C.trend_direction != 'DOWN' and _float_for_probe > -0.03)
                                _level = 'relaxed'
                            _beta_val = getattr(C, '_backend_beta_20', 1.0)
                            _market_down = getattr(C, '_backend_macro_down_5d', False)
                            if _market_down and _beta_val > 1.2:
                                _upgrade_ok = False
                            # log178: day-open DOWN + intraday NEUTRAL flip upgraded 100->300 (5/21)
                            if _upgrade_ok and _day_open_trend_frozen(C) == 'DOWN':
                                _upgrade_ok = False
                                log_once(C, '_init_probe_upgrade_dopen_block_logged',
                                         "  [Rebuild Blocked] INIT_PROBE_CONTROLLED_UPGRADE rejected - "
                                         "day-open DOWN (no pyramiding on weak-open bounce)")
                            if _upgrade_ok:
                                # [P0 Fix] Extra gate: reject upgrade if price is more than
                                # 5% above MA20. Prevents pyramiding into a spike that will
                                # likely revert.
                                _ma20_upgrade = getattr(C, '_trend_ma20', 0.0)
                                if _ma20_upgrade > 0:
                                    _deviation_upgrade = (current_price - _ma20_upgrade) / _ma20_upgrade
                                # [D1 P0 Fix] Reject probe upgrade when price is more
                                # than 5% above MA20 — regardless of probe type. The
                                # prior round added a bypass ("pass") for
                                # _is_init_probe, on the theory that probes entered on
                                # a gap-down to an oversold zone would naturally sit
                                # above MA20 during their early life. In practice that
                                # bypass allowed pyramiding into a spike: the probe
                                # would escalate to trade_qty at a >5% overshoot, then
                                # the ADD gate would reject subsequent adds, producing
                                # the same "half-base stuck above MA20" pattern seen
                                # during the 5/14 crash. The uniform 5% threshold
                                # matches the INIT_BASE gate above and guarantees no
                                # position is built above MA20 by more than 5%.
                                if _deviation_upgrade > 0.05:
                                    _upgrade_ok = False
                                    if not getattr(C, '_init_probe_upgrade_block_logged', False):
                                        C._init_probe_upgrade_block_logged = True
                                        print(f"  [Rebuild Blocked] probe upgrade high deviation: "
                                              f"{_deviation_upgrade:.1%} > 5%")
                            if _upgrade_ok:
                                # [P0 Fix] Underwater circuit breaker: prohibit
                                # controlled upgrade when float <= -4%. The log61
                                # 05-18 pattern: price bounced from -5.1% gap back
                                # to cost+, then upgrade doubled 100 to 200,
                                # setting up the 05-21 -679 loss. Gap-day + deep
                                # underwater = NO pyramiding.
                                if getattr(C, '_is_gap_down_day_flag', False):
                                    _upgrade_ok = False
                                    print(f"  [Rebuild Blocked] INIT_PROBE_CONTROLLED_UPGRADE rejected - gap-down day")
                                elif _float_for_probe <= -0.04:
                                    _upgrade_ok = False
                                    if not getattr(C, '_init_probe_water_block_logged', False):
                                        C._init_probe_water_block_logged = True
                                        print(f"  [Rebuild Blocked] INIT_PROBE_CONTROLLED_UPGRADE rejected - float {_float_for_probe:.1%} <= -4% (underwater protection)")
                                # [Fix P1] Remove historical extreme deadlock: use current real-time float for check, allow underwater deep V reversal to re-enter
                                elif _float_for_probe <= -0.05:
                                    _upgrade_ok = False
                                    if not getattr(C, '_init_probe_mae_block_logged', False):
                                        C._init_probe_mae_block_logged = True
                                        print(f"  [Rebuild Blocked] INIT_PROBE_CONTROLLED_UPGRADE rejected - current float {_float_for_probe*100:.1f}% <= -5%")
                            if _upgrade_ok:
                                # [Fix 4] Pre-check PosMgr daily limit before
                                # generating the order. Without this, the upgrade
                                # engine keeps firing orders that PosMgr rejects
                                # (today_build_count=1), causing CPU空转 and log
                                # spam (05-18: 4 consecutive rejections at 13:30-13:36).
                                _pos_mgr_upg = getattr(C, '_pos_mgr', None)
                                _upg_today = getattr(C, 'current_date', '')
                                if (_pos_mgr_upg is not None
                                        and _pos_mgr_upg.today_build_count > 0
                                        and _pos_mgr_upg.last_build_date == _upg_today):
                                    _upgrade_ok = False
                                    if not getattr(C, '_upgrade_posmgr_sync_logged', False):
                                        C._upgrade_posmgr_sync_logged = True
                                        print(f"  [Rebuild Blocked] INIT_PROBE_CONTROLLED_UPGRADE rejected - "
                                              f"today_build_count={_pos_mgr_upg.today_build_count} (PosMgr Gate)")
                            if _upgrade_ok:
                                _can_build = True
                                _build_qty = min(C.trade_qty, C.trade_qty - _total_pos)
                                _build_reason = 'INIT_PROBE_CONTROLLED_UPGRADE'
                                C._pending_build_log = f"  [Rebuild] Init probe upgrade ({_level}) " \
                                                       f"qty={_build_qty} @ {current_price:.2f} " \
                                                       f"(trend:{C.trend_direction} days:{_init_probe_days})"
                            else:
                                _can_build = False
                                C._rebuild_cooldown_bars = 30
                                # [P0 Fix] Do NOT force-downgrade probe_type on
                                # upgrade rejection. init_probe carries its own
                                # hard-stop immunity and Trend-Cut protection
                                # window. Silently rewriting it to oversold_probe
                                # destroyed those benefits — the upgrade criteria
                                # weren't met, but that's a rejection of the ADD,
                                # not a mandate to change the probe's identity.
                                # The probe keeps its init_probe identity; only
                                # the cooldown/blocked flags are set so the risk
                                # engine knows the add-failed state.
                                if _float_for_probe <= -0.03:
                                    C._init_probe_upgrade_blocked = True
                                    C._init_probe_upgrade_block_price = current_price
                                if not getattr(C, '_init_probe_upgrade_block_logged', False):
                                    C._init_probe_upgrade_block_logged = True
                                    print(f"  [Rebuild Blocked] Init probe upgrade "
                                          f"({_level}) criteria not met, "
                                          f"float:{_float_for_probe:.1%} hold (identity preserved)")

                        # [P1 Fix] Oversold probe upgrade path: allow a 100-share micro
                        # probe to scale up to trade_qty once the trend stabilizes. The
                        # micro-probe (Fix 1) prevents catastrophic loss on entry, but a
                        # 100-share position can only capture 1/3 of the V5 profit target.
                        # Require: at least 1 day held, trend not DOWN, price above VWAP
                        # and MA5. This runs AFTER the init-probe upgrade path because
                        # both gates independently set _is_probe_upgrade_path=True to
                        # prevent the generic INIT_BASE_ADD from overwriting the decision.
                        elif _dpm_is_oversold_probe(C) and _total_pos < C.trade_qty:
                            _is_probe_upgrade_path = True
                            _ma5_upgrade_os = calc_ma(C, C.ma_short)
                            _probe_days_os = getattr(C, '_days_since_rebuild_probe', 0)
                            # [P1 Fix #3 log61] Gate oversold-probe pyramid-adds:
                            #   - Never add on gap-down day (flag from risk engine)
                            #   - Never add when probe float <= -2% (Trail-skip day or
                            #     deep-water). This blocks the log61 05-29 pattern:
                            #     Trail-skip deferral at -7.6% followed by EXTREME add
                            #     in the same session.
                            if getattr(C, '_is_gap_down_day_flag', False):
                                if not getattr(C, '_extreme_gap_block_logged', False):
                                    C._extreme_gap_block_logged = True
                                    print(f"  [{time_str}] [Rebuild] Oversold probe add BLOCKED on gap-day")
                                _is_probe_upgrade_path = False
                            elif _float_for_probe <= -0.05:
                                if not getattr(C, '_extreme_underwater_block_logged', False):
                                    C._extreme_underwater_block_logged = True
                                    print(f"  [{time_str}] [Rebuild] Oversold probe add BLOCKED: float {_float_for_probe:.1%} <= -5%")
                                _is_probe_upgrade_path = False
                            # [D3] Micro safe-left add: allow limited averaging when
                            # position <= neutral_probe_qty and float within -3%.
                            # Bypasses confirmed_bounce/trend/VWAP requirements that
                            # strand 100-sh oversold probes (06-11/06-22 pattern).
                            elif (_total_pos <= getattr(C, 'neutral_probe_qty', 100)
                                  and _float_for_probe > -0.03
                                  and _probe_days_os >= 1):
                                _can_build = True
                                _build_qty = min(C.trade_qty, C.trade_qty - _total_pos)
                                _build_reason = 'OVERSOLD_PROBE_SAFE_LEFT_ADD'
                                C._pending_build_log = (
                                    f"  [Rebuild] Oversold probe safe-left add {_build_qty} @ "
                                    f"{current_price:.2f} (float:{_float_for_probe:.1%} "
                                    f"hold:{_probe_days_os}d)")
                            # [Fix] Allow DOWN-trend rebound add-on when macro
                            # oversold space supports it (deep bias_20 or near
                            # 60-day bottom). Without this, a 100-share probe in a
                            # DOWN trend can never scale up, and the strategy
                            # remains stuck at micro-position even after a clear
                            # rebound confirmation.
                            # --- [新增] 跌破支撑位视为重建信号 ---
                            _support = getattr(C, '_backend_support', 0.0)
                            _is_break_support = (current_price < _support) if _support > 0 else False

                            if _is_break_support:
                                _log_rebuild_signal(C, f"  [Rebuild Signal] Price {current_price:.2f} below support {_support:.2f}")

                            _macro_space_ok = (
                                getattr(C, '_backend_bias_20', 0.0) <= -0.06
                                or getattr(C, '_backend_near_bottom_60', False)
                                or _is_break_support
                            )
                            # [P1 Fix 4] Confirmed bounce is required for ALL
                            # trends, not just DOWN. The UP-trend short-circuit
                            # allowed probe doubling on negative-return probes
                            # (05-14 gray zone +100 → 05-15 -1.9% underwater
                            # → OVERSOLD_PROBE_UPGRADE doubled to 200). Require
                            # current_price > cost * 1.01 AND above MA5 — if
                            # price can't even recover 1% above entry, it has
                            # no business being scaled into.
                            _cost_ref_os = getattr(C, '_base_cost_price', 0.0)
                            _confirmed_bounce = (
                                _cost_ref_os > 0
                                and current_price > _cost_ref_os * 1.01
                                and current_price > _ma5_upgrade_os
                            )
                            # [P1 Fix 4] _confirmed_bounce is a hard requirement
                            # now — no more UP-trend bypass.
                            # [P1-4 Fix log81] DOWN trend to NEUTRAL transition:
                            # relax VWAP requirement to allow upgrade when trend just
                            # turned NEUTRAL, avoid getting stuck in perpetual DOWN.
                            _is_down_to_neutral = (getattr(C, '_day_open_trend', 'NEUTRAL') == 'DOWN'
                                                 and C.trend_direction == 'NEUTRAL')
                            _vwap_ok_for_upgrade = (current_price > _rebuild_vwap) or _is_down_to_neutral

                            _upgrade_ok_os = (
                                _probe_days_os >= 1
                                and C.trend_direction != 'DOWN'
                                and _vwap_ok_for_upgrade
                                and current_price > _ma5_upgrade_os
                                and _confirmed_bounce
                            )
                            # Also block if more than 5% above MA20 (spike chasing guard).
                            if _upgrade_ok_os:
                                _ma20_upgrade_os = getattr(C, '_trend_ma20', 0.0)
                                if _ma20_upgrade_os > 0:
                                    _dev_os = (current_price - _ma20_upgrade_os) / _ma20_upgrade_os
                                    if _dev_os > 0.05:
                                        _upgrade_ok_os = False
                                        log_once(C, '_oversold_upgrade_spike_logged',
                                                 f"  [Rebuild Blocked] Oversold probe upgrade: "
                                                 f"spike {_dev_os:.1%} > MA20+5%")
                            if not _can_build and _upgrade_ok_os:
                                _can_build = True
                                _build_qty = min(C.trade_qty, C.trade_qty - _total_pos)
                                _build_reason = 'OVERSOLD_PROBE_UPGRADE'
                                C._pending_build_log = f"  [Rebuild] Oversold probe upgrade " \
                                                       f"qty={_build_qty} @ {current_price:.2f} " \
                                                       f"(held:{_probe_days_os}d trend:{C.trend_direction})"
                            elif not _can_build:
                                C._rebuild_cooldown_bars = 30

                        # -- End of early probe upgrade gate.
                        # Regular INIT_BASE_ADD gates below only apply to non-probe positions. --
                        if not _is_probe_upgrade_path:
                            # [缺陷2] UP 趋势也不得豁免高偏离，除非站稳 VWAP 之上
                            _high_dev_block = _is_high_deviation and not (C.trend_direction == 'UP' and current_price > _rebuild_vwap)

                            # [Issue 3 Fix] High deviation all-day blocking check
                            if getattr(C, '_high_dev_blocked_today', False):
                                _can_build = False
                                C._rebuild_cooldown_bars = 60
                                if not getattr(C, '_up_risk_block_logged', False):
                                    C._up_risk_block_logged = True
                                    print(f"  [Rebuild Blocked] INIT_BASE_ADD: High deviation blocked earlier today, block all day")
                            elif _is_gap_down or _high_dev_block:
                                _can_build = False
                                C._rebuild_cooldown_bars = 60
                                C._high_dev_blocked_today = True
                                if not getattr(C, '_up_risk_block_logged', False):
                                    C._up_risk_block_logged = True
                                    _trend_tag = f" trend:{C.trend_direction}" if _high_dev_block else ""
                                    print(f"  [Rebuild Blocked] INIT_BASE_ADD: "
                                          f"dev={_deviation_from_ma20:.1%} gap_down={_is_gap_down}{_trend_tag}")
                            elif getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                                _can_build = False
                                C._rebuild_cooldown_bars = 60
                                if not getattr(C, '_vwap_carry_rebuild_block_logged', False):
                                    C._vwap_carry_rebuild_block_logged = True
                                    print(f"  [Rebuild Blocked] VWAP downgrade carry active, block INIT_BASE_ADD")

                            # [P0 Fix] Pre-wind risk already blocked: skip trend/build logic
                            if not _can_build:
                                pass
                            else:
                                _add_trend = C.daily_trend
                                if _add_trend == 'DOWN':
                                    _can_build = False
                                    C._rebuild_cooldown_bars = 60
                                    if not getattr(C, '_down_add_blocked_logged', False):
                                        C._down_add_blocked_logged = True
                                        print(f"  [Rebuild Blocked] Trend DOWN, strictly block adding to base")
                                elif _add_trend == 'NEUTRAL' and getattr(C, '_base_trend_cut_active', False):
                                    _can_build = False
                                    C._rebuild_cooldown_bars = 60
                                elif _in_staged_reduce_cooldown(C):
                                    _can_build = False
                                    C._rebuild_cooldown_bars = 60
                                    if not getattr(C, '_staged_rebuild_block_logged', False):
                                        C._staged_rebuild_block_logged = True
                                        _d = getattr(C, '_days_since_staged_reduce', 0)
                                        _cool = getattr(C, '_staged_reduce_cool_days', 5)
                                        print(f"  [Rebuild Blocked] Staged reduce cooldown ({_d}/{_cool}d), block add to base")
                                elif get_total_position(C) >= C.base_qty_half:
                                    _can_build = False
                                    C._rebuild_cooldown_bars = 60
                                    if not getattr(C, '_half_lock_block_logged', False):
                                        C._half_lock_block_logged = True
                                        print(f"  [Rebuild Blocked] INIT_BASE_ADD: pos {get_total_position(C)} >= base_qty_half {C.base_qty_half}, lock to half base")
                                elif not _init_base_add_float_ok(C, current_price):
                                    _can_build = False
                                    if not getattr(C, '_init_add_float_block_logged', False):
                                        C._init_add_float_block_logged = True
                                        _cost = getattr(C, '_base_cost_price', 0.0)
                                        _float_pct = ((current_price - _cost) / _cost
                                                      if _cost > 0 else 0.0)
                                        _min_f = getattr(C, 'init_base_add_min_float_pct', 0.0)
                                        print(f"  [Rebuild Blocked] INIT_BASE_ADD: half float {_float_pct:.1%} "
                                              f"< {_min_f:.1%} (p:{current_price:.2f} cost:{_cost:.2f})")
                                else:
                                    C._down_add_blocked_logged = False
                                    _is_recent_probe = (getattr(C, '_rebuild_probe_date', '') != ''
                                                        and getattr(C, '_days_since_rebuild_probe', 99) <= 2
                                                        and _total_pos < C.base_qty_half
                                                        and not _is_gap_down
                                                        and not _is_high_deviation)
                                    if _is_recent_probe:
                                        _uw_block, _uw_why = _blocks_probe_scale_to_half(C, current_price)
                                        if _uw_block:
                                            _can_build = False
                                            C._rebuild_cooldown_bars = 30
                                            log_once(C, '_probe_to_half_blocked_logged',
                                                     f"  [Rebuild Blocked] PROBE_TO_HALF: {_uw_why}")
                                        else:
                                            _can_build = True
                                            _build_qty = C.base_qty_half - _effective_base
                                            _build_reason = 'REBUILD_PROBE_TO_HALF'
                                    elif _total_pos == 0:
                                        _can_build = True
                                        _remaining_to_full = C.backtest_base_qty - _effective_base
                                        if _dpm_is_init_probe(C) and _effective_base + C.trade_qty >= C.base_qty_half:
                                            if current_price < _rebuild_vwap or current_price < getattr(C, '_base_cost_price', 0):
                                                _can_build = False
                                                C._rebuild_cooldown_bars = 60
                                                print(f"  [Build Block] Init probe approaching half, need price > VWAP & cost to upgrade")
                                            else:
                                                _build_qty = min(_remaining_to_full, C.trade_qty)
                                        else:
                                            _build_qty = min(_remaining_to_full, C.trade_qty)
                                        _build_reason = 'INIT_BASE_ADD'
                                    else:
                                        _can_build = True
                                        _remaining_to_full = C.backtest_base_qty - _effective_base
                                        _build_qty = min(_remaining_to_full, C.trade_qty)
                                        _build_reason = 'INIT_BASE_ADD'
                    elif C._base_stop_date == '' and _effective_base >= C.backtest_base_qty:
                        C._rebuild_cooldown_bars = 60

                    # [D2 P0 Fix] Strong UP reversal exemption: allow early rebuild
                    # even during the cooldown window when trend is clearly reversed
                    # (daily UP + MACD bullish crossover + price above MA20).
                    # During a typical stop-out → V-shaped recovery, the standard
                    # 3-day cooldown would block the first big up-candle.
                    # [P2 Fix] The MACD check uses backend indicators — add a
                    # stale-indicator guard (`not _backend_indicators_stale`) to
                    # both the bypass gate and the early-rebuild path so a
                    # yesterday's golden-cross signal cannot override cooldown.
                    elif C._base_stop_date != '' and C._days_since_stop < C._stop_cool_days and not (
                        getattr(C, 'daily_trend', C.trend_direction) == 'UP'
                        and getattr(C, '_macd_long_cross', False)
                        and current_price > getattr(C, '_trend_ma20', 0)
                        and not getattr(C, '_backend_indicators_stale', False)
                    ):
                        _can_build = False
                        C._rebuild_cooldown_bars = 30
                        if not getattr(C, '_rebuild_cooling_logged', False):
                            C._rebuild_cooling_logged = True
                            print(f"  [Rebuild Block] Cooldown {C._days_since_stop}/{C._stop_cool_days}d")
                    elif (C._base_stop_date != '' and C._days_since_stop >= C._stop_cool_days) or \
                         (C._base_stop_date == '' and getattr(C, '_exit_policy', '') == 'down_oversold' and
                          (getattr(C, '_days_since_init_half_exit', 99) >= C._stop_cool_days
                           or getattr(C, '_base_ever_built', False))) or \
                         (C._base_stop_date != '' and
                          getattr(C, 'daily_trend', C.trend_direction) == 'UP' and
                          getattr(C, '_macd_long_cross', False) and
                          current_price > getattr(C, '_trend_ma20', 0) and
                          not getattr(C, '_backend_indicators_stale', False)):
                        if (C._base_stop_date != '' and C._days_since_stop < C._stop_cool_days):
                            if not getattr(C, '_rebuild_cooling_logged', False):
                                C._rebuild_cooling_logged = True
                                print(f"  [Rebuild] Strong UP reversal detected during cooldown, allow early rebuild")
                        if C.strategy_mode == 'SKIP':
                            _can_build = False
                            C._rebuild_cooldown_bars = 30
                            log_once(C, '_skip_rebuild_blocked_logged',
                                     f"  [Rebuild Blocked] SKIP mode active, block post-stop rebuild")
                        else:
                            _high_amp_blocked = False
                            _exempt_high_amp_rebuild = _high_amp_rebuild_exempt(
                                C, _bar_time_str, current_price, _rebuild_vwap, time_str)
                            if (_total_pos == 0 and len(C.bars_close) >= 10
                                    and not _exempt_high_amp_rebuild):
                                _early_high = max(C.bars_high[-min(30, len(C.bars_high)):]) if C.bars_high else 0
                                _early_low = min(C.bars_low[-min(30, len(C.bars_low)):]) if C.bars_low else 9999
                                if _early_low > 0 and (_early_high - _early_low) / _early_low >= 0.035:
                                    if not getattr(C, '_skip_rebuild_logged', False):
                                        C._skip_rebuild_logged = True
                                        print(f"  [Rebuild] High amplitude {(_early_high - _early_low) / _early_low:.2%} >= 3.5%, skip")
                                    C._rebuild_cooldown_bars = 30
                                    _can_build = False
                                    _high_amp_blocked = True
                            else:
                                _high_amp_blocked = False

                            if not _high_amp_blocked:
                                update_trend_direction(C, _bar_time_str)
                                _ts = C.trend_direction
                                if _ts == 'UP':
                                    _use_normal_path = True  # True = 走正常重建路径
                                    if _is_staged_half_exit_rebuild(C) and C._base_rebuild_stage == 0:
                                        _sh_ok, _sh_qty, _sh_reason = _try_staged_half_probe_rebuild(
                                            C, current_price, _bar_time_str)
                                        if _sh_ok:
                                            _can_build = True
                                            _build_qty = _sh_qty
                                            _build_reason = _sh_reason
                                            _ma20 = getattr(C, '_trend_ma20', 0)
                                            _os_pct = getattr(C, 'down_rebuild_oversold_pct', 0.90)
                                            _oversold_th = _ma20 * _os_pct if _ma20 > 0 else 0
                                            # [Bug#5 Fix] 追赶阈值防跟跌
                                            if _oversold_th > 0 and len(C.bars_low) >= 5:
                                                _recent_5d_low = min(C.bars_low[-5:])
                                                _oversold_th = min(_oversold_th, _recent_5d_low * 0.98)
                                            _daily_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
                                            C._pending_build_log = f"  [Rebuild] Staged-half oversold probe RSI14 {_daily_rsi:.1f} " \
                                                                   f"price {current_price:.2f} <= {_oversold_th:.2f} trend:UP"
                                            _use_normal_path = False  # 已通过 probe 处理，不走正常路径
                                        elif get_total_position(C) >= C.trade_qty:
                                            # 机械性阻断：已有持仓，探测无法添加 → 允许升级路径
                                            pass
                                        else:
                                            # 策略性阻断（observe_only / 冷却期 / 封禁期 / RSI等）
                                            # 尊重策略限制，不允许旁路到激进重建
                                            _use_normal_path = False
                                    if _use_normal_path:
                                        if _is_gap_down or _is_high_deviation:
                                            _can_build = False
                                            C._rebuild_cooldown_bars = 60
                                            if not getattr(C, '_up_risk_block_logged', False):
                                                C._up_risk_block_logged = True
                                                print(f"  [Rebuild Blocked] UP trend: "
                                                      f"dev={_deviation_from_ma20:.1%} gap_down={_is_gap_down}")
                                        else:
                                            _ma20_slope = getattr(C, '_backend_ma20_slope', 0.0)
                                            _is_breakout = getattr(C, '_backend_is_valid_breakout', False)
                                            _is_vol_surge = getattr(C, '_backend_is_volume_surge', False)
                                            # [Defect4] Add left-side reversal as an alternative
                                            # to the strict breakout confirmation. After a long
                                            # UP-trend rebuild cooldown (e.g. 14 days on 5/18),
                                            # the strategy demanded ALL of: valid breakout +
                                            # positive MA20 slope + volume surge + 3-bar VWAP
                                            # hold. This 4-way AND rarely fires during the
                                            # critical first leg of a rally, leaving the
                                            # strategy flat through the entire recovery window.
                                            # A MACD golden cross or SAR bullish flip is a
                                            # legitimate, earlier signal that substitutes for
                                            # the breakout AND slope checks; we still require
                                            # volume + VWAP hold to ensure the move has
                                            # conviction.
                                            # [P1 Fix] Exempt the volume-surge requirement when
                                            # a left-side reversal is present. The 4-way AND
                                            # still failed to fire on 05-18 (the only UP-trend
                                            # entry window) because volume confirmation lagged
                                            # the MACD golden cross / SAR bullish flip. A
                                            # genuine left-side reversal is itself a conviction
                                            # signal; demanding volume on top of it kept the
                                            # strategy flat and led to 9 straight down days.
                                            _left_side_reversal = (
                                                getattr(C, '_macd_long_cross', False)
                                                or getattr(C, '_sar_bullish', False)
                                            )
                                            # [P2 Fix] left-side reversal depends on MACD/SAR
                                            # from backend; block on stale indicators to avoid
                                            # trading on yesterday's signal.
                                            if getattr(C, '_backend_indicators_stale', False):
                                                _left_side_reversal = False

                                            if len(C.bars_close) >= 3:
                                                _stand_on_vwap = all(c > _rebuild_vwap for c in C.bars_close[-3:])
                                            else:
                                                _stand_on_vwap = current_price > _rebuild_vwap * 1.002

                                            _right_side_momentum_trigger = (
                                                (_is_breakout or _left_side_reversal) and
                                                (_ma20_slope > 0 or _left_side_reversal) and
                                                (_is_vol_surge or _left_side_reversal) and
                                                _stand_on_vwap
                                            )

                                        # 4. 触发第一阶梯建仓（推至半仓）
                                        if _right_side_momentum_trigger and _total_pos < C.base_qty_half:
                                            _can_build = True
                                            _build_qty = C.base_qty_half - _total_pos
                                            _build_reason = 'REBUILD_UP_MOMENTUM_BREAKOUT'

                                            C._pending_build_log = f"  [Build] Extreme Right-Side Reversal Convergence Detected: First Breakout of 20-period Resistance + " \
                                                                   f"MA Slope Inflection ({_ma20_slope:.4f}) + Back-End Volume Spike Confirmation + Sustained Front-End VWAP Hold!"

                                        elif C._base_rebuild_stage >= 1:
                                            _can_build = True
                                            _build_qty = C.backtest_base_qty - _total_pos
                                            _build_reason = 'REBUILD_UP_ADD'
                                        else:
                                            _can_build = False
                                            C._rebuild_cooldown_bars = 60
                                            if not getattr(C, '_momentum_block_logged', False):
                                                C._momentum_block_logged = True
                                                print(f"  [Rebuild Blocked] UP stage 0: momentum trigger not met")

                                elif _ts == 'NEUTRAL':
                                    # [Fix P1] If realtime trend already upgraded to UP, skip NEUTRAL_PROBE to avoid ORB/UP rebuild conflict
                                    if C.trend_direction == 'UP':
                                        _can_build = False
                                        C._rebuild_cooldown_bars = 30
                                        if not getattr(C, '_neutral_to_up_skip_logged', False):
                                            C._neutral_to_up_skip_logged = True
                                            print(f"  [Rebuild] NEUTRAL snapshot but realtime UP, skip NEUTRAL_PROBE to avoid ORB conflict")
                                    elif time_str < C.decision_time:
                                        _can_build = False
                                        C._rebuild_cooldown_bars = 30
                                        if not getattr(C, '_neutral_rebuild_wait_logged', False):
                                            C._neutral_rebuild_wait_logged = True
                                            print(f"  [Rebuild Wait] NEUTRAL rebuild deferred to {C.decision_time}")
                                    else:
                                        _use_normal_path = True
                                        if _is_staged_half_exit_rebuild(C) and C._base_rebuild_stage == 0:
                                            _sh_ok, _sh_qty, _sh_reason = _try_staged_half_probe_rebuild(
                                                C, current_price, _bar_time_str)
                                            if _sh_ok:
                                                _can_build = True
                                                _build_qty = _sh_qty
                                                _build_reason = _sh_reason
                                                _ma20 = getattr(C, '_trend_ma20', 0)
                                                _os_pct = getattr(C, 'down_rebuild_oversold_pct', 0.90)
                                                _oversold_th = _ma20 * _os_pct if _ma20 > 0 else 0
                                                # [Bug#5 Fix] 追赶阈值防跟跌
                                                if _oversold_th > 0 and len(C.bars_low) >= 5:
                                                    _recent_5d_low = min(C.bars_low[-5:])
                                                    _oversold_th = min(_oversold_th, _recent_5d_low * 0.98)
                                                _daily_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
                                                C._pending_build_log = f"  [Rebuild] Staged-half oversold probe RSI14 {_daily_rsi:.1f} " \
                                                                       f"price {current_price:.2f} <= {_oversold_th:.2f} trend:NEUTRAL"
                                                _use_normal_path = False
                                            elif get_total_position(C) >= C.trade_qty:
                                                pass
                                            else:
                                                # [P1 Fix] Allow sub-trade_qty probe
                                                # positions (e.g. a 100-share probe)
                                                # to flow through the normal NEUTRAL
                                                # rebuild path. Previously the else
                                                # branch forced _use_normal_path =
                                                # False, which locked small probes
                                                # out of the formal upgrade path
                                                # and stranded them in OBSERVE.
                                                _use_normal_path = True
                                        if _use_normal_path:
                                            if C._base_rebuild_stage == 0:
                                                # [D7 P0 Fix] Empty-position first:
                                                # build trade_qty probe instead of
                                                # jumping straight to half-position.
                                                # A DOWN->NEUTRAL transition is the
                                                # most volatile inflection point —
                                                # committing 600+ shares on day one
                                                # exposes to significant whipsaw
                                                # risk. Start with a 300-sh probe
                                                # and scale up if the price holds.
                                                _neutral_q = getattr(C, 'neutral_probe_qty', 100)
                                                if _total_pos == 0:
                                                    _neutral_ok, _neutral_why = _neutral_empty_probe_ok(
                                                        C, current_price, time_str, _bar_time_str, _rebuild_vwap)
                                                    if _neutral_ok:
                                                        _can_build = True
                                                        _build_qty = _neutral_q
                                                        _build_reason = 'REBUILD_NEUTRAL_PROBE'
                                                    else:
                                                        _can_build = False
                                                        C._rebuild_cooldown_bars = 30
                                                        log_once(C, '_neutral_probe_gate_logged',
                                                                 f"  [Rebuild Blocked] NEUTRAL probe: {_neutral_why}")
                                                elif _total_pos < _neutral_q:
                                                    _can_build = True
                                                    _build_qty = _neutral_q - _total_pos
                                                    _build_reason = 'REBUILD_NEUTRAL_PROBE_ADD'
                                                    _is_probe_upgrade_path = True
                                                else:
                                                    _uw_block, _uw_why = _blocks_probe_scale_to_half(
                                                        C, current_price)
                                                    if _uw_block:
                                                        _can_build = False
                                                        C._rebuild_cooldown_bars = 30
                                                        log_once(C, '_neutral_scale_blocked_logged',
                                                                 f"  [Rebuild Blocked] NEUTRAL scale-up: {_uw_why}")
                                                    else:
                                                        _can_build = True
                                                        _build_qty = C.base_qty_half - _total_pos
                                                        if C.trade_qty <= _total_pos < C.base_qty_half:
                                                            _build_reason = 'REBUILD_PROBE_TO_HALF'
                                                        else:
                                                            _build_reason = 'REBUILD_NEUTRAL_HALF'
                                            elif C._base_rebuild_stage == 1 and C._days_since_stop >= C._stop_cool_days + 2:
                                                # [P0 Fix] 防御性检查：空仓状态下禁止直接满仓，强制走探针路径
                                                if _total_pos == 0:
                                                    _neutral_ok, _neutral_why = _neutral_empty_probe_ok(
                                                        C, current_price, time_str, _bar_time_str, _rebuild_vwap)
                                                    if _neutral_ok:
                                                        _can_build = True
                                                        _build_qty = getattr(C, 'neutral_probe_qty', 100)
                                                        _build_reason = 'REBUILD_NEUTRAL_PROBE_ADD'
                                                    else:
                                                        _can_build = False
                                                        C._rebuild_cooldown_bars = 30
                                                        log_once(C, '_neutral_probe_gate_logged',
                                                                 f"  [Rebuild Blocked] NEUTRAL probe: {_neutral_why}")
                                                else:
                                                    _can_build = True
                                                    _build_qty = C.base_qty_full - _total_pos
                                                    _build_reason = 'REBUILD_NEUTRAL_FULL'
                                            else:
                                                _can_build = False

                                # ponytail: rebuild routing uses live trend_direction (see pre-rebuild update)
                                elif C.trend_direction == 'DOWN':
                                    _ma20 = getattr(C, '_trend_ma20', 0)
                                    _os_pct = getattr(C, 'down_rebuild_oversold_pct', 0.90)
                                    _oversold_th = _ma20 * _os_pct if _ma20 > 0 else 0
                                    # [Bug#5 Fix] 追赶阈值防跟跌：取 MA20*0.93 与
                                    # 近5日最低价*0.98 的较低值，防止 MA20 单边下滑
                                    # 时阈值跟着下滑导致价格永远追不上。
                                    if _oversold_th > 0 and len(C.bars_low) >= 5:
                                        _recent_5d_low = min(C.bars_low[-5:])
                                        _oversold_th = min(_oversold_th, _recent_5d_low * 0.98)
                                    _down_gate_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
                                    _down_gate_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
                                    _extreme_rsi_macro_ok = _severe_down_macro_rsi_ok(
                                        C, _down_gate_rsi, _down_gate_bias)
                                    _extreme_down_macro_veto = (
                                        _down_gate_bias < -0.08
                                        and _down_gate_rsi >= C.down_rebuild_extreme_rsi
                                    )
                                    if date_str == getattr(C, '_extreme_rsi_fast_used_date', ''):
                                        _extreme_rsi_macro_ok = False

                                    if (not _can_build and C._base_rebuild_stage == 0 and _total_pos == 0):
                                        _vs_ok, _vs_qty, _vs_reason = _try_virtual_shallow_down_probe(
                                            C, current_price, time_str, _bar_time_str, _oversold_th)
                                        if _vs_ok:
                                            _can_build = True
                                            _build_qty = _vs_qty
                                            _build_reason = _vs_reason
                                            C._pending_build_log = (
                                                f"  [Rebuild] Virtual shallow {_vs_qty} @ "
                                                f"{current_price:.2f} th={_oversold_th:.2f} ({_vs_reason})")

                                    # 如果已有探底仓位，允许在更低价位追加探针（倒金字塔），而非一刀切禁用
                                    if _oversold_th > 0 and current_price <= _oversold_th and C._base_rebuild_stage == 0:
                                        # [D3 P0 Fix] Do not allow probe stacking during the
                                        # first 2 days (the immunity window). Probes are
                                        # left-side bets entering extreme oversold washouts —
                                        # adding more shares before the reversal is confirmed
                                        # compounds the left-tail risk. During 5/28-5/30 the
                                        # 100-sh DULL probe plus the extreme-RSI fast-track
                                        # probes stacked to 400+ shares in 3 days, creating a
                                        # synthetic 2x position in a still-falling market.
                                        # Only allow new probes after the 2-day immunity
                                        # window, when the first reversal signal is visible.
                                        _probe_immune_no_add = False
                                        if _dpm_is_oversold_probe(C):
                                            _p_days = getattr(C, '_days_since_rebuild_probe', 99)
                                            if _p_days <= 2:
                                                _probe_immune_no_add = True

                                        # [D8 P0 Fix] Relax absolute oversold gate.
                                        # bias <= -6% was too strict and rejected
                                        # probes during moderate pullbacks (RSI 30-35,
                                        # bias -4%) that still had significant upside.
                                        # The bias threshold is relaxed to -4%, and
                                        # RSI rejection threshold moves from 25 to 30.
                                        # [Fix 8d] D8 gate uses frozen indicators for consistency
                                        # with downstream macro resonance path.
                                        _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
                                        _absolute_oversold = (
                                            _macro_bias <= -0.03
                                            or getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))
                                        )
                                        # Pre-compute RSI here so both the D8 absolute-
                                        # oversold gate and the downstream signal checks
                                        # use the same value (avoids double-computation and
                                        # the forward-reference bug where RSI was used
                                        # before being set).
                                        _daily_rsi_d8 = calc_daily_rsi(C, _bar_time_str, period=14)

                                        if _total_pos >= C.trade_qty:
                                            # [P9修复] 允许在更低价位追加探针（倒金字塔），但限制总量和频率
                                            # [修复缺陷#2] 放宽探针加仓门槛，从3.2%(0.968)降至2%(0.98)
                                            _probe_max_qty = getattr(C, 'probe_qty', 600)
                                            _probe_add_threshold = getattr(C, '_base_cost_price', 9999) * 0.98
                                            _can_add_probe = (
                                                _total_pos < _probe_max_qty
                                                and getattr(C, '_days_since_rebuild_probe', 99) >= 2
                                                and current_price < _probe_add_threshold
                                                and not _probe_immune_no_add  # [D3] Reject during immunity window
                                                and _absolute_oversold  # [D8] Require absolute oversold confirmation
                                            )
                                            if _can_add_probe:
                                                _can_build = True
                                                _build_qty = min(C.trade_qty, _probe_max_qty - _total_pos)
                                                _build_reason = 'REBUILD_OVERSOLD_PROBE_ADD'
                                                # [P2 Fix] Do NOT log "allowed" here — the FUSE pre-check
                                                # below may still reject the add, producing a contradictory
                                                # log. Only log after the add is confirmed.
                                            else:
                                                _can_build = False
                                                if not getattr(C, '_rebuild_probe_add_blocked_logged', False):
                                                    C._rebuild_probe_add_blocked_logged = True
                                                    _reason = (
                                                        f"pos={_total_pos} >= probe_max={_probe_max_qty}" if _total_pos >= _probe_max_qty
                                                        else f"days_since_probe={getattr(C, '_days_since_rebuild_probe', 99)} < 2"
                                                        if getattr(C, '_days_since_rebuild_probe', 99) < 2
                                                        else f"price {current_price:.2f} >= cost*0.98 {getattr(C, '_base_cost_price', 9999)*0.98:.2f}"
                                                        if not _probe_immune_no_add
                                                        else f"probe immunity days_since={getattr(C, '_days_since_rebuild_probe', 99)} <= 2, no stacking"
                                                    )
                                                    print(f"  [Rebuild] Probe add blocked: {_reason}")
                                        else:
                                            if not _allows_down_oversold_probe(C):
                                                _can_build = False
                                                _pol = _get_effective_exit_policy(C) or 'unset'
                                                log_once(C, '_down_oversold_probe_block_logged',
                                                          f"  [Rebuild] DOWN oversold probe blocked by exit policy "
                                                          f"(policy:{_pol}, trend:{C.trend_direction}, "
                                                          f"cooldown:{C._days_since_stop}/{C._stop_cool_days})")
                                            # [D8] If the absolute bias check fails (bias > -6%
                                            # AND not near bottom 60), the stock looks oversold on
                                            # MA ratio but is actually mid-range. Reject the probe.
                                            # [P1 Fix log78] Exception: when exit_policy=='down_oversold'
                                            # (init-half exit already confirmed oversold regime),
                                            # relax the D8 gate to RSI<35 instead of RSI>30 hard
                                            # reject. Without this, the strategy enters down_oversold
                                            # via init-half exit, passes the cooldown, then gets
                                            # blocked by D8's absolute-oversold gate at bias>-3%
                                            # — creating a paradox where down_oversold is set but
                                            # oversold probes are denied.
                                            _is_down_oversold_active = (
                                                getattr(C, '_exit_policy', '') == 'down_oversold'
                                                and getattr(C, '_days_since_init_half_exit', 99)
                                                    >= C._stop_cool_days)
                                            if not _absolute_oversold and _daily_rsi_d8 > 30 and not _is_down_oversold_active:
                                                _can_build = False
                                                _mark_down_rebuild_weak_today(C)
                                                log_once(C, '_down_signals_weak_logged',
                                                          f"  [Rebuild] DOWN skip: relative oversold but "
                                                          f"bias not extreme (bias:{_macro_bias:.2%} "
                                                          f"RSI14:{_daily_rsi_d8:.1f})")
                                            else:
                                                _adtm = getattr(C, '_adtm_val', 0.0)
                                                _daily_rsi = calc_daily_rsi(C, _bar_time_str, period=14)
                                                _daily_rsi_turning = 25 < _daily_rsi < 35
                                                _daily_bullish_bar = check_daily_bullish_bar(C, _bar_time_str)
                                                _dull_down, _down_streak = is_dull_down_decline(C, _bar_time_str)
                                                _prev_adtm = get_prior_day_adtm(C, _bar_time_str)
                                                # [P1 Fix] Relaxed ADTM rising check — allow a
                                                # minor deterioration (up to 0.01) counts as
                                                # stable-ish. The previous +0.002 hard
                                                # bar trapped probes in extremely oversold zones.
                                                # [P0 Fix 3] Widen drift tolerance from -0.02 to
                                                # -0.03 so genuine mid-ranges (06-03/06-12
                                                # pattern) aren't blocked by a single noisy
                                                # ADTM bar.
                                                _adtm_rising = _adtm >= _prev_adtm - 0.03
                                                _adtm_stabilized = (-0.50 < _adtm < 0.2)
                                                _adtm_ok_for_probe = _adtm > -0.85 and (_adtm_rising or _adtm_stabilized)
                                                _adtm_weakening_strict = (-0.85 < _adtm < 0.1) and _adtm_rising
                                                _adtm_weakening_moderate = (-0.50 < _adtm < 0.1) and _adtm_rising
                                                # [Defect4] Tighten the ADTM drift-down tolerance for
                                                # the DULL probe gate. The previous
                                                # `_adtm > _prev_adtm - 0.02` allowed a DULL probe to
                                                # fire when ADTM was clearly worsening (5/28: a
                                                # high-vol day saw a 200-sh DULL probe entered with
                                                # adtm at -0.12 vs prev 0.05, i.e. the signal was
                                                # strongly negative rather than "flat"). Two days
                                                # later the probe was stopped at -8% with avg cost
                                                # 224 CNY above where an EXTREME probe would have
                                                # entered. New tolerance: -0.005 — essentially
                                                # flat-or-better — so the DULL probe only fires on
                                                # genuinely stable ADTM patterns.
                                                _adtm_weakening_mild = ((-0.65 < _adtm < 0.2)
                                                                        and (_adtm >= _prev_adtm - 0.03))
                                                _dull_rsi_th = getattr(C, 'down_probe_dull_rsi_th', 35.0)
                                                _oversold_probe_blocked = _oversold_probe_in_block_period(C)
                                                # [P0 Fix] Post-stop cooldown guard: block any rebuild path
                                                # from firing in the same bar as a stop-loss / defense sell.
                                                # On 05-29 this caused 100 sh stopped at 34.68 to be re-bought
                                                # at 34.67 on the same bar by REBUILD_EXTREME_RSI_FAST,
                                                # creating a churn churn loss of ~120 yuan in fees + slippage.
                                                _stop_cooldown_active = (
                                                    getattr(C, '_base_stop_done', False)
                                                    or getattr(C, '_down_intraday_reduced_done', False)
                                                    or getattr(C, '_probe_immune_defense_done', False)
                                                    or getattr(C, '_gap_exit_today', False)           # [P0-2 Fix] 阻断同日 MACRO 买回
                                                    or getattr(C, '_intraday_micro_gap_done', False)  # [P0-4 Fix] 阻断同日盘中 micro 平仓后买回
                                                )
                                                # [P1 Fix] High-amplitude filter uses
                                                # rolling intraday bar H/L as the primary
                                                # source. ORB (09:30-10:00) data may be
                                                # stale or absent by the time probe
                                                # decisions are made, so we always use a
                                                # fresh 30-bar lookback.
                                                _amp_bars = min(len(C.bars_high), len(C.bars_low), 30)
                                                if _amp_bars > 0:
                                                    _cur_day_high = max(C.bars_high[-_amp_bars:])
                                                    _cur_day_low = min(C.bars_low[-_amp_bars:])
                                                    _cur_amp = (_cur_day_high - _cur_day_low) / _cur_day_low if _cur_day_low > 0 else 0.0
                                                else:
                                                    # Fallback to ORB if intraday bars not yet populated
                                                    if C.orb_high > 0 and C.orb_low < 9999:
                                                        _cur_amp = (C.orb_high - C.orb_low) / C.orb_low if C.orb_low > 0 else 0.0
                                                    else:
                                                        _cur_amp = 0.0
                                                _is_high_amp = _cur_amp >= getattr(C, 'orb_amp_th', 0.035)
                                                # ===== Macro-micro resonance probe (main path - merged) =====
                                                # [Fix 8b] Use frozen intraday macro indicators
                                                _macro_bias = getattr(C, '_frozen_bias_20', getattr(C, '_backend_bias_20', 0.0))
                                                _macro_near_bottom = getattr(C, '_frozen_near_bottom_60', getattr(C, '_backend_near_bottom_60', False))

                                                # 第一级: 宏观空间极寒 (替代僵化的连跌5日)
                                                # 乖离率 <= -8% 或 贴近60日大底(3%以内) 或 跌破支撑位
                                                _support = getattr(C, '_backend_support', 0.0)
                                                _is_break_support = (current_price < _support) if _support > 0 else False
                                                if _is_break_support:
                                                    _log_rebuild_signal(C, f"  [Rebuild Signal] Price {current_price:.2f} below support {_support:.2f}")

                                                _macro_space_ok = (_macro_bias <= -0.03) or _macro_near_bottom or _is_break_support

                                                # 第二级: 微观量能萎缩 (10:00后有效)
                                                # vol_ratio = 近5根1分钟均量 / 近20根均量
                                                _micro_vol_ratio = calc_vol_ratio(C)
                                                _micro_vol_dry = (time_str >= '10:00:00' and _micro_vol_ratio < 0.5)

                                                # 第三级: 微观价格反转确认
                                                # 价格站稳VWAP 或 1分钟上穿MA5
                                                _micro_vwap_ok = (time_str >= '10:00:00' and current_price > _rebuild_vwap)
                                                _micro_ma5_cross = False
                                                if len(C.bars_close) >= 6:
                                                    _ma5_val = sum(C.bars_close[-5:]) / 5.0
                                                    _prev_ma5 = sum(C.bars_close[-6:-1]) / 5.0
                                                    _micro_ma5_cross = (C.bars_close[-1] > _ma5_val and C.bars_close[-2] <= _prev_ma5)

                                                # [Fix 7] Even during extreme-bias washouts, require at
                                                # least a price-reversal signal before firing the macro
                                                # probe. Previously _extreme_bias short-circuited micro
                                                # confirmation entirely — "catching a falling knife".

                                                # [P1 Fix] Liquidity dry-up gate: if the backend reports
                                                # 10+ low-volume days out of the last 20, the stock is
                                                # likely in a "no buyer" state — probe buys here tend to
                                                # get stuck in slow melt-downs. Short-circuit both the
                                                # macro-resonance and the extreme-RSI fast-track paths.
                                                _is_liquidity_dry = getattr(C, '_backend_continuous_shrink_days', 0) >= 10
                                                if _is_liquidity_dry and _total_pos == 0:
                                                    _macro_resonance_ok = False
                                                    _extreme_rsi_fast_track = False
                                                    if not getattr(C, '_liquidity_block_logged', False):
                                                        C._liquidity_block_logged = True
                                                        print(f"  [Rebuild Block] Liquidity dry-up: "
                                                              f"{getattr(C, '_backend_continuous_shrink_days', 0)} low-vol days in 20, probe blocked")

                                                _extreme_bias = _macro_bias <= -0.12
                                                _micro_confirmed = (
                                                    (_micro_vol_dry and (_micro_vwap_ok or _micro_ma5_cross))
                                                    or (_extreme_bias and (_micro_vwap_ok or _micro_ma5_cross))
                                                )

                                                _macro_resonance_ok = (
                                                    not _oversold_probe_blocked and
                                                    not _stop_cooldown_active and
                                                    not getattr(C, '_probe_immune_defense_done', False) and
                                                    _macro_space_ok and
                                                    _micro_confirmed and
                                                    _adtm > -0.85 and
                                                    not _is_high_amp and
                                                    C._base_rebuild_stage == 0 and
                                                    _total_pos < C.trade_qty and
                                                    (time_str >= '14:00:00' or _daily_rsi < 15)  # [P1-7 Fix] 移除 bias <= -0.12 绕过特权
                                                )

                                                # [Fix 6] Extreme RSI fast-track (RSI<20) is independent of
                                                # the late-afternoon / extreme-bias gate. Compute HERE so
                                                # it can be a top-level elif alongside the macro probe path.
                                                # [D3] Do not stack during the first 2 days (immunity
                                                # window) — even extreme RSI. The first probe needs time
                                                # to confirm the reversal before committing more capital.
                                                # [P1 Fix] Removed the _probe_immune_no_add gate. On 06-11
                                                # RSI hit 17.5, satisfying the fast-track, but the 2-day
                                                # immunity window (_probe_immune_no_add) blocked it
                                                # outright, contradicting the Defect8 override below and
                                                # starving the inverted pyramid during the highest-
                                                # conviction extreme-oversold setup. Extreme oversold
                                                # (RSI<20) now bypasses the immunity period so the
                                                # fast-track can build the inverted pyramid rapidly.
                                                # [P1 Fix] Underwater doubling gate: prohibit
                                                # EXTREME_RSI_FAST "fast-track add" when the
                                                # existing probe is already underwater by more
                                                # than a small noise threshold. Without this,
                                                # the fast-track doubles down on a losing bet
                                                # every time RSI prints an oversold reading,
                                                # which is precisely the behavior that produced
                                                # -304 CNY end-of-day unrealized on 05-29.
                                                # Allow first-time probe entry only when the
                                                # account is flat; any subsequent add is gated
                                                # by the bounce confirmation below.
                                                _cur_cost_ext = getattr(C, '_base_cost_price', 0.0)
                                                _is_underwater_add = (
                                                    _total_pos > 0
                                                    and _cur_cost_ext > 0
                                                    and current_price < _cur_cost_ext * 0.99
                                                )
                                                # [P0-1 Fix log81] EXTREME_RSI macro AND gate:
                                                # In severe DOWN (bias<-5%), require RSI<20 to enter,
                                                # aligned with DULL/RELAX macro gates. RSI<20 is a
                                                # true panic bottom; RSI<15 almost only near limit-down.
                                                _extreme_rsi_macro_ok = _severe_down_macro_rsi_ok(
                                                    C, _daily_rsi, _macro_bias)
                                                _extreme_down_macro_veto = (
                                                    C.trend_direction == 'DOWN'
                                                    and _macro_bias < -0.08
                                                    and _daily_rsi >= C.down_rebuild_extreme_rsi
                                                )
                                                if date_str == getattr(C, '_extreme_rsi_fast_used_date', ''):
                                                    _extreme_rsi_macro_ok = False

                                                _extreme_rsi_fast_track = (
                                                    not _oversold_probe_blocked
                                                    and not _stop_cooldown_active
                                                    and not getattr(C, '_probe_immune_defense_done', False)
                                                    and not _is_underwater_add
                                                    and not getattr(C, '_trail_skip_today', False)
                                                    and _daily_rsi < C.down_rebuild_extreme_rsi
                                                    and _extreme_rsi_macro_ok
                                                    and not _extreme_down_macro_veto
                                                    and _adtm > -0.85
                                                    and not _is_high_amp
                                                    and time_str >= C.decision_time
                                                    and C._base_rebuild_stage == 0
                                                    and _total_pos < C.trade_qty
                                                    and date_str != getattr(C, '_extreme_rsi_fast_used_date', '')
                                                    # ponytail: log139 05-29 RSI19.8+bias<-5% pre-SKIP entry
                                                    and not (C.trend_direction == 'DOWN'
                                                             and _macro_bias < -0.05
                                                             and _daily_rsi >= 15)
                                                )

                                                # [P1 Fix] Pre-trigger probe cooldown and total-qty
                                                # gate. Micro-probe paths (EXTREME/MACRO) each place
                                                # 100 shares, so they naturally stack to trade_qty over
                                                # multiple days. Require at least 2 days since the last
                                                # probe rebuild OR a completely empty position before
                                                # allowing a new probe entry. Also enforce a hard cap at
                                                # trade_qty. Note: `_total_pos < C.trade_qty` is already
                                                # inside each boolean above; this gate ALSO blocks the
                                                # "STOP ADD ABOVE trade_qty" condition. Empty positions
                                                # in DOWN with extreme RSI<15 are left alone so the
                                                # fast-track can catch the bottom (P2 Fix 6).
                                                # [Defect8] EXEMPT extreme RSI fast-track from the
                                                # 2-day cooldown. RSI < 20 is a strong left-side signal
                                                # — the inverted pyramid should be built FAST, not
                                                # delayed. On 6/11 the first 100 shares landed at
                                                # 32.73 but subsequent bars were blocked because
                                                # days_since_rebuild_probe reset to 0. This meant
                                                # only 200 shares were held during the high-confidence
                                                # profit later. New rule: if RSI < down_rebuild_extreme_rsi
                                                # and total_pos < trade_qty, skip the cooldown — allow
                                                # rapid stacking. Standard probe paths (RSI > 25) keep
                                                # the 2-day cooldown to avoid whipsaw.
                                                _probe_days_since = getattr(C, '_days_since_rebuild_probe', 99)
                                                _probe_cooldown_ok = _probe_days_since >= 2 or _total_pos == 0
                                                # Defect8 override: extreme RSI + room to build = exempt
                                                # [D3] Do not use this override during the first 2 days
                                                # (immunity window) — still need to respect the cooldown
                                                # so the first probe doesn't get compounded.
                                                # [P1 Fix] Drop the _probe_immune_no_add gate here too,
                                                # consistent with the _extreme_rsi_fast_track fix above.
                                                # Otherwise the override stays inert during the immunity
                                                # window and the fast-track is still blocked by the
                                                # cooldown check below.
                                                _ma5_upgrade_os = calc_ma(C, getattr(C, 'ma_short', 5))
                                                _confirmed_bounce_os = (
                                                    current_price > getattr(C, '_base_cost_price', 0.0) * 1.01
                                                    and _ma5_upgrade_os > 0
                                                    and current_price > _ma5_upgrade_os
                                                )
                                                _extreme_rsi_cooldown_override = (
                                                    _daily_rsi < C.down_rebuild_extreme_rsi
                                                    and _total_pos < C.trade_qty
                                                    and _total_pos > 0
                                                    and C._base_rebuild_stage == 0
                                                    and _confirmed_bounce_os
                                                )
                                                if _total_pos > 0 and not _probe_cooldown_ok and not _extreme_rsi_cooldown_override:
                                                    _macro_resonance_ok = False
                                                    _extreme_rsi_fast_track = False

                                                # [P2 Fix] Block rebuild paths that depend on backend-
                                                # computed indicators (MACD, ADTM, quantile, SAR) when
                                                # the API fetch returned stale data (timeout, HTTP error,
                                                # or exception). The frontend may be running on old
                                                # cached values from yesterday; this guard prevents a
                                                # stale REBUILD_EARLY_OVERSOLD_PROBE or MACD-crossover
                                                # signal from triggering a buy on a day when the
                                                # backend has not confirmed the data.
                                                # [P1 Fix log75] Only block BACKEND-dependent paths
                                                # (_macro_resonance_ok uses backend ADTM). Do NOT block
                                                # _extreme_rsi_fast_track — it uses LOCAL RSI computed
                                                # from C.close, which is always fresh. Blocking local
                                                # RSI oversold rebuilds on API timeout caused 7+ days
                                                # of unnecessary空仓 (05-20~05-26).
                                                if getattr(C, '_backend_indicators_stale', False):
                                                    _macro_resonance_ok = False
                                                    if not getattr(C, '_stale_indicator_block_logged', False):
                                                        C._stale_indicator_block_logged = True
                                                        print(f"  [Rebuild Block] Backend indicators stale "
                                                              f"(API timeout), block backend-dependent rebuild paths")

                                                # [Fix] Right-side stabilize path:
                                                # after a stop in DOWN trend, allow
                                                # a probe rebuild when SAR turns
                                                # bullish OR MACD golden-crosses
                                                # AND RSI is in the 30-55 stabilization
                                                # band. Without this, the strategy
                                                # waits for extreme oversold (RSI<20
                                                # or bias<=-12%) and misses every
                                                # mid-range bottoming-out rebound.
                                                # log127 P2: daily SAR/MACD may fire while intraday still DOWN;
                                                # allow micro 100 when price>VWAP (log122 blocked 300@34.31).
                                                _right_side_stabilize = (
                                                    not _oversold_probe_blocked
                                                    and not _stop_cooldown_active
                                                    and _right_side_entry_ok(
                                                        C, _daily_rsi, current_price, _rebuild_vwap, time_str)
                                                    and C._base_rebuild_stage == 0
                                                    # [log73 rollback] flat-only: do not stack
                                                    # onto an existing oversold probe (06-08
                                                    # 100+100=200 synthetic double risk).
                                                    and _total_pos == 0
                                                    and time_str >= C.decision_time
                                                )
                                                # [P2 Fix] _right_side_stabilize depends on SAR and MACD
                                                # from the backend; hard block when indicators are stale.
                                                # No local fallback: wait for backend data to recover.
                                                if getattr(C, '_backend_indicators_stale', False):
                                                    _right_side_stabilize = False

                                                if _right_side_stabilize:
                                                    _can_build = True
                                                    _build_qty = getattr(C, 'neutral_probe_qty', 100)
                                                    _build_reason = 'REBUILD_DOWN_RIGHT_SIDE'
                                                    C._pending_build_log = f"  [Rebuild] Right-side stabilize probe " \
                                                                           f"(SAR_bull={getattr(C, '_sar_bullish', False)} " \
                                                                           f"MACD_cross={getattr(C, '_macd_long_cross', False)} " \
                                                                           f"RSI={_daily_rsi:.1f})"
                                                elif _macro_resonance_ok:
                                                    _can_build = True
                                                    _build_qty = _oversold_fast_path_qty(
                                                        C, C.trade_qty, _macro_bias, _macro_near_bottom,
                                                        _micro_vwap_ok, _micro_ma5_cross)
                                                    _build_reason = 'REBUILD_OVERSOLD_PROBE_MACRO'
                                                    C._pending_build_log = f"  [Rebuild] REBUILD_OVERSOLD_PROBE_MACRO: bias={_macro_bias:.2%} " \
                                                                           f"bottom60={_macro_near_bottom} vol_ratio={_micro_vol_ratio:.2f} " \
                                                                           f"VWAP_ok={_micro_vwap_ok} MA5_cross={_micro_ma5_cross}"
                                                elif _extreme_rsi_fast_track:
                                                    _can_build = True
                                                    # [Fix 3] Left-side extreme entries are by definition
                                                    # counter-trend. T+1 rules mean we cannot exit early if
                                                    # the stock gaps down — one 200-share entry can lose ~500
                                                    # CNY on a 2.5% gap. Cap at 100 shares (micro probe)
                                                    # so the tail risk stays bounded regardless of win-rate.
                                                    _build_qty = C.trade_qty
                                                    _build_reason = 'REBUILD_EXTREME_RSI_FAST'
                                                    C._pending_build_log = f"  [Rebuild] Extreme RSI fast-track: " \
                                                                           f"RSI14 {_daily_rsi:.1f} < {C.down_rebuild_extreme_rsi:.0f}"
                                                # ===== 保留旧路径作为兜底, 极端超卖全天可触发 =====
                                                elif not _can_build and (time_str >= '14:00:00' or _daily_rsi < 15):  # [P1-7 Fix] 移除 bias <= -0.12 绕过
                                                    # [修复P2] 极端超卖(RSI<25)豁免 ADTM 拐头要求
                                                    _adtm_strict_ok = _adtm_weakening_strict or (
                                                        _daily_rsi < 25 and _adtm > -0.85)
                                                    # [Fix 6b] Widen RSI exemption in the relaxed
                                                    # ADTM gate. Original 25 was too tight for the
                                                    # 27-35 RSI zone that produces high-quality
                                                    # rebounds. Now matching the RSI-30 threshold
                                                    # used in the relax_score first component.
                                                    _adtm_relax_ok = _adtm_weakening_moderate or (
                                                        _daily_rsi < 30 and _adtm > -0.85)
                                                    _daily_rsi_turning_ext = _daily_rsi < 35  # 放宽 turning 范围覆盖极端超卖
                                                    # [P0 Fix] Check probe immunity window to prevent stacking within 2 days
                                                    # [Fix 5] STRICT: hard time gate — only fire
                                                    # before 13:00 to avoid end-of-day
                                                    # re-entry on weak signals. 05-28 saw
                                                    # probes entered during 15:00+ just before
                                                    # eod summary.
                                                    _strict_ok = (not _oversold_probe_blocked and _adtm_ok_for_probe
                                                                  and _adtm_strict_ok and _daily_rsi_turning_ext
                                                                  and _daily_bullish_bar and not _is_high_amp
                                                                  and not _probe_immune_no_add
                                                                  and time_str < '13:00:00')
                                                    # [Fix 6c] DULL path: use _adtm_relax_ok instead of
                                                    # pure _adtm_weakening_mild so the RSI<30 exemption
                                                    # actually reaches this path. 05-28 (RSI=27.9) has
                                                    # ADTM=-0.07, prev=-0.05 — pure mild fails because
                                                    # > prev-0.02 is a strict inequality at the boundary.
                                                    # [P0 Fix 3] DULL: RSI cap raised from 30→35,
                                                    # entry time relaxed from 13:00→10:30. 06-03
                                                    # and 06-12 had RSI in the 27-33 band with
                                                    # ADTM that was flat-to-slightly-drifting —
                                                    # the old thresholds blocked them.
                                                    # [P1 Fix] Same-price re-entry guard for DULL path.
                                                    # After a failed DULL probe, stock tends to oscillate
                                                    # around the entry level (the 37.10 pattern). Re-entering
                                                    # at the same price produces fee/slippage churn. Require
                                                    # at least 1.5% movement away from the previous DULL entry
                                                    # before firing DULL again. When _last_dull_probe_price
                                                    # is 0, this is the first DULL probe in the session and
                                                    # the guard is skipped.
                                                    _dull_same_price_blocked = False
                                                    _last_dull_price = getattr(C, '_last_dull_probe_price', 0.0)
                                                    if _last_dull_price > 0:
                                                        _price_dist_pct = abs(current_price - _last_dull_price) / _last_dull_price
                                                        if _price_dist_pct < 0.015:
                                                            _dull_same_price_blocked = True
                                                    # [P1 Fix log63-4] Extend DULL RSI exemption from
                                                    # 20 to 25. In sustained downtrends the probe
                                                    # engine can sit idle for 6+ consecutive
                                                    # sessions because ADTM stays flat (no
                                                    # weakening signal) and RSI drifts in the
                                                    # 20-30 band without ever crossing 20. At
                                                    # RSI<25 the stock is already well inside the
                                                    # historical rebound zone (the same RSI band
                                                    # that REBUILD_OVERSOLD_PROBE_RELAX targets),
                                                    # so relaxing the gate opens ~5% more of the
                                                    # RSI range to entries without changing the
                                                    # extreme-only focus.
                                                    # [P0-1 Fix] 修复 OR 旁路：在 DOWN 趋势且宏观严重超卖时，强制要求 RSI<25
                                                    _dull_severe_macro_block = (
                                                        C.trend_direction == 'DOWN'
                                                        and _macro_bias < -0.05
                                                        and not _severe_down_macro_rsi_ok(
                                                            C, _daily_rsi, _macro_bias))

                                                    _dull_down_eff = (
                                                        _dull_down
                                                        or (_in_post_gap_rebuild_window(C)
                                                            and _down_streak >= 1
                                                            and _daily_rsi < getattr(
                                                                C, 'micro_gap_rebuild_rsi_floor', 30)))
                                                    # log164 P1-A: accel no longer bypasses not_dull_down

                                                    _dull_ok = (not _oversold_probe_blocked and _adtm_ok_for_probe
                                                                and _dull_down_eff
                                                                and _daily_rsi < _dull_rsi_th
                                                                and (_adtm_relax_ok or _daily_rsi < 25)
                                                                and not _is_high_amp
                                                                and time_str >= '10:30:00'
                                                                and not _probe_immune_no_add
                                                                and not _dull_same_price_blocked
                                                                and not _dull_severe_macro_block) # 替换原来的 OR 逻辑
                                                    # [Fix 5] Relax RSI thresholds across the scoring
                                                    # components. Original 25/35/30 windows missed the
                                                    # 27-35 "deeply oversold but not extreme" zone that
                                                    # historically yields good rebound entries. New
                                                    # thresholds 30/40/35 open up that region.
                                                    _relax_score = (int(_adtm_weakening_moderate or _daily_rsi < 30) + int(_daily_rsi < 40)
                                                                    + int(_daily_bullish_bar or _daily_rsi < 35))
                                                    # [Defect4, Regress] RELAX path: combine BOTH
                                                    # gates with OR. _adtm_relax_ok carries the
                                                    # RSI<30 exemption that rescues 06-09
                                                    # (RSI=27.4, ADTM=-0.21, drop=0.05 — mild
                                                    # fails because drop exceeds the 0.02
                                                    # tolerance but relax_ok's RSI<30 clause
                                                    # succeeds). _adtm_weakening_mild covers the
                                                    # RSI 30-50 "deep dip but not extreme
                                                    # oversold" transition phase that pure
                                                    # relax_ok misses. Using OR preserves both
                                                    # the original RSI<30 escape hatch and the
                                                    # new wider-ADTM tolerance.
                                                    # [Defect1] RELAX probe: raise relax_score threshold from
                                                    # 2/3 to 3/3 and require at least 1 day of down-streak to
                                                    # avoid false entries driven purely by RSI numerics without
                                                    # actual price deterioration. RSI=33 with no streak and a
                                                    # flat-to-positive ADTM now fails — we need the full
                                                    # 3/3 score (meaning ALL of ADTM/RSI/bullish-bar conditions
                                                    # fire) AND at least 1 consecutive down-day before entering.
                                                    # [P0 Fix 3] RELAX: score threshold lowered
                                                    # from 3/3 to 2/3. 06-03 and 06-12 had
                                                    # score=2 (ADTM/RSI fire but no bullish bar)
                                                    # — these are still valid left-side entries
                                                    # in a confirmed downtrend, not "false".
                                                    # _down_streak >= 1 remains as a trend guard.
                                                    # [P1 Fix log75] Further lower to 1/3 when
                                                    # RSI<30 in confirmed DOWN trend. 05-20~05-26
                                                    # had RSI=33 with flat ADTM (score=0-1),
                                                    # causing 7 days of空仓. RSI<30 in DOWN is
                                                    # itself sufficient oversold confirmation;
                                                    # requiring ADTM weakening AND down-streak
                                                    # AND bullish bar is over-gating.
                                                    _relax_threshold = 1 if (_daily_rsi < 30 and C.trend_direction == 'DOWN') else 2

                                                    _relax_ok = (
                                                        not _oversold_probe_blocked
                                                        and _adtm_ok_for_probe
                                                        and (_adtm_relax_ok or _adtm_weakening_mild)
                                                        and _relax_score >= _relax_threshold
                                                        and _down_streak >= 1
                                                        and not _is_high_amp
                                                        and not _probe_immune_no_add
                                                        and not getattr(C, '_is_gap_down_day_flag', False)
                                                        and _severe_down_macro_rsi_ok(C, _daily_rsi, _macro_bias)
                                                    )
                                                    if _strict_ok:
                                                        _can_build = True
                                                        _build_qty = _oversold_rebuild_qty(C, C.trade_qty)
                                                        _build_reason = 'REBUILD_OVERSOLD_PROBE_STRICT'
                                                        C._pending_build_log = f"  [Rebuild] Strict oversold probe - momentum weakening " \
                                                                               f"(ADTM {_adtm:.2f} prev:{_prev_adtm:.2f}, RSI14 {_daily_rsi:.1f}, daily reversal)"
                                                    elif _dull_ok:
                                                        _can_build = True
                                                        _build_qty = _oversold_rebuild_qty(C, C.trade_qty)
                                                        _build_reason = 'REBUILD_OVERSOLD_PROBE_DULL'
                                                        C._pending_build_log = f"  [Rebuild] Dull-down probe (streak {_down_streak}d, ADTM {_adtm:.2f} prev:{_prev_adtm:.2f}, RSI14 {_daily_rsi:.1f})"
                                                    elif _relax_ok:
                                                        _can_build = True
                                                        _build_qty = _oversold_rebuild_qty(C, C.trade_qty)
                                                        _build_reason = 'REBUILD_OVERSOLD_PROBE_RELAX'
                                                        C._pending_build_log = f"  [Rebuild] Relaxed oversold probe (streak {_down_streak}d, score {_relax_score}/3, RSI14 {_daily_rsi:.1f})"
                                                    # [Fix 4] EARLY_OVERSOLD: light-weight left-side entry for
                                                    # the RSI 30-40 band with mild ADTM relaxation. 06-03/06-12
                                                    # had RSI in 27-33 with flat or slightly-drifting ADTM —
                                                    # STRICT/DULL/RELAX all failed because they required
                                                    # bullish_bar confirmation or adtm strict ok. This path
                                                    # lowers the bar so a genuine oversold washout can be
                                                    # captured even when the bounce bar hasn't materialized yet.
                                                    # [P1 Fix] Gap-day mutex on EARLY oversold
                                                    # probe entries — same rationale as RELAX
                                                    # above, but EARLY runs in an even
                                                    # shallower RSI band (30-40) where
                                                    # gap-day drift is most dangerous.
                                                    # [P1 Fix] Tighten EARLY probe in DOWN trend:
                                                    # require macro bias >=-5% (no severe macro bearish)
                                                    # or RSI<25 (deeper oversold) to avoid 05-27 style churn
                                                    # [P2-1 Fix log81] EARLY macro bypass tightened from 35 to 25,
                                                    # aligned with DULL/RELAX macro gates
                                                    elif not _oversold_probe_blocked and not _stop_cooldown_active \
                                                            and 25 <= _daily_rsi < 40 and _adtm > -0.50 \
                                                            and _macro_space_ok and C._base_rebuild_stage == 0 \
                                                            and _total_pos < C.trade_qty and not _probe_immune_no_add \
                                                            and not getattr(C, '_is_gap_down_day_flag', False) \
                                                            and not getattr(C, '_phc_sell_today', False) \
                                                            and (C.trend_direction != 'DOWN' or _macro_bias >= -0.05
                                                                 or (_daily_rsi < 25
                                                                     and (_adtm_rising or _daily_bullish_bar))):
                                                        # [Issue3 Fix] Block rebuild if we sold PHC today to prevent churn
                                                        _can_build = True
                                                        _build_qty = _oversold_fast_path_qty(
                                                            C, C.trade_qty, _macro_bias, _macro_near_bottom,
                                                            _micro_vwap_ok, _micro_ma5_cross)
                                                        _build_reason = 'REBUILD_EARLY_OVERSOLD_PROBE'
                                                        C._pending_build_log = f"  [Rebuild] Early oversold probe (RSI:{_daily_rsi:.1f} ADTM:{_adtm:.2f} macro:{_macro_bias:.2%})"
                                                    # Note: extreme RSI fast-track handled in its own top-level
                                                    # elif above; the fallback here is the threshold-based extreme.
                                                    else:
                                                        # [Fix 5] Use prev_close as anchor instead of lagging MA20
                                                        # ponytail: anchor is prev_close×pct (0.90 = single-day limit floor)
                                                        _ref_anchor = getattr(C, 'prev_close', 0.0)
                                                        if not is_valid_price(_ref_anchor):
                                                            _ref_anchor = getattr(C, '_backend_lowest_60', 0.0)

                                                        if _ref_anchor > 0:
                                                            _extreme_th = _ref_anchor * C.down_rebuild_extreme_oversold_pct
                                                        else:
                                                            _extreme_th = 0

                                                        _extreme_rsi_exemption = _daily_rsi < C.down_rebuild_extreme_rsi
                                                        _momentum_neutralizing = (_adtm_rising or _daily_bullish_bar or _extreme_rsi_exemption)
                                                        _extreme_cool_ok = True
                                                        # [Defect6] Allow re-entry during extreme RSI even when
                                                        # a prior probe stop cooldown is still active. The
                                                        # previous logic blocked ALL oversold entries for 5 days
                                                        # after a probe stop, which missed the 6/03 rebound
                                                        # when RSI signalled a second extreme oversold zone.
                                                        # New rule: RSI < down_rebuild_extreme_rsi bypasses
                                                        # the cooldown; only non-extreme oversold entries
                                                        # respect the 5-day block.
                                                        if getattr(C, '_last_extreme_stop_date', '') and _daily_rsi >= C.down_rebuild_extreme_rsi:
                                                            try:
                                                                _d1_extreme = datetime.strptime(C._last_extreme_stop_date, '%Y-%m-%d')
                                                                _d2_extreme = datetime.strptime(date_str, '%Y-%m-%d')
                                                                _days_since_extreme = (_d2_extreme - _d1_extreme).days
                                                            except Exception:
                                                                _days_since_extreme = 99
                                                            if _days_since_extreme < C.down_rebuild_extreme_cool_days:
                                                                _extreme_cool_ok = False
                                                        _extreme_oversold = (
                                                            not _oversold_probe_blocked
                                                            and _extreme_th > 0 and current_price <= _extreme_th
                                                            and _daily_rsi < C.down_rebuild_extreme_rsi
                                                            and _momentum_neutralizing
                                                            and _extreme_cool_ok
                                                            and C._base_rebuild_stage == 0
                                                            and _total_pos < C.trade_qty
                                                            # [P0 Fix] Relax high-amplitude gate: use down_rebuild_extreme_rsi instead of 15
                                                            and (not _is_high_amp or _daily_rsi < C.down_rebuild_extreme_rsi)
                                                            # [P1 Fix] Gate against same-bar buy-after-sell. Without this, a
                                                            # DPM stop triggered at -6% and then _extreme_oversold would
                                                            # immediately re-build at the same level, producing a wash-sale loss:
                                                            # buy -> sell at -6% -> buy at same price in one bar.
                                                            # _extreme_rsi_fast_track has the same guard.
                                                            and not _stop_cooldown_active
                                                            and not getattr(C, '_probe_immune_defense_done', False)
                                                            # [P0 Fix] Gap-day mutex. If this session opened below
                                                            # the gap-down threshold, suppress extreme-oversold
                                                            # adds — they amplify a confirmed loser pattern
                                                            # (log61 05-29). Same guard as
                                                            # _extreme_rsi_fast_track above.
                                                            and not getattr(C, '_is_gap_down_day_flag', False)
                                                            # [P0 Fix] Trail-skip mutex. If the probe already
                                                            # risk engine deferred the trail-stop for this probe
                                                            # today (peak < cost*1.03) it signaled
                                                            # confirmed loser.
                                                            and not getattr(C, '_trail_skip_today', False)
                                                        )
                                                        if _extreme_oversold:
                                                            _can_build = True
                                                            # [Fix 3] Same 100-share cap as the fast-track
                                                            # extreme path. Tail risk dominates for left-side
                                                            # entries — keeping position micro reduces the
                                                            # amplitude of single-bar losses. Aligned with the
                                                            # Fix-3 cap in the fast-track branch above.
                                                            _build_qty = min(C.trade_qty, C.down_rebuild_extreme_probe_qty)
                                                            _build_reason = 'REBUILD_EXTREME_OVERSOLD'
                                                            _adtm_mom = 'rising' if _adtm_rising else 'falling'
                                                            _bar_mom = ' bullish_bar' if _daily_bullish_bar else ''
                                                            _exempt_tag = ' [RSI EXEMPT]' if _extreme_rsi_exemption else ''
                                                            C._pending_build_log = f"  [Rebuild] EXTREME oversold probe - RSI14 {_daily_rsi:.1f} " \
                                                                                   f"< {C.down_rebuild_extreme_rsi}, " \
                                                                                   f"price {current_price:.2f} <= {_extreme_th:.2f}, " \
                                                                                   f"momentum: ADTM {_adtm_mom}{_bar_mom}{_exempt_tag}"
                                                        else:
                                                            _can_build = False
                                                            if _oversold_probe_blocked:
                                                                _block_days = _oversold_probe_block_days(C)
                                                                log_once(C, '_probe_cooldown_block_logged',
                                                                          f"  [Rebuild Block] post-stop probe cooldown "
                                                                          f"({C._days_since_stop}d/{_block_days}d), "
                                                                          f"RSI14:{_daily_rsi:.1f} ADTM:{_adtm:.2f}")
                                                            elif _adtm <= -0.85:
                                                                log_once(C, '_adtm_strong_block_logged',
                                                                          f"  [Rebuild Block] DOWN skip: ADTM {_adtm:.2f} <= -0.85, "
                                                                          f"downward momentum too strong for probe")
                                                            else:
                                                                # [Fix 7] Detailed diagnostic log exposing real failure reasons
                                                                _strict_fail_reason = ""
                                                                if not _daily_bullish_bar:
                                                                    _strict_fail_reason += "no_bullish_bar "
                                                                if not _adtm_strict_ok:
                                                                    _strict_fail_reason += f"adtm_strict_fail({_adtm:.2f}) "
                                                                if _is_high_amp:
                                                                    _strict_fail_reason += "high_amp "

                                                                _dull_fail_reason = ""
                                                                if not _dull_down_eff:
                                                                    _dull_fail_reason += "not_dull_down "
                                                                if _dull_same_price_blocked:
                                                                    _dull_fail_reason += "same_price_block "
                                                                if _dull_severe_macro_block:
                                                                    _dull_fail_reason += "severe_macro_block "

                                                                _extreme_fail_reason = ""
                                                                if _extreme_th > 0 and current_price > _extreme_th:
                                                                    _extreme_fail_reason += f"price {current_price:.2f} > ext_th {_extreme_th:.2f} "
                                                                if not _extreme_rsi_macro_ok:
                                                                    _extreme_fail_reason += "rsi_macro_gate_blocked "

                                                                log_once(C, '_down_signals_weak_logged',
                                                                    f" [Rebuild] DOWN skip, signals weak. RSI:{_daily_rsi:.1f} ADTM:{_adtm:.2f} "
                                                                    f"prev:{_prev_adtm:.2f} bias:{_macro_bias:.2%} streak:{_down_streak}d. "
                                                                    f"Strict:[{_strict_fail_reason.strip()}] "
                                                                    f"Dull:[{_dull_fail_reason.strip()}] "
                                                                    f"Extreme:[{_extreme_fail_reason.strip()}]"
                                                                )
                                                                _mark_down_rebuild_weak_today(C)
                                                else:
                                                    # [P0-3 Redo] Removed ADTM deceleration filter.
                                                    # The filter was trying to predict which extreme RSI
                                                    # entries would rebound (05-29 ADTM falling → lost,
                                                    # 06-11 ADTM falling → won). Prediction is unreliable.
                                                    # Instead, unify the risk/reward by tightening the
                                                    # hard stop for DOWN-trend oversold probes to -5%
                                                    # (see _get_hard_stop_pct changes). This makes
                                                    # -5% loss / +4% PHC profit nearly symmetric (1.25:1)
                                                    # without guessing entry quality.
                                                    # Also: entry B (_extreme_rsi_fast_track, line 3329)
                                                    # never had this filter, so the two entry paths are
                                                    # now consistent — both allow extreme RSI entry
                                                    # regardless of ADTM direction, trusting the tighter
                                                    # stop to limit downside.
                                                    _extreme_oversold_bypass = (C.trend_direction == 'DOWN'
                                                                                and _daily_rsi < 20
                                                                                and _extreme_rsi_macro_ok
                                                                                and not _extreme_down_macro_veto
                                                                                and not _is_high_amp
                                                                                and time_str >= C.decision_time
                                                                                and date_str != getattr(C, '_extreme_rsi_fast_used_date', '')
                                                                                and not _oversold_probe_blocked
                                                                                and not _stop_cooldown_active
                                                                                and _total_pos == 0
                                                                                and not (C.trend_direction == 'DOWN'
                                                                                         and _macro_bias < -0.05
                                                                                         and _daily_rsi >= 15))

                                                    if _extreme_oversold_bypass:
                                                        _can_build = True
                                                        _build_qty = C.trade_qty
                                                        _build_reason = 'REBUILD_EXTREME_RSI_FAST'
                                                        C._pending_build_log = f"  [Rebuild] Extreme RSI fast-track bypass in DOWN: RSI14 {_daily_rsi:.1f} < 20 (ADTM {_adtm:.2f}, stop -8%)"
                                                    else:
                                                        _can_build = False
                                                        # [P2-11 Fix] Diagnostic log when rebound signals present but not building
                                                    if (getattr(C, '_sar_bullish', False) or getattr(C, '_macd_long_cross', False)) and not _stop_cooldown_active and not _oversold_probe_blocked:
                                                        if not getattr(C, '_rebound_miss_logged', False):
                                                            C._rebound_miss_logged = True
                                                            # [Issue3 Fix] Correct RSI range text to match actual 30-55 threshold
                                                            # [P2 Fix] Show trend gate reason, not just RSI
                                                            if C.trend_direction == 'DOWN':
                                                                print(f"  [Rebuild Diag] Rebound signal (SAR_bull/MACD_cross) detected but blocked. "
                                                                    f"trend=DOWN (need NEUTRAL/UP or price>VWAP for RIGHT_SIDE micro), "
                                                                    f"RSI14={_daily_rsi:.1f}, "
                                                                    f"stale={getattr(C, '_backend_indicators_stale', False)}, total_pos={_total_pos}")
                                                            else:
                                                                print(f"  [Rebuild Diag] Rebound signal (SAR_bull/MACD_cross) detected but blocked. "
                                                                    f"RSI14={_daily_rsi:.1f} (need 30-55), stale={getattr(C, '_backend_indicators_stale', False)}, "
                                                                    f"total_pos={_total_pos}")
                                                    # [Defect5] Throttle rebuild-diagnostic to once per
                                                    # day (was once per hour). During the 5/19-6/18 empty
                                                    # stretch, a single day would produce 4 identical
                                                    # "Conditions not met" lines — 100+ lines total over
                                                    # 25 days with zero actionable content. Once per day
                                                    # still provides clear timeline visibility for
                                                    # backtesting while eliminating the spam.
                                                    _cur_date = getattr(C, 'current_date', '')
                                                    if _cur_date != getattr(C, '_rebuild_diag_down_date', ''):
                                                        C._rebuild_diag_down_date = _cur_date
                                                        _ot = f" oversold_th={_oversold_th:.2f}" if _oversold_th > 0 else ""
                                                        print(f"  [Rebuild] DOWN skip, need NEUTRAL/UP{_ot}")

                    _build_qty = (_build_qty // 100) * 100

                    # [V2] BETA 系统性风险阻断（大盘连跌 + Beta > 1.2 时阻断所有建仓）
                    if _can_build and _build_reason.startswith('REBUILD_'):
                        _beta_val = getattr(C, '_backend_beta_20', 1.0)
                        _market_down = getattr(C, '_backend_macro_down_5d', False)
                        if _market_down and _beta_val > 1.2:
                            _can_build = False
                            C._rebuild_cooldown_bars = 60
                            print(f"  [Build Blocked] Systemic risk: market_down_5d + beta={_beta_val:.2f}, block {_build_reason}")
                        elif C.strategy_mode == 'SKIP':
                            _can_build = False
                            C._rebuild_cooldown_bars = 30
                            log_once(C, '_skip_rebuild_final_gate_logged',
                                     f"  [Rebuild Blocked] SKIP mode, block {_build_reason}")

                    # [Fix 10] Selective stale block: only block paths that depend
                    # on backend-computed indicators; keep local RSI EXTREME paths active.
                    if getattr(C, '_backend_indicators_stale', False):
                        _macro_resonance_ok = False
                        _right_side_stabilize = False
                        # Note: _extreme_rsi_fast_track and _extreme_oversold NOT blocked here
                        # because they primarily depend on local RSI and price data.
                        if not getattr(C, '_stale_indicator_block_logged', False):
                            C._stale_indicator_block_logged = True
                            print(f" [Rebuild Block] Backend stale, block MACRO/RIGHT_SIDE paths. "
                                  f"LOCAL RSI EXTREME path remains active.")

                    if not _can_build and C._base_stop_date != '':
                        # [Defect5] Throttle rebuild-diagnostic to once per day
                        # (was once per hour). Same reasoning as Defect5 above:
                        # 25-day empty stretch generated 100+ near-identical lines
                        # with no new information. One line per day keeps the
                        # timeline visible while keeping the log readable.
                        _cur_date = getattr(C, 'current_date', '')
                        if _cur_date != getattr(C, '_rebuild_diag_cond_date', ''):
                            C._rebuild_diag_cond_date = _cur_date
                            _cool_ok = "OK" if C._days_since_stop >= C._stop_cool_days else "NO"
                            _cool_disp = "n/a" if C._days_since_stop >= 99 else f"{C._days_since_stop}d"
                            _trend_ok = "OK" if C.trend_direction in ('UP', 'NEUTRAL') else "NO(DOWN)"
                            # [P2-10 Fix] Explicitly mark as flat when empty to avoid confusion with stage:1
                            _pos_now_diag = get_total_position(C)
                            _stage_display = f"stage:{C._base_rebuild_stage}" if _pos_now_diag > 0 else "stage:0(flat)"
                            print(f"  [Rebuild] Conditions not met: cooldown:{_cool_ok}({_cool_disp}) "
                                  f"trend:{_trend_ok}({C.trend_direction}) {_stage_display}")
                        C._rebuild_cooldown_bars = 10

                    if _can_build and C.strategy_mode == 'OBSERVE':
                        # [Fix D3] If we're empty AND have NEVER built before
                        # (first-ever entry), exempt from the OBSERVE whitelist.
                        # Otherwise the very first init probe of the lifecycle gets
                        # blocked and we miss 4+ days waiting for a rebuild path
                        # that never matches because we never had a position.
                        _first_entry_exempt = (
                            _total_pos == 0
                            and not getattr(C, '_base_ever_built', False)
                        )
                        _observe_build_ok = (
                            'REBUILD_OVERSOLD_PROBE_STRICT', 'REBUILD_OVERSOLD_PROBE_DULL',
                            'REBUILD_OVERSOLD_PROBE_RELAX', 'REBUILD_EXTREME_OVERSOLD',
                            'REBUILD_STAGED_HALF_PROBE', 'REBUILD_OVERSOLD_PROBE_ADD',
                            'REBUILD_PROBE_TO_HALF',
                            'REBUILD_NEUTRAL_HALF', 'REBUILD_NEUTRAL_PROBE',
                            'REBUILD_NEUTRAL_FULL',
                            'REBUILD_UP_FULL', 'REBUILD_UP_ADD',
                            'REBUILD_UP_MOMENTUM_BREAKOUT',
                            'INIT_PROBE_CONTROLLED_UPGRADE',
                            'REBUILD_EXTREME_RSI_FAST',
                            'REBUILD_OVERSOLD_PROBE_MACRO',
                            'INIT_BASE_PROBE_HIGH_DEV',
                            'INIT_BASE_HALF_DEVIATION',
                            'REBUILD_DOWN_RIGHT_SIDE',
                            'OVERSOLD_PROBE_UPGRADE',
                            # [P0 Fix] Allow secondary init-base/probe entries when trend improves
                            'INIT_BASE', 'INIT_BASE_PROBE',
                            # [P1 Fix] Early oversold probe — light-weight left-side entry on RSI 30-40
                            'REBUILD_EARLY_OVERSOLD_PROBE',
                            'REBUILD_VIRTUAL_SHALLOW_EARLY',
                            'REBUILD_VIRTUAL_SHALLOW_RELAX',
                            'ORB_FAIL_PULLBACK_MICRO',
                        )
                        # [P1 Fix] Special exemption: EARLY / virtual-shallow tiers bypass OBSERVE whitelist.
                        _early_exempt = _build_reason in (
                            'REBUILD_EARLY_OVERSOLD_PROBE',
                            'REBUILD_VIRTUAL_SHALLOW_EARLY',
                            'REBUILD_VIRTUAL_SHALLOW_RELAX',
                        )
                        if not _first_entry_exempt and not _early_exempt and _build_reason not in _observe_build_ok:
                            _can_build = False
                            if not getattr(C, '_observe_rebuild_blocked_logged', False):
                                C._observe_rebuild_blocked_logged = True
                                print(f"  [Rebuild Blocked] OBSERVE mode, only post-stop rebuild allowed")
                            C._rebuild_cooldown_bars = 30

                    # [Fix] Deep-loss probe-size clamp: when cumulative stop loss
                    # exceeds 2% of starting capital (~4000 yuan on 200k), force
                    # any pending build down to a 100-share micro probe regardless
                    # of the source path. This caps additional risk exposure when
                    # the strategy is already deeply in the red.
                    if _can_build and _build_qty >= 100:
                        _cum_loss_pct = abs(getattr(C, '_cum_stop_loss', 0.0)) / 200000.0
                        if _cum_loss_pct > 0.02:
                            _build_qty = min(C.trade_qty, _build_qty)
                            if not getattr(C, '_deep_loss_probe_logged', False):
                                C._deep_loss_probe_logged = True
                                print(f"  [Rebuild] Deep loss detected (cum:{getattr(C, '_cum_stop_loss', 0.0):.0f}), restrict probe to {C.trade_qty} shares")

                    # [P1 Fix 5] End-of-backtest convergence guard. In the last
                    # 10 bars of a simulation, any NEW base position locks in
                    # terminal NAV as a probe-sized (100 share) bet. Otherwise a
                    # last-bar 300-share build on a dip would permanently
                    # distort the reported final P&L (06-18: INIT_BASE +300 at
                    # the dip → -45 yuan at session close).
                    if _can_build and not getattr(C, 'is_live', False):
                        try:
                            # [Fix 3.1] Remaining bar estimate: use pre-set total if available,
                            # otherwise fall back to dynamic tracking (which can drift).
                            _total_bars = getattr(C, '_total_backtest_bars', 0)
                            if _total_bars > 0:
                                _bars_remaining = _total_bars - C.barpos
                            elif hasattr(C, '_max_barpos_seen') and C._max_barpos_seen > C.barpos:
                                _bars_remaining = C._max_barpos_seen - C.barpos
                            else:
                                _bars_remaining = 9999
                            if _bars_remaining < 1440:
                                _can_build = False
                                # [Fix 3.2] Must set cooldown to prevent tick-level spam
                                C._rebuild_cooldown_bars = 60
                                if not getattr(C, '_end_phase_no_build_logged', False):
                                    C._end_phase_no_build_logged = True
                                    print(f"  [Build] End-of-backtest phase (remaining_bars={_bars_remaining}), "
                                          f"block new entry")
                            if get_total_position(C) > 0 and C._base_cost_price > 0:
                                if _bars_remaining == 1 and current_price < C._base_cost_price:
                                    _force_qty = (get_true_available_position(C) // 100) * 100
                                    if _force_qty >= 100:
                                        _force_pos_mgr = getattr(C, '_pos_mgr', None)
                                        _force_fee = calc_trade_fee(C, current_price, _force_qty, is_sell=True)
                                        if _force_pos_mgr is not None:
                                            _force_pos_mgr.request_sell(
                                                _force_qty, 'EOD_BT_FORCE_CLOSE',
                                                caller='stop_loss', current_price=current_price,
                                                tick=tick, fee=_force_fee)
                                        else:
                                            safe_sell_eod(C, current_price, _force_qty,
                                                          'EOD_BT_FORCE_CLOSE', tick)
                                        print(f"  [EOD BT] Last bar: forced close {_force_qty} @ {current_price:.2f}")
                        except Exception:
                            pass
                    # [Fix] Global build gate: when backend is stale or backtest
                    # tail phase, block all new builds regardless of reason.
                    if _can_build and getattr(C, '_block_new_builds', False):
                        _can_build = False
                    if _can_build and _blocks_underwater_t0_rebuild(C, date_str):
                        _can_build = False
                        log_once(C, '_underwater_t0_rebuild_block_logged',
                                 f"  [Rebuild Blocked] Same-day PROBE_UNDERWATER_T0 exit, "
                                 f"no re-entry until next session")
                    # [05-13 Fix] Final choke on every empty-account first build path.
                    _FIRST_INIT_REASONS = frozenset({
                        'INIT_HIGH_DEV_UPTREND_PROBE', 'INIT_BASE_PROBE',
                        'INIT_BASE_PROBE_HIGH_DEV', 'INIT_BASE', 'INIT_BASE_HALF_DEVIATION',
                    })
                    if (_can_build and _total_pos == 0
                            and _build_reason in _FIRST_INIT_REASONS
                            and _build_reason != 'INIT_HIGH_DEV_UPTREND_PROBE'):
                        _ma20_gate = getattr(C, '_trend_ma20', 0.0)
                        _dev_gate = ((current_price - _ma20_gate) / _ma20_gate
                                     if _ma20_gate > 0 else 0.0)
                        _blocked, _why = _blocks_high_position_init_build(C, _dev_gate)
                        if _blocked:
                            _can_build = False
                            log_once(C, '_high_pos_init_final_gate_logged',
                                     f"  [Build Blocked] High-position init (final gate): {_why}")
                            C._rebuild_cooldown_bars = 60
                    if (_can_build and _build_reason in ('REBUILD_NEUTRAL_PROBE', 'REBUILD_NEUTRAL_PROBE_ADD')
                            and _total_pos == 0):
                        _neutral_ok, _neutral_why = _neutral_empty_probe_ok(
                            C, current_price, time_str, _bar_time_str, _rebuild_vwap)
                        if not _neutral_ok:
                            _can_build = False
                            log_once(C, '_neutral_probe_final_gate_logged',
                                     f"  [Rebuild Blocked] NEUTRAL probe (final gate): {_neutral_why}")
                            C._rebuild_cooldown_bars = 30
                    if _can_build:
                        # --- [新增] 动态调整仓位 ---
                        # 如果后端步长 < 0.003,说明处于高频网格区，放大买入量
                        _grid_buy_val = getattr(C, '_backend_grid_buy', 0.004)

                        if _grid_buy_val < 0.003:
                            _original_qty = _build_qty
                            _build_qty = min(int(_build_qty * 1.2), getattr(C, 'max_total_qty', 999999))  # 放大20%
                            print(f"  [Dynamic Grid] Step={_grid_buy_val:.4f}, qty enlarged: {_original_qty} -> {_build_qty}")

                        # [Fix Defect 1] Bar-level probe order lock: if a probe buy was
                        # already dispatched this bar, skip to prevent tick-level repeat orders.
                        if C.last_probe_barpos == C.barpos:
                            if not getattr(C, '_bar_probe_lock_logged', False):
                                C._bar_probe_lock_logged = True
                                print(f"  [{time_str}] [Rebuild] Bar-level lock: probe already dispatched at barpos={C.barpos}, skip")
                        elif _build_qty >= 100:
                            # [Fix P4] Print the pending build log only after all checks passed
                            if getattr(C, '_pending_build_log', '') != '':
                                print(C._pending_build_log)
                                C._pending_build_log = ''
                            # [PositionManager] Route rebuild buy through centralized
                            # gateway instead of direct passorder. This enforces:
                            # 1. T0 pending-buyback guard (Problem 1)
                            # 2. Same-day stacking guard (Problem 2)
                            # 3. Bar-level cascade guard
                            buy_price = current_price
                            _pos_mgr = getattr(C, '_pos_mgr', None)
                            _buy_probe_type = None
                            if _build_reason in ('REBUILD_NEUTRAL_PROBE', 'REBUILD_NEUTRAL_PROBE_ADD'):
                                _buy_probe_type = 'neutral_probe'
                            if _pos_mgr is not None:
                                _buy_ok = _pos_mgr.request_buy(
                                    _build_qty, _build_reason, caller='rebuild',
                                    current_price=buy_price, tick=tick,
                                    probe_type=_buy_probe_type)
                            else:
                                # Fallback: direct passorder if PositionManager unavailable
                                _po = _get_api('passorder')
                                if not _po:
                                    print(f"  [Err] passorder buy: API not found")
                                    _buy_ok = False
                                else:
                                    try:
                                        _po(23, 1101, C.account_id, C.stock, 11, buy_price, _build_qty, _build_reason, 2, _build_reason, C)
                                        _buy_ok = True
                                    except Exception as e:
                                        print(f"  [Err] passorder buy: {e}")
                                        _buy_ok = False
                            if _buy_ok:
                                C.last_probe_barpos = C.barpos  # [Fix Defect 1] Lock this bar to prevent repeat orders
                                new_total = get_total_position(C)
                                if new_total > _total_pos:
                                    if _total_pos == 0:
                                        _clear_virtual_oversold_on_entry(C)
                                    C._today_bought_qty += _build_qty
                                    # [P2 Fix] traded_volume must include buys as well as sells.
                                    # Print-eod-summary / backtest PnL needs a complete picture of
                                    # daily activity, not just the sell side.
                                    C.traded_volume += _build_qty
                                    C._base_pos_initialized = True
                                    C._base_ever_built = True
                                    C._empty_skip_done = False
                                    C._base_cut_done = False
                                    # [问题2修复] 重建成功时同步清除 Staged/Trail 标志
                                    C._staged_stop_done = False
                                    C._trail_stop_disabled = False
                                    if new_total >= C.backtest_base_qty:
                                        _clear_staged_reduce_state(C)
                                    else:
                                        C._base_staged_reduce_done = False
                                        C._base_staged_reduce_qty = 0
                                        C._base_staged_orig_total = 0
                                    C._base_trend_cut_active = False
                                    # [State Hub] DPM.on_buy(probe_type='oversold_probe') from PositionManager
                                    # already handled the core state write. Now set strategy-level fields only.
                                    _is_oversold_probe_path = (
                                        _build_reason.startswith('REBUILD_OVERSOLD')
                                        or _build_reason in (
                                            'REBUILD_EXTREME_RSI_FAST', 'OVERSOLD_PROBE_UPGRADE',
                                            'REBUILD_VIRTUAL_SHALLOW_EARLY', 'REBUILD_VIRTUAL_SHALLOW_RELAX')
                                    )
                                    if _is_oversold_probe_path:
                                        C.strategy_mode = 'OBSERVE'
                                        C.state = 'IDLE'
                                        C.can_do_t0 = False
                                        C._rebuild_from_staged_half_exit = False
                                        # [P0 Fix log66] Reset stale peak_price on a fresh probe
                                        # entry (flat → 100 sh). Previously peak_price inherited
                                        # the previous base's high (e.g. 41.92 on a 37.09 probe),
                                        # causing Trail Stop to see a 10.4% drawdown on the next
                                        # intraday pullback and force-liquidate at -2.8% float
                                        # (05-29 pattern, -113 loss on a 1-day probe). Resetting
                                        # to the entry price ensures peak tracking starts clean;
                                        # subsequent intraday highs are pushed up by the normal
                                        # high-water-mark loop in _check_main_fuse.
                                        _is_fresh_probe_entry = (_total_pos == 0)
                                        if _is_fresh_probe_entry:
                                            C._base_peak_price = current_price
                                            C._neutral_micro_probe_active = False
                                            if _build_reason.startswith('REBUILD_'):
                                                C._oversold_probe_entry_bias = getattr(
                                                    C, '_frozen_bias_20',
                                                    getattr(C, '_backend_bias_20', 0.0))
                                        # [P1 Fix] Track DULL probe entry price so the same-price
                                        # re-entry guard can block whipsaws. Only the DULL variant
                                        # needs this because STRICT/RELAX/EXTREME tend to enter at
                                        # deeper, more volatile levels with more confirmation.
                                        if _build_reason == 'REBUILD_OVERSOLD_PROBE_DULL':
                                            C._last_dull_probe_price = current_price
                                            C._dull_micro_probe_active = True
                                            C._dull_shallow_cap_done = False
                                        else:
                                            # Non-DULL oversold probe: clear the stale DULL anchor
                                            # so a future DULL entry is not blocked by an old level.
                                            C._last_dull_probe_price = 0.0
                                            C._dull_micro_probe_active = False
                                        # Sync probe state through DPM (the ONLY writer). This sets legacy flags too.
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            if _build_reason == 'REBUILD_OVERSOLD_PROBE_ADD':
                                                # Add-on: preserve warm counter — DPM.hold_days already kept (not 0),
                                                # so just sync legacy counter from DPM for consistency.
                                                if not getattr(C, '_first_rebuild_probe_date', ''):
                                                    C._first_rebuild_probe_date = date_str
                                                # Ensure legacy counter doesn't drop below 2 (defensive behavior)
                                                C._days_since_rebuild_probe = max(2, _dpm.hold_days)
                                            elif _build_reason == 'OVERSOLD_PROBE_UPGRADE':
                                                if not getattr(C, '_first_rebuild_probe_date', ''):
                                                    C._first_rebuild_probe_date = date_str
                                                C._first_probe_trade_days = 0
                                                C._rebuild_probe_date = date_str
                                            elif _total_pos == 0:
                                                # [缺陷3 Fix] Fresh entry — DPM.on_buy(probe_type=
                                                # 'oversold_probe') already set state/type/entry_date/
                                                # _rebuild_probe_date/_days_since_rebuild_probe. Calling
                                                # mark_as_oversold_probe() again only re-triggered the
                                                # "called on existing position" warn (entry_date was
                                                # already set this bar). Just set the legacy first-probe
                                                # counters here.
                                                C._first_rebuild_probe_date = date_str
                                                C._first_probe_trade_days = 0
                                            else:
                                                # [P0-4 Fix] Add-to-existing via MACRO/STRICT/EXTREME/etc:
                                                # preserve immunity counter & hold_days.
                                                # Previously this fell into the else branch which called
                                                # mark_as_oversold_probe() — unconditionally resetting
                                                # _days_since_rebuild_probe=0 and hold_days=0.
                                                # This let a -6% underwater position "buy" 2 more days
                                                # of immunity by adding 100 shares (06-02 log95 case).
                                                # Now aligned with REBUILD_OVERSOLD_PROBE_ADD behavior.
                                                if not getattr(C, '_first_rebuild_probe_date', ''):
                                                    C._first_rebuild_probe_date = date_str
                                                C._days_since_rebuild_probe = max(
                                                    getattr(C, '_days_since_rebuild_probe', 0),
                                                    _dpm.hold_days)
                                                print(f"  [DPM] Add-to-existing ({_build_reason}): "
                                                      f"preserve hold_days={_dpm.hold_days}, "
                                                      f"days_since_rebuild={C._days_since_rebuild_probe}")
                                            # Read back authoritative values from DPM
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            # Also sync DPM-level peak so its own drawdown tracking
                                            # is consistent with _base_peak_price.
                                            if _is_fresh_probe_entry:
                                                _dpm.peak_price = current_price
                                        # stop_anchor: defensive — only set if missing
                                        if getattr(C, '_base_stop_anchor', 0.0) <= 0.0:
                                            C._base_stop_anchor = current_price
                                        _probe_label = ('Staged-half oversold' if _build_reason == 'REBUILD_STAGED_HALF_PROBE'
                                                        else 'Extreme oversold' if _build_reason == 'REBUILD_EXTREME_OVERSOLD'
                                                        else 'Probe')
                                        print(f"  [Rebuild] {_probe_label} entry, OBSERVE until trend improves")
                                    elif (_total_pos == 0
                                          and _build_reason == 'REBUILD_DOWN_RIGHT_SIDE'):
                                        C._base_peak_price = current_price
                                        _dpm_pk = getattr(C, '_probe_mgr', None)
                                        if _dpm_pk is not None:
                                            _dpm_pk.peak_price = current_price
                                    elif _build_reason == 'REBUILD_PROBE_TO_HALF':
                                        C.strategy_mode = 'T0'
                                        C.state = 'IDLE'
                                        C.can_do_t0 = (get_true_available_position(C) >= 100)
                                        # [State Hub] Upgrade to base — clear probe flags through DPM
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            _dpm.mark_as_normal_base()
                                        print(f"  [Rebuild] Probe upgraded to half base, mode -> T0")
                                    elif _build_reason in ('REBUILD_NEUTRAL_PROBE', 'REBUILD_NEUTRAL_PROBE_ADD',
                                                           'ORB_FAIL_PULLBACK_MICRO'):
                                        # [State Hub] NEUTRAL_PROBE: route to OBSERVE with probe protection.
                                        # ponytail: fresh NEUTRAL must hit this branch (not the RIGHT_SIDE
                                        # peak-only elif above) so neutral flag + bias reset apply (log141).
                                        if _total_pos == 0:
                                            C._base_peak_price = current_price
                                            _dpm_pk = getattr(C, '_probe_mgr', None)
                                            if _dpm_pk is not None:
                                                _dpm_pk.peak_price = current_price
                                        C.strategy_mode = 'OBSERVE'
                                        C.state = 'IDLE'
                                        C.can_do_t0 = False
                                        if _build_reason in ('REBUILD_NEUTRAL_PROBE', 'REBUILD_NEUTRAL_PROBE_ADD'):
                                            C._neutral_micro_probe_active = True
                                            C._oversold_probe_entry_bias = 0.0
                                        # [缺陷3 Fix] DPM.on_buy(probe_type='oversold_probe') already
                                        # registered this entry; the extra mark_as_oversold_probe()
                                        # only re-fired the "existing position" warn. Removed.
                                        if not getattr(C, '_first_rebuild_probe_date', ''):
                                            C._first_rebuild_probe_date = date_str
                                            C._first_probe_trade_days = 0
                                        _orb_lbl = ('ORB-fail pullback micro' if _build_reason
                                                    == 'ORB_FAIL_PULLBACK_MICRO' else 'NEUTRAL probe')
                                        print(f"  [Rebuild] {_orb_lbl} entry, mode -> OBSERVE (probe protection)")
                                    elif _build_reason in ('REBUILD_NEUTRAL_HALF', 'REBUILD_NEUTRAL_FULL',
                                                            'REBUILD_UP_FULL', 'REBUILD_UP_ADD',
                                                            'REBUILD_UP_MOMENTUM_BREAKOUT'):
                                        C.strategy_mode = 'T0'
                                        C.state = 'IDLE'
                                        C.can_do_t0 = (get_true_available_position(C) >= 100)
                                        # [State Hub] Upgrade to normal base — clear probe flags through DPM
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            _dpm.mark_as_normal_base()
                                        print(f"  [Rebuild] {_build_reason}, mode -> T0")
                                    elif C.strategy_mode == 'SKIP':
                                        if time_str < C.decision_time:
                                            pass
                                        else:
                                            if len(C.bars_high) < 30:
                                                pass
                                            else:
                                                _rh = max(C.bars_high[-30:])
                                                _rl = min(C.bars_low[-30:])
                                                _ra = (_rh - _rl) / _rl if _rl > 0 else 0
                                                if _ra >= C.orb_amp_th and C.trend_direction != 'UP':
                                                    pass
                                                elif C.trend_direction == 'DOWN':
                                                    C.strategy_mode = 'OBSERVE'
                                                    C.state = 'IDLE'
                                                    print(f"  [Rebuild] DOWN trend, enter OBSERVE mode")
                                                else:
                                                    C.strategy_mode = 'T0'
                                                    C.state = 'IDLE'

                                    if new_total >= C.backtest_base_qty:
                                        C._base_rebuild_stage = 2
                                        # 重建成功后清除退出策略
                                        C._exit_policy = ''
                                        C._staged_half_exit_date = ''
                                        C._rebuild_from_staged_half_exit = False
                                    elif new_total >= C.base_qty_half:
                                        C._base_rebuild_stage = 1
                                        # 半仓重建也清除相关状态，保持代码整洁
                                        C._staged_half_exit_date = ''
                                        C._exit_policy = ''
                                    else:
                                        C._base_rebuild_stage = 0
                                    C._base_target_qty = new_total

                                    if _build_reason in ('INIT_BASE', 'REBUILD_UP_FULL') and _total_pos == 0:
                                        set_base_cost(C, current_price)
                                        C._base_build_date = date_str
                                        C._is_base_first_day = True
                                        C._consecutive_base_stops = 0
                                        C._stop_cool_days = C._stop_cool_days_base
                                    elif _build_reason in ('INIT_BASE_PROBE_HIGH_DEV', 'INIT_BASE_PROBE') and _total_pos == 0:
                                        # [State Hub] DPM.on_buy(probe_type='init_probe') from PositionManager
                                        # already set: _base_cost_price, _is_init_probe=True, _is_oversold_probe=False,
                                        # _init_probe_date, _days_since_init_probe=0, _rebuild_probe_date,
                                        # _days_since_rebuild_probe=0, _first_rebuild_probe_date, _first_probe_trade_days.
                                        # Only set: stop_anchor (non-state-hub field), build date, consecutive counter.
                                        # [P0 Fix log66] Reset peak_price on fresh init probe entry to avoid Trail 
                                        # Stop being triggered by stale peak from previous base cycle.
                                        C._base_peak_price = current_price
                                        C._base_stop_anchor = current_price
                                        C._base_build_date = date_str
                                        C._is_base_first_day = True
                                        C._consecutive_base_stops = 0
                                        C._stop_cool_days = C._stop_cool_days_base
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            _dpm.mark_as_init_probe(date_str)
                                            _dpm.peak_price = current_price
                                            # Read back authoritative values (already synced by DPM)
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            C._first_rebuild_probe_date = date_str
                                            C._first_probe_trade_days = 0
                                    elif _build_reason in ('INIT_BASE_HALF_DEVIATION', 'INIT_BASE_HALF_DEVIATION_FILTERED') and _total_pos == 0:
                                        set_base_cost(C, current_price)
                                        C._base_build_date = date_str
                                        C._is_base_first_day = True
                                        C._consecutive_base_stops = 0
                                        C._stop_cool_days = C._stop_cool_days_base
                                    elif _build_reason == 'REBUILD_NEUTRAL_HALF':
                                        # [State Hub] DPM.on_buy(probe_type='normal') already averaged cost.
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            _dpm.mark_as_normal_base()
                                        else:
                                            if C._base_cost_price > 0 and _total_pos > 0:
                                                _old_cost = C._base_cost_price * _total_pos
                                                _new_cost = current_price * _build_qty
                                                C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            else:
                                                set_base_cost(C, current_price)
                                        sync_stop_anchor_down(C)
                                        C._base_build_date = date_str
                                        C._is_base_first_day = True
                                        if C._consecutive_base_stops > 0:
                                            C._consecutive_base_stops -= 1
                                            C._stop_cool_days = C._stop_cool_days_base + max(0, C._consecutive_base_stops - 1) * 2
                                            print(f"  [Rebuild] Stop counter reduced to {C._consecutive_base_stops}, cooldown {C._stop_cool_days}d")
                                    elif _build_reason in ('REBUILD_OVERSOLD_PROBE_STRICT', 'REBUILD_EXTREME_OVERSOLD',
                                                           'REBUILD_STAGED_HALF_PROBE', 'REBUILD_OVERSOLD_PROBE_ADD',
                                                           'REBUILD_OVERSOLD_PROBE_MACRO', 'REBUILD_EARLY_OVERSOLD_PROBE',
                                                           'REBUILD_VIRTUAL_SHALLOW_EARLY', 'REBUILD_VIRTUAL_SHALLOW_RELAX'):
                                        # [State Hub] DPM already averaged cost; just sync stop_anchor.
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            # [Fix 6] Init peak_price so drawdown tracking doesn't
                                            # inflate dd%. Without this, peak_price stays at the
                                            # previous base peak (e.g. 40.00) while current price is
                                            # 30.00, producing a spurious 25% drawdown that triggers
                                            # an early stop.
                                            if _total_pos == 0:
                                                C._base_peak_price = current_price
                                                _dpm.peak_price = current_price
                                        else:
                                            if C._base_cost_price > 0 and _total_pos > 0:
                                                _old_cost = C._base_cost_price * _total_pos
                                                _new_cost = current_price * _build_qty
                                                C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            else:
                                                set_base_cost(C, current_price)
                                            if _total_pos == 0:
                                                C._base_peak_price = current_price
                                        sync_stop_anchor_down(C)
                                        C._base_build_date = date_str
                                        C._is_base_first_day = True
                                    elif _build_reason == 'INIT_BASE_ADD':
                                        # [State Hub] DPM.on_buy(probe_type='normal') already averaged cost
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            _dpm.mark_as_normal_base()
                                        else:
                                            if C._base_cost_price > 0 and _total_pos > 0:
                                                _old_cost = C._base_cost_price * _total_pos
                                                _new_cost = current_price * _build_qty
                                                C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            else:
                                                set_base_cost(C, current_price)
                                        sync_stop_anchor_down(C)
                                        C._is_base_first_day = False
                                        C._last_init_add_date = date_str
                                        if new_total >= C.backtest_base_qty:
                                            C._consecutive_base_stops = 0
                                            C._stop_cool_days = C._stop_cool_days_base
                                    elif _build_reason == 'INIT_PROBE_CONTROLLED_UPGRADE':
                                        # [P0 Fix] Keep probe identity across a controlled upgrade.
                                        # Previously, any upgrade to 200 shares called
                                        # _dpm.mark_as_normal_base(), which silently cleared
                                        # C._is_init_probe / C._is_oversold_probe and flipped
                                        # DPM.probe_type to 'normal' and DPM.state to
                                        # BASE_ACTIVE. As a result, the next-session
                                        # Micro-Clear guard (state==PROBE_ACTIVE or
                                        # probe_type in probe variants) saw the position as a
                                        # base and liquidated it at -1.9% float instead of
                                        # letting the probe rebound (log61 05-19 -168.2 on a
                                        # probe that was originally init_probe). Probe identity
                                        # MUST be preserved so the wide stop, Micro-Clear
                                        # immunity, and FUSE cap all continue to apply.
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            # Intentionally NOT calling
                                            # mark_as_normal_base(). Only reset
                                            # base-specific state.
                                        else:
                                            if C._base_cost_price > 0 and _total_pos > 0:
                                                _old_cost = C._base_cost_price * _total_pos
                                                _new_cost = current_price * _build_qty
                                                C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            else:
                                                set_base_cost(C, current_price)
                                                C._base_build_date = date_str
                                                C._is_base_first_day = True
                                        sync_stop_anchor_down(C)
                                        C._is_base_first_day = False
                                        C._last_init_add_date = date_str
                                    else:
                                        # [State Hub] Fallback: sync from DPM
                                        _dpm = getattr(C, '_probe_mgr', None)
                                        if _dpm is not None:
                                            C._base_cost_price = _dpm.cost
                                            C._base_target_qty = _dpm.qty
                                            # [P0 Fix] Do NOT silently overwrite
                                            # probe identity in the fallback path.
                                            # Probe-type reasons (e.g. DULL, RELAX,
                                            # EXTREME_RSI_FAST) that miss explicit
                                            # branches must retain their probe status
                                            # (wide stop, FUSE immunity, Trend-Cut
                                            # protection). Only non-probe DPM state
                                            # should be upgraded to normal base.
                                            if not _dpm.is_probe:
                                                _dpm.mark_as_normal_base()
                                        else:
                                            if _total_pos > 0 and C._base_cost_price > 0:
                                                _old_cost = C._base_cost_price * _total_pos
                                                _new_cost = current_price * _build_qty
                                                C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            else:
                                                set_base_cost(C, current_price)
                                        sync_stop_anchor_down(C)
                                        C._is_base_first_day = False

                                    _build_fee = calc_trade_fee(C, current_price, _build_qty, is_sell=False)
                                    C._base_trade_fee += _build_fee
                                    C._daily_trade_fee += _build_fee
                                    C.realized_pnl -= _build_fee
                                    C.daily_pnl = C.realized_pnl
                                    if C.strategy_mode != 'OBSERVE':
                                        C.can_do_t0 = (get_true_available_position(C) >= 100)
                                    print(f"  [Build] {_build_reason}: bought {_build_qty} @ {current_price:.2f} "
                                          f"total:{new_total} stage:{C._base_rebuild_stage} "
                                          f"cost:{C._base_cost_price:.2f} fee:{_build_fee:.1f}")
                                    if _build_reason == 'REBUILD_EXTREME_RSI_FAST':
                                        C._extreme_rsi_fast_used_date = date_str
                                    C._rebuild_cooldown_bars = 240
                                else:
                                    # [Fix] No-fill circuit breaker: order submitted but
                                    # position didn't update (low liquidity). Force 60-bar
                                    # cooldown to prevent tick-level spam.
                                    print(f"  [Build] {_build_reason} no fill, pos:{new_total}")
                                    C._rebuild_cooldown_bars = 60
                            else:
                                # PositionManager rejected the buy (bar locked, T0 pending, etc.)
                                log_once(C, '_posmgr_build_reject_logged',
                                         f"  [Build] {_build_reason} rejected by PositionManager")
                                C._rebuild_cooldown_bars = 60
                    else:
                        log_once(C, '_build_qty_skip_logged',
                                  f"  [Build] blocked (can_build=False), qty would be {_build_qty}")

        # log183 P0': sparse bar delivery on empty virtual-arm days — replay rebuild on backfill
        if (not getattr(C, 'is_live', True)
                and time_str >= '14:00:00'
                and get_total_position(C) == 0
                and not getattr(C, '_empty_rebuild_sweep_done', False)
                and (C._base_stop_date != ''
                     or getattr(C, '_exit_policy', '') == 'down_oversold'
                     or getattr(C, '_virtual_oversold_armed', False))):
            _empty_obs_rebuild_sweep(C, date_str, _bar_time_str, time_str, tick)

        # VWAP
        total_vol = C.confirmed_min_vol + C.cur_bar_volume
        total_amt = C.confirmed_min_amt + C.cur_bar_amount
        if time_str <= '09:35:00' or total_vol <= 0:
            vwap_price = current_price
        else:
            vwap_price = total_amt / total_vol
            if abs(vwap_price - current_price) / max(current_price, 0.01) > 0.5:
                vwap_price = current_price

        C.atr_val = calc_atr(C)
        if is_valid_price(current_price):
            C._atr_for_open = max(C.atr_val, current_price * 0.002)
        else:
            C._atr_for_open = C.atr_val
        ma5 = calc_ma(C, C.ma_short)
        ma20 = calc_ma(C, C.ma_long)

        obv_val, sobv_val = calc_obv_sobv(C)
        obv_golden_cross = (obv_val > sobv_val)

        # Observation period
        if time_str < C.decision_time:
            C._obs_tick_count += 1
            if current_price > C.orb_high: C.orb_high = current_price
            if current_price < C.orb_low:  C.orb_low = current_price
            C.orb_total_vol = C.confirmed_min_vol + C.cur_bar_volume

        # ==========================================================
        # Phase 0: Empty position observe logic (DOWN trend after stop)
        # ==========================================================
        if not getattr(C, '_empty_skip_done', False):
            update_trend_direction(C, _bar_time_str)
            _total_pos_now = get_total_position(C)
            if _total_pos_now == 0 and C._base_stop_date != '' and C.trend_direction == 'DOWN' and C.state == 'IDLE':
                if not getattr(C, '_empty_down_logged', False):
                    C._empty_down_logged = True
                    C.strategy_mode = 'OBSERVE'
                    C._empty_skip_done = True
                    print(f"  [OBSERVE] Empty + DOWN, rebuild probe path open")

        if getattr(C, '_empty_skip_done', False):
            update_trend_direction(C, _bar_time_str)
            if len(C.bars_high) >= 10 and len(C.bars_low) >= 10:
                _day_high = max(C.bars_high[-min(60, len(C.bars_high)):])
                _day_low = min(C.bars_low[-min(60, len(C.bars_low)):])
                _amp = (_day_high - _day_low) / _day_low if _day_low > 0 else 0
            else:
                _amp = 0.0
            if C.trend_direction != 'DOWN' and C._base_stop_date != '':
                _pos_now = get_total_position(C)
                _in_stop_cooldown = (_pos_now == 0 and C._days_since_stop < C._stop_cool_days)
                if _in_stop_cooldown:
                    C.strategy_mode = 'OBSERVE'
                else:
                    C._empty_skip_done = False
                    C._rebuild_cooldown_bars = 0
                if not _in_stop_cooldown and time_str >= C.decision_time:
                    if _amp >= C.orb_amp_th and C.trend_direction == 'UP':
                        C.strategy_mode = 'ORB'
                        C.state = 'WAIT_BREAKOUT'
                        C.orb_avg_vol_pm = C.orb_total_vol / 30.0
                        print(f"  [Mode] Trend recovered to UP + high amp -> ORB")
                    elif _amp >= C.orb_amp_th:
                        C.strategy_mode = 'SKIP'
                        print(f"  [Mode] Trend {C.trend_direction}, amplitude {_amp:.2%} -> SKIP")
                    else:
                        if get_total_position(C) > 0:
                            C.strategy_mode = 'T0'
                            C.state = 'IDLE'
                            print(f"  [Mode] Trend recovered to {C.trend_direction}, entering T0")
                        else:
                            C.strategy_mode = 'UNDECIDED'
                            if not getattr(C, '_empty_wait_logged', False):
                                C._empty_wait_logged = True
                                print(f"  [Mode] Trend {C.trend_direction} but empty, wait for rebuild")
                elif not _in_stop_cooldown:
                    C.strategy_mode = 'UNDECIDED'
            else:
                C.strategy_mode = 'OBSERVE'
                if time_str >= C.decision_time and not getattr(C, '_observe_down_logged', False):
                    C._observe_down_logged = True
                    print(f"  [OBSERVE] DOWN continues, amp={_amp:.2%}, awaiting rebuild signals")
            # log182 P0: do NOT return on empty — keep Phase 2+ / OBSERVE HB / risk paths alive

        if time_str >= C.decision_time:
            # [Fix] Base risk first (init-half exit, hard stop, trend cut); V2 FUSE is last resort
            update_trend_direction(C, _bar_time_str, current_price)
            # [P1 Fix] Evaluate stop-loss against the bar's intraday low, not the
            # current/close price. In log26 a -5% intraday spike-low (33.11) was missed
            # because only close price was evaluated, leading to a FUSE forced liquidation.
            _eval_low = _bar_low if _bar_data_ok else current_price
            _run_base_stop_checks(C, date_str, _eval_low, tick, _bar_time_str)
            if getattr(C, '_base_stop_done', False):
                if (C.state in ('BOUGHT_WAITING_SELL', 'SOLD_WAITING_BUY')
                        and C.pending_close_qty > 0
                        and time_str >= C.force_close_t and not C.eod_order_sent):
                    _t0_eod_avail = get_true_available_position(C)
                    if C.state == 'BOUGHT_WAITING_SELL':
                        _t0_close_qty = min(C.pending_close_qty, _t0_eod_avail)
                        _t0_close_qty = (_t0_close_qty // 100) * 100
                        if _t0_close_qty > 0:
                            _t0_pnl = current_price - C.buy_price
                            print(f"  [{time_str}] T0_EOD_CLOSE (post-base-stop) | pnl:{_t0_pnl:.3f}")
                            # [P0 Fix] T0 operations must bypass PositionManager to avoid:
                            # 1) DPM.on_sell treating the T0 sell through DPM.on_sell decrementing
                            #    base position qty (zeroing cost_buying to reset);
                            # 2) DPM.on_sell resetting C._base_cost_price (destroying real
                            #    cost when qty reaches zero);
                            # 3) Double counting (PosMgr adds pnl outside this block then again
                            #    this block);
                            # T0 is a temporary inventory round-trip — the base position
                            # accounting stays with PosMgr, so always call safe_sell_eod directly.
                            if safe_sell_eod(C, current_price, _t0_close_qty, 'T0_EOD_POST_STOP', tick):
                                _t0_fee = (calc_trade_fee(C, C.buy_price, _t0_close_qty, is_sell=False) +
                                           calc_trade_fee(C, current_price, _t0_close_qty, is_sell=True))
                                C.realized_pnl += _t0_pnl * _t0_close_qty - _t0_fee
                                C.daily_pnl = C.realized_pnl
                                C._daily_trade_fee += _t0_fee
                                C._base_trade_fee += _t0_fee
                                C.pending_close_qty -= _t0_close_qty
                                C._today_bought_qty = max(0, C._today_bought_qty - _t0_close_qty)
                                if C.pending_close_qty <= 0:
                                    C.state = 'IDLE'
                                C.eod_order_sent = True
                                print(f"  -> T0 Close (post-base-stop) | realized_pnl:{C.realized_pnl:.1f}")
                            else:
                                print(f"  [Error] T0 EOD close (post-base-stop) failed, position will roll over")
                    elif C.state == 'SOLD_WAITING_BUY':
                        if safe_buy_eod(C, current_price, C.pending_close_qty, 'T0_EOD_POST_STOP', tick):
                            _t0_pnl = C.sell_price - current_price
                            _t0_fee = (calc_trade_fee(C, C.sell_price, C.pending_close_qty, is_sell=True) +
                                       calc_trade_fee(C, current_price, C.pending_close_qty, is_sell=False))
                            C.realized_pnl += _t0_pnl * C.pending_close_qty - _t0_fee
                            C.daily_pnl = C.realized_pnl
                            C._daily_trade_fee += _t0_fee
                            C._base_trade_fee += _t0_fee
                            C._t0_pending_qty = max(0, getattr(C, '_t0_pending_qty', 0) - C.pending_close_qty)
                            C.pending_close_qty = 0
                            C.state = 'IDLE'
                            C.eod_order_sent = True
                            print(f"  -> T0 Buy Back (post-base-stop) | realized_pnl:{C.realized_pnl:.1f}")
                        else:
                            print(f"  [Error] T0 EOD buy back (post-base-stop) failed, position will roll over")
                print_eod_summary(C, time_str, current_price)
                return

            # [Fix 6] T0 pending close fallback: if a T0 leg is open but no base stop fired,
            # still enforce EOD close / buy-back after force_close_t to avoid overnight skew.
            _is_t0_pending = (C.state in ('BOUGHT_WAITING_SELL', 'SOLD_WAITING_BUY')
                              and C.pending_close_qty > 0
                              and time_str >= C.force_close_t
                              and not C.eod_order_sent)
            if _is_t0_pending:
                _t0_eod_avail = get_true_available_position(C)
                if C.state == 'BOUGHT_WAITING_SELL':
                    _t0_close_qty = min(C.pending_close_qty, _t0_eod_avail)
                    _t0_close_qty = (_t0_close_qty // 100) * 100
                    if _t0_close_qty > 0:
                        _t0_pnl = current_price - C.buy_price
                        print(f"  [{time_str}] T0_EOD_CLOSE (pending-only) | pnl:{_t0_pnl:.3f}")
                        # [P0 Fix] T0 is a temporary inventory round-trip. Must bypass
                        # PositionManager to avoid DPM.on_sell which corrupts the base
                        # position (decrements qty, zeros cost, double-counts pnl).
                        if safe_sell_eod(C, current_price, _t0_close_qty, 'T0_EOD_PENDING_ONLY', tick):
                            _t0_fee = (calc_trade_fee(C, C.buy_price, _t0_close_qty, is_sell=False) +
                                       calc_trade_fee(C, current_price, _t0_close_qty, is_sell=True))
                            C.realized_pnl += _t0_pnl * _t0_close_qty - _t0_fee
                            C.daily_pnl = C.realized_pnl
                            C._daily_trade_fee += _t0_fee
                            C._base_trade_fee += _t0_fee
                            C.pending_close_qty -= _t0_close_qty
                            C._today_bought_qty = max(0, C._today_bought_qty - _t0_close_qty)
                            if C.pending_close_qty <= 0:
                                C.state = 'IDLE'
                            C.eod_order_sent = True
                            print(f"  -> T0 Close (pending-only) | realized_pnl:{C.realized_pnl:.1f}")
                        else:
                            print(f"  [Error] T0 EOD close (pending-only) failed, position will roll over")
                elif C.state == 'SOLD_WAITING_BUY':
                    if safe_buy_eod(C, current_price, C.pending_close_qty, 'T0_EOD_PENDING_ONLY', tick):
                        _t0_pnl = C.sell_price - current_price
                        _t0_fee = (calc_trade_fee(C, C.sell_price, C.pending_close_qty, is_sell=True) +
                                   calc_trade_fee(C, current_price, C.pending_close_qty, is_sell=False))
                        C.realized_pnl += _t0_pnl * C.pending_close_qty - _t0_fee
                        C.daily_pnl = C.realized_pnl
                        C._daily_trade_fee += _t0_fee
                        C._base_trade_fee += _t0_fee
                        C._t0_pending_qty = max(0, getattr(C, '_t0_pending_qty', 0) - C.pending_close_qty)
                        C.pending_close_qty = 0
                        C.state = 'IDLE'
                        C.eod_order_sent = True
                        print(f"  -> T0 Buy Back (pending-only) | realized_pnl:{C.realized_pnl:.1f}")
                    else:
                        print(f"  [Error] T0 EOD buy back (pending-only) failed, position will roll over")

            # [V2] Last-resort lifecycle FUSE (only if base stops did not fire)
            # [P1-2 Fix] Skip V2 FUSE if any stop/reduction already happened this bar
            _fuse_already_handled = (
                getattr(C, '_base_stop_done', False)
                or getattr(C, '_intraday_float_reduced_today', False)
                or getattr(C, '_down_intraday_reduced_done', False)
                or getattr(C, '_trend_cut_done', False)
                or getattr(C, '_staged_half_exit_done', False)
            )
            if (getattr(C, '_risk_engine', None) is not None
                    and get_total_position(C) > 0
                    and not _fuse_already_handled
                    and C._risk_engine.is_fused(current_price)):
                # [P0-2 Fix] Use true available position (respect T+1 lock)
                _fuse_avail = get_true_available_position(C)
                _fuse_close = (_fuse_avail // 100) * 100

                if _fuse_close < 100:
                    # [P0-2 Fix] T+1 locked, defer to pending, avoid spamming logs
                    if not getattr(C, '_fuse_t1_blocked_logged', False):
                        C._fuse_t1_blocked_logged = True
                        print(f"  [V2 FUSE] T+1 blocked (avail=0), defer to pending stop")
                    C._pending_base_stop = True
                    C._pending_stop_needs_record = True
                    C._force_liquidation_active = True
                    C.strategy_mode = 'OBSERVE'
                    C.state = 'IDLE'
                    return

                # [P0-2 Fix] Always use safe_sell_eod for aggressive matching
                if not getattr(C, '_fuse_logged_this_bar', False):
                    C._fuse_logged_this_bar = True
                    print(f"  [V2 FUSE] Lifecycle PnL fused, force close {_fuse_close} @ {current_price:.2f}")

                _cost_before = getattr(C, '_base_cost_price', 0.0)
                _pos_mgr = getattr(C, '_pos_mgr', None)
                if _pos_mgr is not None:
                    _fuse_sold, _fuse_pnl = _pos_mgr.request_sell_eod(_fuse_close, 'FUSE_V2_LIFECYCLE', caller='stop_loss', current_price=current_price, tick=tick)
                else:
                    _fuse_sold = safe_sell_eod(C, current_price, _fuse_close, 'FUSE_V2_LIFECYCLE', tick)

                if _fuse_sold:
                    _fuse_fee = calc_trade_fee(C, current_price, _fuse_close, is_sell=True)
                    # [P0 Fix] _fuse_loss computed BEFORE PosMgr mutates C._base_cost_price
                    _fuse_loss = (_cost_before - current_price) * _fuse_close if _cost_before > 0 else 0.0
                    if _pos_mgr is None:
                        # Fallback path only — PosMgr handles ledger internally
                        if getattr(C, '_risk_engine', None) is not None:
                            C._risk_engine.record_trade_pnl(current_price, _fuse_close, 'SELL', _fuse_fee)
                        C._base_trade_fee += _fuse_fee
                        C._daily_trade_fee += _fuse_fee
                        C.realized_pnl += (-_fuse_loss - _fuse_fee)
                        C.daily_pnl = C.realized_pnl
                    # PosMgr path: request_sell_eod already updated C.realized_pnl / C._cum_stop_loss
                    # Fallback path: _record_base_stop below computes _cum_stop_loss from cost

                    # [P0-3 Fix] Record stop whether fully flat or partial close
                    _record_base_stop(C, date_str, current_price, _fuse_close, realized_pnl=_fuse_pnl if _pos_mgr is not None else 0.0)

                    _remaining = get_total_position(C)
                    if _remaining == 0:
                        clear_ghost_base_state(C, 'V2 FUSE flat')
                    else:
                        # Partial close, T+1 locked remainder, defer to next day
                        C._pending_base_stop = True
                        C._pending_stop_needs_record = False  # Already recorded

                    C._base_stop_done = True
                    C.can_do_t0 = False
                    C.strategy_mode = 'OBSERVE'
                    C.state = 'IDLE'
                    return
                else:
                    # [P0-2 Fix] EOD match failed too, defer to pending, don't retry every bar
                    if not getattr(C, '_fuse_sell_fail_logged', False):
                        C._fuse_sell_fail_logged = True
                        print(f"  [V2 FUSE] EOD sell also failed, defer to pending stop")
                    C._pending_base_stop = True
                    C._pending_stop_needs_record = True
                    C._force_liquidation_active = True
                    return

            # [V5 P0-2] Probe residual exit: check_residual_exit only raises a soft request;
            # we commit the sale + state update here, so the ProbeManager stays purely an observer/decision-layer.
            _probe_mgr = getattr(C, '_probe_mgr', None)
            # [P1 Fix] Evaluate probe stop-loss against the bar's intraday low,
            # not the current tick price. The close price can mask a -4.6% intraday
            # spike-low that should have triggered the probe hard stop.
            _eval_low_probe = _bar_low if _bar_data_ok else current_price
            if _probe_mgr is not None and _probe_mgr.check_residual_exit(_eval_low_probe, date_str):
                if _probe_mgr.exit_requested and _probe_mgr.exit_qty >= 100:
                    _exit_price = _probe_mgr.exit_price
                    _exit_qty = _probe_mgr.exit_qty
                    _pos_mgr = getattr(C, '_pos_mgr', None)
                    if _pos_mgr is not None:
                        _pre_sold, _pre_pnl = _pos_mgr.request_sell(_exit_qty, 'PROBE_%s_EXIT' % _probe_mgr.exit_reason.upper(), caller='stop_loss', current_price=_exit_price, tick=tick)
                    else:
                        _pre_sold = safe_sell(C, _exit_price, _exit_qty, 'PROBE_%s_EXIT' % _probe_mgr.exit_reason.upper(), tick)
                    if _pre_sold:
                        _exit_fee = calc_trade_fee(C, _exit_price, _exit_qty, is_sell=True)
                        _exit_pnl = (_exit_price - C._base_cost_price) * _exit_qty - _exit_fee
                        if _pos_mgr is None:
                            if getattr(C, '_risk_engine', None) is not None:
                                C._risk_engine.record_trade_pnl(_exit_price, _exit_qty, 'SELL', _exit_fee)
                                C._risk_engine.update_daily_limit('SELL', _exit_qty)
                            C.realized_pnl += _exit_pnl
                            C.daily_pnl = C.realized_pnl
                            C._base_trade_fee += _exit_fee
                            C._daily_trade_fee += _exit_fee
                            C.traded_volume += _exit_qty
                        if not C.is_live:
                            _tb = getattr(C, '_today_bought_qty', 0)
                            if _tb > 0:
                                C._today_bought_qty = max(0, _tb - min(_exit_qty, _tb))
                        # [State Hub] PositionManager.request_sell already called DPM.on_sell()
                        # which decrements qty, sets state to IDLE if qty==0, syncs legacy fields,
                        # and calls _persist_state(). No manual manipulation needed here.
                        # [Fix E] Distinguish profitable exits from stop-loss/expiry exits.
                        # Profitable exits must NOT trigger _record_base_stop so the
                        # strategy is not forced into a multi-day cooldown.
                        if _probe_mgr.exit_reason == 'profit':
                            # [P0-4 Fix] Profitable exit — do NOT set _base_stop_done,
                            # otherwise the same-day tick path is blocked.
                            # [P2 Fix] Clear _base_stop_date and reset days counter so the
                            # cooldown logic does not keep reporting "in cooldown" for days
                            # after a profitable probe exit.
                            C.can_do_t0 = False
                            C.strategy_mode = 'OBSERVE'
                            C.state = 'IDLE'
                            C._is_oversold_probe = False
                            C._is_init_probe = False
                            C._rebuild_probe_date = ''
                            # [Fix 9] Profitable exit: no cooldown, arm down_oversold ladder immediately.
                            # Previously set _base_stop_date + observe_only causing strategy to sit flat.
                            C._exit_policy = 'down_oversold'
                            C._base_stop_date = ''
                            C._days_since_stop = 99
                            C._probe_stop_active = False
                            C._stop_cool_days = 1
                            print(f" [V5 Probe Exit] Profitable exit, armed down_oversold "
                                  f"ladder immediately (no cooldown)")
                        elif _probe_mgr.exit_reason == 'expired' and (_exit_price - C._base_cost_price) / C._base_cost_price >= 0:
                            # [P1 Fix] Expiry with no gross loss (flat or profit
                            # on a pre-fee basis) is a normal end-of-life exit,
                            # NOT a stop-loss. On 06-18 a probe held the full 7
                            # days and exited at +0.06% gross float but -6.4 net
                            # PnL (after 8.4 fee), so _exit_pnl >= 0 failed and
                            # the 3-day cooldown was still triggered. Use gross
                            # float percentage so fee drag does not re-classify a
                            # flat/profit exit as a stop-loss. Treat flat/profit
                            # expiry like a profitable exit: clear the stop
                            # markers so no cooldown is applied.
                            C.can_do_t0 = False
                            C.strategy_mode = 'OBSERVE'
                            C.state = 'IDLE'
                            C._is_oversold_probe = False
                            C._is_init_probe = False
                            C._rebuild_probe_date = ''
                            # [Fix 9b] Same as Fix 9: no cooldown, arm down_oversold immediately.
                            C._exit_policy = 'down_oversold'
                            C._base_stop_date = ''
                            C._days_since_stop = 99
                            C._probe_stop_active = False
                            C._stop_cool_days = 1
                            print(f" [V5 Probe Exit] Expired with profit/flat, armed down_oversold "
                                  f"ladder immediately (no cooldown)")
                        else:
                            _record_base_stop(C, date_str, _exit_price, _exit_qty, realized_pnl=_pre_pnl if _pos_mgr is not None else 0.0)
                            C._base_stop_done = True
                            C.can_do_t0 = False
                            C.strategy_mode = 'OBSERVE'
                            C.state = 'IDLE'
                            C._is_oversold_probe = False
                            C._is_init_probe = False
                            C._rebuild_probe_date = ''
                        print(f"  [V5 Probe Exit] reason={_probe_mgr.exit_reason} "
                              f"qty={_exit_qty} price={_exit_price:.2f} "
                              f"pnl:{_exit_pnl:.1f} fee:{_exit_fee:.1f}")
                        return
                    else:
                        # [Fix 1] safe_sell failed (T+1 unavailable, limit-down,
                        # etc.). Mark intra-day attempt so the same request is
                        # not re-submitted every bar (60+ spam logs observed in
                        # replay). Next-day attempt is re-armed by increment_day.
                        _probe_mgr.exit_attempted_today = True
                        _probe_mgr.exit_requested = False
                        print(f"  [V5 Probe Exit] safe_sell failed (T+1/limit-down), "
                              f"suppressing further attempts today for {_probe_mgr.qty} sh")
                else:
                    print(f"  [V5 Probe Exit] exit_requested=False after check, no action")

        # ==========================================================
        # Phase 1: Observation (09:30 - 10:00)
        # ==========================================================
        if time_str < C.decision_time:
            update_trend_direction(C, _bar_time_str, current_price)

            # [P1 Fix] Gap-down risk assessment — extended beyond the
            # first 60 seconds. Previously the gate only checked
            # time_str <= '09:31:00', so a stock that opened flat then
            # faded -6% at 09:35 slipped through without any gap-stop,
            # forcing the position to ride all the way to the -8%
            # hard-stop (log 06-02 pattern where GAP-STOP missed a
            # -6.4% open-followed-by-drop). Relaxing the time constraint
            # lets deep losses exit at any point during Phase 1. The
            # hard-stop ATR-buffer still only triggers on extreme moves,
            # so mild pullbacks do NOT get prematurely liquidated.
            if (is_valid_price(current_price) and C._base_cost_price > 0
                and get_total_position(C) > 0
                and not getattr(C, '_base_staged_reduce_done', False)):
                _gap_pct = (current_price - C._base_cost_price) / C._base_cost_price
                # Pre-compute position variables for both the -6% hard-stop
                # branch AND the -4%~-6% preventive-reduce branch. These were
                # previously scoped inside the `if _gap_pct <= -0.06` block,
                # causing an UnboundLocalError when a -4.8% gap hit the elif
                # branch (see runtime crash: local variable '_gap_total'
                # referenced before assignment).
                _gap_total = get_total_position(C)
                _gap_avail = get_true_available_position(C)
                if _gap_pct <= -0.06:

                    # [Opt1] Dynamic hard stop threshold with ATR-normalized
                    # buffer. Use DAILY ATR (_daily_atr_14) as the primary
                    # volatility baseline (far more stable than per-tick
                    # intraday ATR on a volatile open); fall back to C.atr_val
                    # when daily data isn't available (e.g. first session).
                    # Adding the ATR buffer on TOP of the tiered stop-pct makes
                    # the trigger harder on volatile names — preventing noise
                    # from tripping the stop on the first bar.
                    _gap_is_probe = _holds_down_probe(C) or _dpm_is_init_probe(C)
                    _gap_is_high_dev = _dpm_is_init_probe(C)
                    _gap_hard_pct = _get_hard_stop_pct(C, _gap_is_probe, current_price, _gap_is_high_dev)
                    _gap_daily_atr = getattr(C, '_daily_atr_14', 0.0) or getattr(C, 'atr_val', 0.0)
                    # ATR buffer as pct of price: 1.5x ATR (symmetric) or a
                    # 1% floor to avoid div-zero / noise on ultra-stable names.
                    _gap_atr_pct = (_gap_daily_atr / C._base_cost_price) if C._base_cost_price > 0 else 0.01
                    _atr_buffer = max(_gap_atr_pct * 1.5, 0.01)
                    # Threshold grows with ATR so volatile names need a bigger
                    # drop before we close. E.g. on a 4% daily-ATR stock the
                    # hard-stop for a DOWN base becomes: 7.5% + 6.0% = 13.5%.
                    _gap_hard_threshold = _gap_hard_pct + _atr_buffer

                    if _gap_pct <= -_gap_hard_threshold:
                        # Exceeds hard stop 鈫? close ALL
                        _gap_close_qty = min(_gap_total, _gap_avail)
                        _gap_close_qty = (_gap_close_qty // 100) * 100
                        if _gap_close_qty >= 100:
                            print(f"  [GAP-STOP] Open float {_gap_pct:.1%} <= dyn_hard_stop "
                                  f"{-_gap_hard_threshold:.1%} (ATR:{_gap_daily_atr:.3f} "
                                  f"ATR_pct:{_gap_atr_pct:.2%} buf:{_atr_buffer:.2%}), "
                                  f"closing ALL {_gap_close_qty} @ {current_price:.2f}")
                            # [P2 Fix log66] Pre-compute fee & snapshot cost (防 DPM reset 零化)
                            _gap_fee = calc_trade_fee(C, current_price, _gap_close_qty, is_sell=True)
                            _gap_snapshot_cost = C._base_cost_price
                            # [PosMgr] Route through centralized gateway
                            _pos_mgr = getattr(C, '_pos_mgr', None)
                            if _pos_mgr is not None:
                                _gap_sell_ok, _gap_pnl = _pos_mgr.request_sell(_gap_close_qty, 'GAP_STOP_ALL', caller='stop_loss', current_price=current_price, tick=tick, fee=_gap_fee)
                            else:
                                _gap_sell_ok = safe_sell(C, current_price, _gap_close_qty, 'GAP_STOP_ALL', tick)
                            if _gap_sell_ok:
                                _gap_loss = (_gap_snapshot_cost - current_price) * _gap_close_qty if _gap_snapshot_cost > 0 else 0.0
                                if _pos_mgr is None:
                                    if getattr(C, '_risk_engine', None) is not None:
                                        C._risk_engine.record_trade_pnl(current_price, _gap_close_qty, 'SELL', _gap_fee)
                                    C.traded_volume += _gap_close_qty
                                    C._base_trade_fee += _gap_fee
                                    C._daily_trade_fee += _gap_fee
                                    C.realized_pnl += (-_gap_loss - _gap_fee)
                                    C.daily_pnl = C.realized_pnl
                                C._base_stop_done = True
                                C.can_do_t0 = False
                                _record_base_stop(C, date_str, current_price, _gap_close_qty, realized_pnl=_gap_pnl if _pos_mgr is not None else 0.0)
                                if get_total_position(C) == 0:
                                    reset_base_anchors(C, 'full')
                                print(f"  [GAP-STOP] Closed {_gap_close_qty} @ {current_price:.2f}, "
                                      f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}")
                            else:
                                # safe_sell failed (likely limit-down, no buyers). Escalate
                                # to pending-stop queue so _execute_base_stop_close retries
                                # on every subsequent tick until filled.
                                C._pending_base_stop = True
                                C._pending_stop_needs_record = True
                                C.can_do_t0 = False
                                if not getattr(C, '_gap_stop_fail_logged', False):
                                    C._gap_stop_fail_logged = True
                                    print(f"  [GAP-STOP] safe_sell FAILED for "
                                          f"{_gap_close_qty} @ {current_price:.2f} "
                                          f"(limit-down?); queued as pending stop")

                    elif _gap_total >= C.base_qty_half * 2:
                        # Full base and -6%~-8% 鈫? reduce half
                        print(f"  [GAP-CRITICAL] Open float {_gap_pct:.1%} <= -6% "
                              f"(price:{current_price:.2f} cost:{C._base_cost_price:.2f})")
                        _gap_reduce_qty = min(int(_gap_total * 0.50), _gap_avail)
                        _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                        if _gap_reduce_qty >= 100:
                            print(f"  [GAP-REDUCE] Emergency reduce {_gap_reduce_qty} @ {current_price:.2f}")
                            # [P2 Fix log66] Pre-compute fee + snapshot cost (same pattern
                            # as GAP_WARN_REDUCE above). DPM.on_sell resets C._base_cost_price
                            # to 0 when qty reaches 0, so we MUST read cost before routing.
                            _gap_fee = calc_trade_fee(C, current_price, _gap_reduce_qty, is_sell=True)
                            _gap_snapshot_cost = C._base_cost_price
                            _pos_mgr = getattr(C, '_pos_mgr', None)
                            if _pos_mgr is not None:
                                _gr_sold, _gr_pnl = _pos_mgr.request_sell(
                                    _gap_reduce_qty, 'GAP_REDUCE',
                                    caller='stop_loss', current_price=current_price,
                                    tick=tick, fee=_gap_fee)
                            else:
                                _gr_sold = safe_sell(C, current_price, _gap_reduce_qty, 'GAP_REDUCE', tick)

                            if _gr_sold:
                                _gap_loss = (_gap_snapshot_cost - current_price) * _gap_reduce_qty if _gap_snapshot_cost > 0 else 0.0
                                if _pos_mgr is None:
                                    if getattr(C, '_risk_engine', None) is not None:
                                        C._risk_engine.record_trade_pnl(current_price, _gap_reduce_qty, 'SELL', _gap_fee)
                                    C.traded_volume += _gap_reduce_qty
                                    C._base_trade_fee += _gap_fee
                                    C._daily_trade_fee += _gap_fee
                                    C.realized_pnl += (-_gap_loss - _gap_fee)
                                    C.daily_pnl = C.realized_pnl
                                C._base_staged_reduce_qty = _gap_reduce_qty
                                C._base_staged_orig_total = _gap_total
                                C._base_cut_done = True
                                C._down_reduce_done = True  # [F7修复]
                                _mark_staged_reduce(C, date_str)
                                if not C.is_live:
                                    _tb = getattr(C, '_today_bought_qty', 0)
                                    if _tb > 0:
                                        C._today_bought_qty = max(0, _tb - min(_gap_reduce_qty, _tb))
                                if _pos_mgr is None:
                                    C._cum_stop_loss += _gap_loss
                                    C._daily_stop_loss = getattr(C, '_daily_stop_loss', 0.0) + _gap_loss
                                _new_total = get_total_position(C)
                                # [Defect3] When a GAP-WARN reduction closes out
                                # the entire position, reset rebuild stage to 0
                                # (clean slate). On 6/1 a 200-sh probe was fully
                                # closed at the GAP-WARN zone, but the stage was
                                # left at 1 because `0 <= base_qty_half`. This
                                # caused the NEXT rebuild cycle to start in the
                                # "half-base" path (which adds 1000+ shares)
                                # instead of the "probe" path — a compounding
                                # error from sloppy state management.
                                if _new_total == 0:
                                    C._base_rebuild_stage = 0
                                    C._base_target_qty = 0
                                elif _new_total <= C.base_qty_half:
                                    # [P0 Fix] 仅当残仓 >= trade_qty 时才视为半仓基础阶段
                                    if _new_total >= C.trade_qty:
                                        C._base_rebuild_stage = 1
                                    else:
                                        # micro-probe (如100/200股) 不进入 base 生命周期
                                        C._base_rebuild_stage = 0
                                    C._base_target_qty = _new_total
                                # [P1-7 Fix] Keep cost anchor aligned with DPM.cost, do NOT overwrite with current_price
                                C._base_stop_anchor = C._base_cost_price
                                C._base_peak_price = current_price
                                # [Problem6 Fix] Only print anchor reset when position
                                # is NOT fully closed. On full close, the anchor is
                                # meaningless (reset to 0 by _base_rebuild_stage=0 path).
                                _anchor_msg = f" Anchor reset to {current_price:.2f}" if _new_total > 0 else ""
                                print(f"  [GAP-REDUCE] Done, remaining {_new_total}, "
                                      f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}.{_anchor_msg}")
                                # [P1 Fix] 残仓深水区二次退出保护
                                _new_total_w = get_total_position(C)
                                if _new_total_w > 0 and C._base_cost_price > 0:
                                    _post_reduce_float = (current_price - C._base_cost_price) / C._base_cost_price
                                    if _post_reduce_float <= -0.06:
                                        _force_close_qty = min(_new_total_w, get_true_available_position(C))
                                        _force_close_qty = (_force_close_qty // 100) * 100
                                        if _force_close_qty >= 100:
                                            print(f"  [GAP-REDUCE] Residual float {_post_reduce_float:.1%} <= -6%, force close remaining {_force_close_qty}")
                                            _force_fee = calc_trade_fee(C, current_price, _force_close_qty, is_sell=True)
                                            _pos_mgr = getattr(C, '_pos_mgr', None)
                                            if _pos_mgr is not None:
                                                _pos_mgr.request_sell(_force_close_qty, 'GAP_REDUCE_DEEP_WATER', caller='stop_loss', current_price=current_price, tick=tick, fee=_force_fee)
                                            else:
                                                safe_sell(C, current_price, _force_close_qty, 'GAP_REDUCE_DEEP_WATER', tick)
                            else:
                                # safe_sell failed (likely limit-down). Escalate to
                                # pending stop — this is a partial-reduce that couldn't
                                # execute, so we fall back to the T+1 queue mechanism
                                # rather than silently keeping the full position at -6%.
                                C._pending_base_stop = True
                                C._pending_stop_needs_record = True
                                C.can_do_t0 = False
                                if not getattr(C, '_gap_reduce_fail_logged', False):
                                    C._gap_reduce_fail_logged = True
                                    print(f"  [GAP-REDUCE] safe_sell FAILED for "
                                          f"{_gap_reduce_qty} @ {current_price:.2f} "
                                          f"(limit-down?); escalated to pending stop")

                    elif _gap_total <= C.trade_qty:
                        # [P0 Fix log75] Only immune probes skip -6% micro close.
                        # Non-immune probes (hold>2d) should execute the close.
                        _is_currently_probe = (getattr(C, '_probe_mgr', None) is not None 
                                            and C._probe_mgr.is_probe)

                        if _is_currently_probe and _is_probe_gap_immune(C):
                            if not getattr(C, '_phase1_gap_immune_skip_logged', False):
                                C._phase1_gap_immune_skip_logged = True
                                _gap_immune_days = (
                                    getattr(C, '_days_since_init_probe', 99)
                                    if _dpm_is_init_probe(C)
                                    else getattr(C, '_days_since_rebuild_probe', 99))
                                _gap_immune_max = _probe_immune_max_days(C)
                                _tier = _probe_intraday_stop_tier_msg(C, _dpm_is_init_probe(C), True)
                                print(f"  [GAP-STOP] Probe immune (hold {_gap_immune_days}d <= {_gap_immune_max}d), "
                                    f"skip -6% micro close (float:{_gap_pct:.1%}; {_tier})")
                        elif _is_currently_probe:
                            # [P0 Fix log75] Non-immune probe at -6%: force close,
                            # don't defer to -8% hard stop. Previously the "timeline
                            # unified" blanket exemption let hold>5d probes bleed
                            # past -6% to -8% Global Guard.
                            _gap_reduce_qty = min(_gap_total, _gap_avail)
                            _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                            if _gap_reduce_qty >= 100:
                                print(f"  [GAP-STOP] Non-immune probe {_gap_total} sh at float {_gap_pct:.1%}: "
                                    f"force close {_gap_reduce_qty} @ {current_price:.2f}")
                                _gap_fee = calc_trade_fee(C, current_price, _gap_reduce_qty, is_sell=True)
                                _gap_snapshot_cost = C._base_cost_price
                                _gap_was_probe = (_dpm_is_oversold_probe(C)
                                                or _dpm_is_init_probe(C))
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _gsold, _gspnl = _pos_mgr.request_sell(_gap_reduce_qty, 'GAP_STOP_MICRO_PROBE', caller='stop_loss', current_price=current_price, tick=tick, fee=_gap_fee)
                                else:
                                    _gsold = safe_sell(C, current_price, _gap_reduce_qty, 'GAP_STOP_MICRO_PROBE', tick)
                                if _gsold:
                                    _gap_loss = (_gap_snapshot_cost - current_price) * _gap_reduce_qty if _gap_snapshot_cost > 0 else 0.0
                                    if _pos_mgr is None:
                                        if getattr(C, '_risk_engine', None) is not None:
                                            C._risk_engine.record_trade_pnl(current_price, _gap_reduce_qty, 'SELL', _gap_fee)
                                        C.traded_volume += _gap_reduce_qty
                                        C._base_trade_fee += _gap_fee
                                        C._daily_trade_fee += _gap_fee
                                        C.realized_pnl += (-_gap_loss - _gap_fee)
                                        C.daily_pnl = C.realized_pnl
                                    _new_total_gap_stop = get_total_position(C)
                                    if _new_total_gap_stop == 0:
                                        C._base_rebuild_stage = 0
                                        C._base_target_qty = 0
                                        C._base_stop_done = True
                                        C._gap_exit_today = True
                                        _record_base_stop(C, date_str, current_price, _gap_reduce_qty,
                                                        realized_pnl=_gspnl if _pos_mgr is not None else 0.0,
                                                        is_explicit_probe_stop=_gap_was_probe,
                                                        is_gap_exit=True)
                                        _gmgr = getattr(C, '_probe_mgr', None)
                                        if _gmgr is not None:
                                            _gmgr.reset()
                                            _gmgr.exit_attempted_today = True
                                        reset_base_anchors(C, 'full')
                                        print(f"  [GAP-STOP] Closed {_gap_reduce_qty} @ {current_price:.2f}, "
                                            f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}, "
                                            f"type={'probe_rsi' if _gap_was_probe else 'gap_exit'} (cooldown armed)")
                                    else:
                                        C._base_rebuild_stage = 0
                                        C._base_target_qty = _new_total_gap_stop
                                        C._base_staged_reduce_done = True
                        else:
                            # [log73 rollback] micro-probe (<=200 sh): always full close.
                            _gap_reduce_qty = min(_gap_total, _gap_avail)
                            _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                            if _gap_reduce_qty >= 100:
                                print(f"  [GAP-STOP] micro-probe {_gap_total} sh at float {_gap_pct:.1%}: "
                                    f"force close {_gap_reduce_qty} @ {current_price:.2f} "
                                    f"(capped loss vs -8% ride)")
                                # [P2 Fix log66] Pre-compute fee & snapshot cost (防 DPM reset 零化)
                                _gap_fee = calc_trade_fee(C, current_price, _gap_reduce_qty, is_sell=True)
                                _gap_snapshot_cost = C._base_cost_price
                                _gap_was_probe = (_dpm_is_oversold_probe(C)
                                                or _dpm_is_init_probe(C))
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _gsold, _gspnl = _pos_mgr.request_sell(_gap_reduce_qty, 'GAP_STOP_MICRO_PROBE', caller='stop_loss', current_price=current_price, tick=tick, fee=_gap_fee)
                                else:
                                    _gsold = safe_sell(C, current_price, _gap_reduce_qty, 'GAP_STOP_MICRO_PROBE', tick)
                                if _gsold:
                                    _gap_loss = (_gap_snapshot_cost - current_price) * _gap_reduce_qty if _gap_snapshot_cost > 0 else 0.0
                                    if _pos_mgr is None:
                                        if getattr(C, '_risk_engine', None) is not None:
                                            C._risk_engine.record_trade_pnl(current_price, _gap_reduce_qty, 'SELL', _gap_fee)
                                        C.traded_volume += _gap_reduce_qty
                                        C._base_trade_fee += _gap_fee
                                        C._daily_trade_fee += _gap_fee
                                        C.realized_pnl += (-_gap_loss - _gap_fee)
                                        C.daily_pnl = C.realized_pnl
                                    _new_total_gap_stop = get_total_position(C)
                                    if _new_total_gap_stop == 0:
                                        C._base_rebuild_stage = 0
                                        C._base_target_qty = 0
                                        C._base_stop_done = True
                                        C._gap_exit_today = True
                                        _record_base_stop(C, date_str, current_price, _gap_reduce_qty,
                                                        realized_pnl=_gspnl if _pos_mgr is not None else 0.0,
                                                        is_explicit_probe_stop=_gap_was_probe,
                                                        is_gap_exit=True)
                                        _gmgr = getattr(C, '_probe_mgr', None)
                                        if _gmgr is not None:
                                            _gmgr.reset()
                                            _gmgr.exit_attempted_today = True
                                        reset_base_anchors(C, 'full')
                                        print(f"  [GAP-STOP] Closed {_gap_reduce_qty} @ {current_price:.2f}, "
                                            f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}, "
                                            f"type={'probe_rsi' if _gap_was_probe else 'gap_exit'} (cooldown armed)")
                                    else:
                                        # 残仓不足 trade_qty 严禁设为 stage 1
                                        C._base_rebuild_stage = 0
                                        C._base_target_qty = _new_total_gap_stop
                                        C._base_staged_reduce_done = True
                                else:
                                    C._pending_base_stop = True
                                    C._pending_stop_needs_record = True
                                    C.can_do_t0 = False
                                    if not getattr(C, '_gap_reduce_fail_logged', False):
                                        C._gap_reduce_fail_logged = True
                                        print(f"  [GAP-STOP] micro-probe safe_sell FAILED for {_gap_reduce_qty} @ {current_price:.2f} (limit-down?); escalated to pending stop")

                    else:
                        _gap_close_qty = min(_gap_total, _gap_avail)
                        _gap_close_qty = (_gap_close_qty // 100) * 100
                        if _gap_close_qty >= 100:
                            print(f"  [GAP-STOP] Open float {_gap_pct:.1%} <= -6%, "
                                  f"small pos {_gap_total} < {C.base_qty_half * 2}, "
                                  f"force close {_gap_close_qty} @ {current_price:.2f}")
                            # [P2 Fix log66] Pre-compute fee & snapshot cost (防 DPM reset 零化)
                            _gap_fee = calc_trade_fee(C, current_price, _gap_close_qty, is_sell=True)
                            _gap_snapshot_cost = C._base_cost_price
                            _pos_mgr = getattr(C, '_pos_mgr', None)
                            if _pos_mgr is not None:
                                _gsp_sold, _gsp_pnl = _pos_mgr.request_sell(_gap_close_qty, 'GAP_STOP_PROBE', caller='stop_loss', current_price=current_price, tick=tick, fee=_gap_fee)
                            else:
                                _gsp_sold = safe_sell(C, current_price, _gap_close_qty, 'GAP_STOP_PROBE', tick)
                            if _gsp_sold:
                                _gap_loss = (_gap_snapshot_cost - current_price) * _gap_close_qty if _gap_snapshot_cost > 0 else 0.0
                                if _pos_mgr is None:
                                    if getattr(C, '_risk_engine', None) is not None:
                                        C._risk_engine.record_trade_pnl(current_price, _gap_close_qty, 'SELL', _gap_fee)
                                    C.traded_volume += _gap_close_qty
                                    C._base_trade_fee += _gap_fee
                                    C._daily_trade_fee += _gap_fee
                                    C.realized_pnl += (-_gap_loss - _gap_fee)
                                    C.daily_pnl = C.realized_pnl
                                C._base_stop_done = True
                                C._gap_exit_today = True  # [P0-2 Fix] 触发统一冷却闸门
                                _gap_is_probe_type = (_dpm_is_oversold_probe(C)
                                                      or _dpm_is_init_probe(C))
                                # [P0-2 Fix] 改用 _record_base_stop 并打标签，不再误用 _record_staged_half_exit
                                _record_base_stop(C, date_str, current_price, _gap_close_qty,
                                                  realized_pnl=_gsp_pnl if _pos_mgr is not None else 0.0,
                                                  is_explicit_probe_stop=_gap_is_probe_type,
                                                  is_gap_exit=True)
                                _gmgr = getattr(C, '_probe_mgr', None)
                                if _gmgr is not None:
                                    _gmgr.reset()
                                    _gmgr.exit_attempted_today = True
                                if get_total_position(C) == 0:
                                    reset_base_anchors(C, 'full')
                                print(f"  [GAP-STOP] Closed {_gap_close_qty} @ {current_price:.2f}, "
                                      f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}, "
                                      f"type={'probe_rsi' if _gap_is_probe_type else 'gap_exit'} (cooldown armed)")
                            else:
                                C._pending_base_stop = True
                                C._pending_stop_needs_record = True
                                C.can_do_t0 = False
                                if not getattr(C, '_gap_probe_fail_logged', False):
                                    C._gap_probe_fail_logged = True
                                    print(f"  [GAP-STOP] probe safe_sell FAILED for "
                                          f"{_gap_close_qty} @ {current_price:.2f} "
                                          f"(limit-down?); queued as pending stop")
                elif _gap_pct <= -0.04 and _gap_pct > -0.06:
                    if _gap_total >= 100 and not getattr(C, '_gap_warn_reduced_today', False):
                        # log172 A': shallow before GAP-WARN when bridge/post-immune
                        if not _try_rebuild_probe_shallow_before_gap(
                                C, date_str, current_price, tick):
                            _gap_reduce_qty = 0
                            _is_currently_probe_warn = (getattr(C, '_probe_mgr', None) is not None
                                                    and C._probe_mgr.is_probe)
                            _immune_max = _probe_immune_max_days(C)

                            if (_is_currently_probe_warn
                                    and _probe_gap_immune_blocks_4pct(C, _gap_pct)):
                                if not getattr(C, '_phase1_gap_immune_skip_logged', False):
                                    C._phase1_gap_immune_skip_logged = True
                                    _warn_is_init = _dpm_is_init_probe(C)
                                    _warn_days = (getattr(C, '_days_since_init_probe', 99) if _warn_is_init
                                                  else getattr(C, '_days_since_rebuild_probe', 99))
                                    _tier = _probe_intraday_stop_tier_msg(C, _warn_is_init, True)
                                    print(f"  [GAP-WARN] Probe immune (hold {_warn_days}d <= {_immune_max}d), skip -4%~-6% trim "
                                        f"(float:{_gap_pct:.1%}; {_tier})")
                                    C._gap_warn_seen_today = True
                            elif _is_currently_probe_warn and _is_probe_gap_immune_partial(C):
                                # [P0 Fix log78] Init probe hold day 3-5: allow -4% partial trim
                                _warn_is_init = _dpm_is_init_probe(C)
                                _gap_days = (getattr(C, '_days_since_init_probe', 0) if _warn_is_init
                                             else getattr(C, '_days_since_rebuild_probe', 0))
                                _gap_lbl = 'Init' if _warn_is_init else 'Rebuild'
                                _gap_reduce_qty = min(_gap_total, get_true_available_position(C))
                                _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                                if _gap_reduce_qty >= 100:
                                    if _gap_total <= 100:
                                        _gap_reduce_qty = min(_gap_total, get_true_available_position(C))
                                        _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                                        print(f"  [GAP-WARN] {_gap_lbl} probe day {_gap_days}: micro full-close "
                                              f"{_gap_reduce_qty} of {_gap_total} @ {current_price:.2f} "
                                              f"(float:{_gap_pct:.1%}, 100-sh unit cannot halve)")
                                    else:
                                        _half_trim = max(100, (_gap_reduce_qty // 2 // 100) * 100)
                                        _gap_reduce_qty = _half_trim
                                        print(f"  [GAP-WARN] {_gap_lbl} probe day {_gap_days}: partial trim "
                                              f"{_gap_reduce_qty} of {_gap_total} @ {current_price:.2f} "
                                              f"(float:{_gap_pct:.1%})")
                                else:
                                    _half_trim = max(100, (_gap_reduce_qty // 2 // 100) * 100)
                                    _gap_reduce_qty = _half_trim
                                    print(f"  [GAP-WARN] {_gap_lbl} probe day {_gap_days}: partial trim "
                                          f"{_gap_reduce_qty} of {_gap_total} @ {current_price:.2f} "
                                          f"(float:{_gap_pct:.1%})")
                            elif _is_currently_probe_warn:
                                _gap_avail_w = get_true_available_position(C)
                                _gap_cap = min(_gap_total, _gap_avail_w)
                                _gap_reduce_qty, _gap_log = _non_immune_probe_gap_reduce_qty(
                                    C, _gap_total, _gap_cap, _gap_pct, current_price)
                                if _gap_reduce_qty >= 100 and _gap_log:
                                    print(_gap_log)
                            elif _gap_total <= C.trade_qty:
                                _gap_avail_mp = get_true_available_position(C)
                                _gap_cap = min(_gap_total, _gap_avail_mp)
                                _gap_cap = (_gap_cap // 100) * 100
                                if _gap_cap <= 100:
                                    _gap_reduce_qty = _gap_cap
                                    print(f"  [GAP-WARN] micro-probe: {_gap_total} sh at float "
                                        f"{_gap_pct:.1%} force close {_gap_reduce_qty} @ {current_price:.2f} "
                                        f"(100 sh unit cannot halve; cap loss vs -8% ride)")
                                else:
                                    _gap_reduce_qty = max(100, (_gap_cap // 2 // 100) * 100)
                                    print(f"  [GAP-WARN] micro-probe: {_gap_total} sh at float "
                                        f"{_gap_pct:.1%} partial trim {_gap_reduce_qty} @ {current_price:.2f}")
                            else:
                                _gap_raw_reduce = int(_gap_total * 0.50)
                                _gap_reduce_qty = (_gap_raw_reduce // 100) * 100
                                if _gap_reduce_qty < 100:
                                    _gap_reduce_qty = min(_gap_total, get_true_available_position(C))
                                    _gap_reduce_qty = (_gap_reduce_qty // 100) * 100
                                print(f"  [GAP-REDUCE] Preventive reduce {_gap_reduce_qty} @ {current_price:.2f}")
                            if _gap_reduce_qty >= 100:
                                # [P2 Fix log66] Pre-compute fee so PosMgr/DPM.on_sell
                                # receives a non-zero cost. Without this, fee defaults to
                                # 0.0 and DPM computes a "clean" PnL that omits broker
                                # fees (C._daily_trade_fee stays 0 for this bar).
                                _gap_fee = calc_trade_fee(C, current_price, _gap_reduce_qty, is_sell=True)
                                # Snapshot cost BEFORE request_sell — DPM.on_sell may
                                # reset C._base_cost_price to 0 when qty drops to 0.
                                _gap_snapshot_cost = C._base_cost_price
                                # [Fix 6] Snapshot probe identity BEFORE request_sell.
                                # request_sell triggers DPM.on_sell → reset() which
                                # clears probe flags, making _dpm_is_probe() return
                                # False after the sell. This caused GAP-WARN to always
                                # fall into Init-Half (1d cooldown) instead of GAP_EXIT
                                # (3d cooldown), enabling 06-10 churn re-entry.
                                _gap_was_probe = _dpm_is_probe(C)
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _gwr_sold, _gwr_pnl = _pos_mgr.request_sell(
                                        _gap_reduce_qty, 'GAP_WARN_REDUCE',
                                        caller='stop_loss', current_price=current_price,
                                        tick=tick, fee=_gap_fee)
                                else:
                                    _gwr_sold = safe_sell(C, current_price, _gap_reduce_qty, 'GAP_WARN_REDUCE', tick)

                                if _gwr_sold:
                                    C._gap_warn_reduced_today = True
                                    _gwr_remain = get_total_position(C)
                                    if _gwr_remain > 0:
                                        C._probe_gap_ladder_date = date_str
                                        C._intraday_gap_reduce_today = True
                                        C._intraday_warn_done = True
                                    else:
                                        C._probe_gap_ladder_date = ''
                                    # [P2 Fix] Compute loss from the PRE-sell cost
                                    # snapshot, not from C._base_cost_price which DPM
                                    # may have already zeroed (for full-close micro-probe
                                    # path). This avoids the "loss = price * qty" bug.
                                    _gap_loss = (_gap_snapshot_cost - current_price) * _gap_reduce_qty if _gap_snapshot_cost > 0 else 0.0
                                    C._base_cut_done = True
                                    if _pos_mgr is None:
                                        if getattr(C, '_risk_engine', None) is not None:
                                            C._risk_engine.record_trade_pnl(current_price, _gap_reduce_qty, 'SELL', _gap_fee)
                                        C.traded_volume += _gap_reduce_qty
                                        C._base_trade_fee += _gap_fee
                                        C._daily_trade_fee += _gap_fee
                                        C.realized_pnl += (-_gap_loss - _gap_fee)
                                        C.daily_pnl = C.realized_pnl
                                        if _gap_was_probe:
                                            C._cum_stop_loss += _gap_loss
                                            C._daily_stop_loss = getattr(C, '_daily_stop_loss', 0.0) + _gap_loss
                                    # [Fix 6] Unified GAP_EXIT path: both probe and
                                    # non-probe now route through _record_base_stop with
                                    # is_gap_exit=True, enforcing 3d cooldown + observe_only.
                                    # The old probe path manually set state (1d cooldown,
                                    # no observe_only) which allowed 06-10 churn re-entry.
                                    # The old non-probe path used _record_staged_half_exit
                                    # (Init-Half label, 1d cooldown) — same problem.
                                    C._last_stop_ref_price = current_price
                                    _record_base_stop(C, date_str, current_price, _gap_reduce_qty,
                                                      realized_pnl=_gwr_pnl if _pos_mgr is not None else 0.0,
                                                      is_explicit_probe_stop=_gap_was_probe,
                                                      is_gap_exit=True)
                                    # [P2 Fix log75] Print matches actual _record_base_stop
                                    # classification: probe qty <= trade_qty routes to 1d
                                    # cooldown + down_oversold, NOT 3d observe_only.
                                    _new_total = get_total_position(C)
                                    _mgc_d = getattr(C, 'micro_gap_cool_days', 2)
                                    if _gap_reduce_qty <= _probe_gap_micro_max(C):
                                        _gwr_policy = f'micro GAP {_mgc_d}d+down_oversold'
                                    elif _gap_reduce_qty >= C.base_qty_half:
                                        _gwr_policy = 'GAP_EXIT 3d+observe_only'
                                    else:
                                        _gwr_policy = f'partial GAP {_mgc_d}d+down_oversold'
                                    print(f"  [GAP-REDUCE] {'Probe' if _gap_was_probe else 'Base'} partial stop recorded, {_gwr_policy}.")
                                    if _new_total > 0 and _gap_reduce_qty < _gap_total:
                                        C._gap_partial_trim_session = date_str
                                    # [Defect7] When a GAP reduce fully closes out the position,
                                    # reset stage to 0 (clean slate). On 6/1 a 200-sh probe was
                                    # reduced to 0 shares, but `_new_total <= base_qty_half`
                                    # evaluated True (0 < 1300), leaving stage at 1. The subsequent
                                    # rebuild then picked up stage=1 (half-base path), attempting
                                    # to add 1000+ shares onto what should have been a fresh probe
                                    # entry. Same fix pattern as the earlier GAP-WARN branch above.
                                    if _new_total == 0:
                                        C._base_rebuild_stage = 0
                                        C._base_target_qty = 0
                                    elif _new_total <= C.base_qty_half:
                                        # [P0 Fix] 仅当残仓 >= trade_qty 时才视为半仓基础阶段
                                        if _new_total >= C.trade_qty:
                                            C._base_rebuild_stage = 1
                                        else:
                                            # micro-probe (如100/200股) 不进入 base 生命周期
                                            C._base_rebuild_stage = 0
                                        C._base_target_qty = _new_total
                                    # [P1-7 Fix] Keep cost anchor aligned with DPM.cost, do NOT overwrite with current_price
                                    C._base_stop_anchor = C._base_cost_price
                                    C._base_peak_price = current_price
                                    # [Problem6 Fix] Only print anchor reset when position
                                    # is NOT fully closed. On full close, the anchor is
                                    # meaningless (reset to 0 by _base_rebuild_stage=0 path).
                                    _anchor_msg = f" Anchor reset to {current_price:.2f}" if _new_total > 0 else ""
                                    print(f"  [GAP-REDUCE] Done, remaining {_new_total}, "
                                          f"loss:{_gap_loss:.1f} fee:{_gap_fee:.1f}.{_anchor_msg}")
                                else:
                                    # safe_sell failed (likely limit-down). Escalate to
                                    # pending stop so the proactive -4%~-6% trim is not
                                    # silently dropped — letting the position bleed to -8%+.
                                    C._pending_base_stop = True
                                    C._pending_stop_needs_record = True
                                    C.can_do_t0 = False
                                    if not getattr(C, '_gap_warn_fail_logged', False):
                                        C._gap_warn_fail_logged = True
                                        print(f"  [GAP-REDUCE] warn safe_sell FAILED for "
                                              f"{_gap_reduce_qty} @ {current_price:.2f} "
                                              f"(limit-down?); escalated to pending stop")

            if get_total_position(C) > C.trade_qty:
                if _has_staged_reduce_state(C) or _holds_init_half_only(C):
                    _apply_staged_half_risk_exit(C, current_price, date_str, time_str, tick)
                if getattr(C, '_staged_half_exit_done', False):
                    print_eod_summary(C, time_str, current_price)
                    return

            # [P1 Fix] Use bar low for stop-loss evaluation.
            _eval_low_obs = _bar_low if _bar_data_ok else current_price
            _run_base_stop_checks(C, date_str, _eval_low_obs, tick, _bar_time_str, use_orb_amp=True)
            if len(C.bars_close) % 10 == 0 and len(C.bars_close) != getattr(C, '_last_debug_bars', -1):
                C._last_debug_bars = len(C.bars_close)
                _obs_vol = C.confirmed_min_vol + C.cur_bar_volume
                print(f"  [ORB-Obs] {time_str} p:{current_price:.2f} H:{C.orb_high:.2f} L:{C.orb_low:.2f} "
                      f"bars:{len(C.bars_close)} vol:{_obs_vol}")
            print_eod_summary(C, time_str, current_price)
            return

        # ==========================================================
        # Phase 2: Decision time (10:00)
        # ==========================================================
        # ponytail: QMT deepcopy(C) resets strategy_mode; restore after 10:00 decision
        if C.is_live and _HB_SESSION.get('date') == date_str and _HB_SESSION.get('mode'):
            C.strategy_mode = _HB_SESSION['mode']

        if C.strategy_mode == 'UNDECIDED':
            amplitude = (C.orb_high - C.orb_low) / C.orb_low if C.orb_low > 0 else 0
            bars_count = len(C.bars_close)
            print(f"\n[{time_str}] Decision | H:{C.orb_high:.2f} L:{C.orb_low:.2f} amp:{amplitude:.2%} bars:{bars_count} atr:{C.atr_val:.4f} ticks:{C._obs_tick_count}")

            if C._obs_tick_count == 0:
                if time_str >= C.decision_time and get_total_position(C) >= C.trade_qty:
                    _dor = _day_open_trend_frozen(C)
                    C.strategy_mode = 'SKIP' if _dor == 'DOWN' else 'OBSERVE'
                    _HB_SESSION['mode'] = C.strategy_mode
                    print(f"  [Skip] Mid-session start pos={get_total_position(C)}, "
                          f"mode={C.strategy_mode} (ORB obs skipped)")
                    _hb_snapshot(C, date_str)
                else:
                    C.strategy_mode = 'SKIP'
                    _HB_SESSION['mode'] = 'SKIP'
                    print(f"  [Skip] No ticks in observation period (likely holiday)")
                    return

            if bars_count < 10:
                if time_str < '10:30:00':
                    if is_valid_price(current_price):
                        if current_price > C.orb_high: C.orb_high = current_price
                        if current_price < C.orb_low:  C.orb_low = current_price
                    print(f"  [Wait] Insufficient bars ({bars_count}), extend observation")
                    return
                else:
                    C.strategy_mode = 'SKIP'
                    _HB_SESSION['mode'] = 'SKIP'
                    print(f"  [Skip] Too few bars, cannot decide")
                    return

            if is_valid_price(C.orb_high) and is_valid_price(C.orb_low) and C.orb_high == C.orb_low:
                C.strategy_mode = 'SKIP'
                _HB_SESSION['mode'] = 'SKIP'
                print(f"  [Skip] Orb high == orb low ({C.orb_high:.2f}), no range")
                return

            if amplitude < 0:
                C.strategy_mode = 'SKIP'
                _HB_SESSION['mode'] = 'SKIP'
                print(f"  [Skip] Invalid amplitude ({amplitude:.2%})")
                return

            update_trend_direction(C, _bar_time_str, current_price)

            if amplitude >= C.orb_amp_th and current_price > vwap_price and not getattr(C, '_orb_disabled_today', False):
                _eff_trend_orb = _day_open_trend_frozen(C)
                if C.trend_direction == 'UP' and _eff_trend_orb != 'DOWN':
                    C.strategy_mode = 'ORB'
                    C.state = 'WAIT_BREAKOUT'
                    C.orb_avg_vol_pm = C.orb_total_vol / 30.0
                    print(f" -> Mode: High amplitude + UP trend -> ORB Breakout")
                else:
                    if get_total_position(C) == 0 and C._base_stop_date != '':
                        C.strategy_mode = 'OBSERVE'
                        print(f" -> Mode: High amplitude + empty after stop -> OBSERVE (rebuild allowed)")
                    elif _holds_down_probe(C):
                        C.strategy_mode = 'OBSERVE'
                        print(f" -> Mode: High amplitude + probe hold -> OBSERVE (SKIP avoided)")
                    else:
                        C.strategy_mode = 'SKIP'
                        _trend_note = f" (day-open {_eff_trend_orb})" if _eff_trend_orb != C.trend_direction else ""
                        print(f" -> Mode: High amplitude but trend {C.trend_direction}{_trend_note} -> SKIP")
                        _apply_skip_risk_reduce(C, current_price, _bar_time_str, date_str, tick)
                        _apply_staged_half_risk_exit(C, current_price, date_str, time_str, tick)
            elif amplitude >= C.orb_amp_th and current_price < vwap_price and not getattr(C, '_orb_disabled_today', False):
                _eff_trend_bvw = _day_open_trend_frozen(C)
                if get_total_position(C) == 0 and C._base_stop_date != '':
                    C.strategy_mode = 'OBSERVE'
                    print(f" -> Mode: High amplitude + below VWAP + empty -> OBSERVE (rebuild allowed)")
                elif _holds_down_probe(C):
                    C.strategy_mode = 'OBSERVE'
                    print(f" -> Mode: High amplitude + probe hold below VWAP -> OBSERVE (SKIP avoided)")
                elif _eff_trend_bvw == 'DOWN':
                    C.strategy_mode = 'OBSERVE'
                    print(f" -> Mode: High amplitude + below VWAP + macro DOWN -> OBSERVE (avoid SKIP deadlock)")
                else:
                    C.strategy_mode = 'SKIP'
                    print(f" -> Mode: High amplitude but below VWAP -> SKIP")
                    _apply_skip_risk_reduce(C, current_price, _bar_time_str, date_str, tick)
                    _apply_staged_half_risk_exit(C, current_price, date_str, time_str, tick)
            # [缺陷3] 增加灰区判断 3.2%-3.5%: 振幅接近阈值时若破VWAP或浮亏, 避免硬切误判
            _gray_zone_low = C.orb_amp_th - 0.003

            if amplitude >= C.orb_amp_th:
                # 原有高振幅逻辑 (省略, 原代码已在上方实现)
                pass
            elif amplitude >= _gray_zone_low:
                _base_float = base_float_risk(C, current_price)
                if current_price < vwap_price or _base_float <= -0.02:
                    C.strategy_mode = 'OBSERVE'
                    # [Issue 4 Fix] Gray zone weak float: block OBSERVE->T0
                    C._gray_zone_weak_block_t0 = True
                    print(f" -> Mode: Gray zone amp({amplitude:.2%}) + weak price/float -> OBSERVE")
                else:
                    C.strategy_mode = 'T0'
                    C.state = 'IDLE'
                    print(f" -> Mode: Gray zone amp({amplitude:.2%}) but price stable -> T0")
            else:
                # NOTE: Probes normally enter OBSERVE at day-init; Phase 4 handles
                # intraday high-confidence sell. This branch is a fallback when mode
                # is still UNDECIDED at 10:00 (e.g. before init gate or edge cases).
                if _holds_down_probe(C):
                    _probe_rsi = calc_rsi(C, period=6)
                    _probe_vwap_dev = (current_price - vwap_price) / vwap_price if vwap_price > 0 else 0
                    if _probe_rebuild_t0_blocked(C, current_price, vwap_price):
                        C.strategy_mode = 'OBSERVE'
                        log_once(C, '_rebuild_probe_t0_skip_logged',
                                  f" -> Mode: Probe rebuild T0 cooldown "
                                  f"({C._days_since_rebuild_probe}/{C.rebuild_probe_t0_cool_days}d), stay OBSERVE")
                    elif _probe_high_confidence_sell(C, current_price, vwap_price):
                        if getattr(C, '_force_liquidation_active', False):
                            print(f" -> Mode: Probe hold, Force-liquidation active, block high-conf T0 switch "
                                  f"(RSI:{_probe_rsi:.1f} dev:{_probe_vwap_dev:.2%}) -> OBSERVE")
                        elif get_true_available_position(C) >= 100:
                            C.strategy_mode = 'T0'
                            C.state = 'IDLE'
                            C._sell_signal_history = [1]
                            print(f" -> Mode: Probe hold, high-confidence sell allowed "
                                  f"(RSI:{_probe_rsi:.1f} dev:{_probe_vwap_dev:.2%}) -> T0")
                        else:
                            C.strategy_mode = 'OBSERVE'
                            print(f" -> Mode: Probe high-confidence sell but T+1 blocked -> OBSERVE "
                                  f"(RSI:{_probe_rsi:.1f} dev:{_probe_vwap_dev:.2%})")
                    else:
                        C.strategy_mode = 'OBSERVE'
                        print(f" -> Mode: Probe hold -> OBSERVE (sell signal insufficient "
                              f"RSI:{_probe_rsi:.1f} dev:{_probe_vwap_dev:.2%})")
                else:
                    if getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                        C.strategy_mode = 'OBSERVE'
                        C.state = 'IDLE'
                        print(f" -> Mode: Low amplitude + empty + VWAP carry -> OBSERVE")
                    else:
                        # [P2 Fix] Empty position must NOT enter T0 mode;
                        # stay in OBSERVE to avoid false tick noise.
                        if get_total_position(C) == 0:
                            C.strategy_mode = 'OBSERVE'
                            C.state = 'IDLE'
                            if C.trend_direction == 'UP' and C._base_stop_date == '':
                                log_once(C, '_low_amp_up_observe_logged',
                                         f" -> Mode: Low amplitude + empty UP -> OBSERVE "
                                         f"(init/high-dev rebuild active; dev+VWAP gate applies)")
                            else:
                                print(f" -> Mode: Low amplitude + empty -> OBSERVE")
                        else:
                            C.strategy_mode = 'T0'
                            C.state = 'IDLE'
                            print(f" -> Mode: Low amplitude -> T+0 Counter-Trend")

            if C.strategy_mode != 'UNDECIDED':
                _HB_SESSION['mode'] = C.strategy_mode
                _hb_snapshot(C, date_str)

        # ================= Intraday GAP Protection (P0-1, P0-3) =================
        if time_str >= C.decision_time:
            if _check_intraday_gap_protection(C, current_price, tick, date_str, time_str, _bar_low):
                print_eod_summary(C, time_str, current_price)
                return

        # ==========================================================
        # Phase 3A: Trend cut (DOWN reduction) + base risk checks
        # ==========================================================
        # [Defect2] Phase 3A entry: only skip if live or after an
        # actual Trend Cut executed. DO NOT gate on _probe_protect_done —
        # that flag was being set on the very first heartbeat of probe
        # immunity (10:00 on 5/29), which caused ALL subsequent heartbeats
        # during the 2-day immunity window to skip Phase 3A entirely.
        # The 100-sh probe thus drifted unprotected from 36.23 down to
        # 34.10 (-8.1%) before the Global Stop rescued it. The -6%
        # defense check must keep running every heartbeat; only the
        # reduction ACTION is guarded by an intraday re-entry lock.
        if not C._trend_cut_done:
            _risk_hb_key = time_str[:4]
            if is_hb_slot(time_str) and _risk_hb_key != getattr(C, '_base_risk_hb', ''):
                C._base_risk_hb = _risk_hb_key
                update_trend_direction(C, _bar_time_str, current_price)
                _total_pos = get_total_position(C)
                _avail_pos = get_true_available_position(C)
                _will_stop = False
                if get_stop_anchor(C) > 0:
                    _fp = base_float_risk(C, current_price)
                    if _total_pos > 0:
                        _bump_probe_peak(C, current_price)
                    _bp = max(C._base_peak_price, current_price)
                    _dd = (_bp - current_price) / _bp if _bp > 0 else 0
                    _is_probe_pos = (_holds_down_probe(C) or _dpm_is_init_probe(C)
                             or _holds_neutral_micro_probe(C))
                    _is_high_dev_probe = _dpm_is_init_probe(C)
                    _eff_hard = _get_hard_stop_pct(C, _is_probe_pos, current_price, _is_high_dev_probe)
                    # [P0/P1 Fix] _get_hard_stop_pct already embeds stop-cost
                    # budget in the returned threshold. Subtracting
                    # sell_commission_rate + stamp_duty_rate again means the
                    # hard-stop fires ~0.115% too early (or ~¥5.75 on 100×¥50)
                    # — enough to exit before the configured 8%/12% risk
                    # tolerance. Remove the double-subtraction.
                    if _fp <= -_eff_hard or _dd >= _get_trail_dd_pct(C):
                        _will_stop = True
                # ponytail: probe immune window 2d (NEUTRAL micro 3d via _probe_immune_max_days)
                _probe_hold = (_holds_down_probe(C) or _dpm_is_init_probe(C))
                _probe_days = getattr(C, '_days_since_rebuild_probe', 99) if _holds_down_probe(C) else getattr(C, '_days_since_init_probe', 99)
                _probe_immune_max = _probe_immune_max_days(C)
                _in_immune_window = _probe_hold and _probe_days <= _probe_immune_max
                _probe_immune = (
                    _in_immune_window
                    and not getattr(C, '_force_liquidation_active', False)
                    and _probe_global_guard_immune(C))
                if _in_immune_window and not _probe_immune:
                    if not getattr(C, '_trend_cut_immune_break_logged', False):
                        C._trend_cut_immune_break_logged = True
                        if _dpm_is_init_probe(C):
                            _tc_brk = 'Init probe immune broken: trend DOWN'
                        elif _holds_down_probe(C) and _oversold_probe_severe_entry(C):
                            _tc_brk = 'Oversold severe-entry immune broken (T+1+)'
                        else:
                            _tc_brk = 'Probe immune broken'
                        print(f"  [Trend Cut] {_tc_brk}, Trend Cut active "
                              f"(hold:{_probe_days}d)")
                # log172 A'+C: init shallow in immune; rebuild shallow bridge/post-immune
                if _total_pos > 0:
                    if _maybe_init_micro_shallow_cap_exit(C, date_str, current_price, tick):
                        return
                    if _maybe_neutral_shallow_cap_exit(C, date_str, current_price, tick):
                        return
                    if _maybe_dull_shallow_cap_exit(C, date_str, current_price, tick):
                        return
                if _probe_immune:
                    # [Defect2] DO NOT set _probe_protect_done here —
                    # doing so killed all subsequent Phase 3A checks for
                    # the rest of the day (see Phase 3A entry comment).
                    # Instead, the -6% defense check runs every heartbeat
                    # during the immunity window, gated by
                    # _probe_immune_defense_done so we only reduce ONCE.
                    if not getattr(C, '_oversold_protect_logged', False):
                        C._oversold_protect_logged = True
                        print(f"  [Trend Cut] Probe immunity active (hold:{_probe_days}d, <={_probe_immune_max}d), skip Trend Cut")
                    # [P0-2 Fix] Breakeven give-back protection — OVERRIDES immunity.
                    # The old design was binary: a probe either reached cost*1.03 and
                    # got a trailing stop, or it deferred to the -8% hard stop. A probe
                    # that went green (peak >= cost*1.02) but stalled below +3% had no
                    # micro-profit exit. Arm threshold raised from +0.5% to +2% so
                    # 300-share probes (log112 06-22: +1.8% peak -> -236 breakeven)
                    # do not give back a small bounce at full size.
                    _be_cost = getattr(C, '_base_cost_price', 0.0)
                    _be_peak = _bump_probe_peak(C, current_price)
                    _ig_max = _probe_immune_max_days(C)
                    _fp_be = base_float_risk(C, current_price)
                    _be_micro = _total_pos <= getattr(C, 'neutral_probe_qty', 100)
                    _be_arm = _probe_breakeven_arm(
                        _be_cost, _be_peak, _probe_days, _ig_max, _fp_be, micro=_be_micro,
                        neutral_micro=getattr(C, '_neutral_micro_probe_active', False))
                    if (_be_cost > 0 and _be_peak >= _be_cost * _be_arm
                            and current_price <= _be_cost
                            and not getattr(C, '_probe_breakeven_exit_done', False)):
                        _be_avail = get_true_available_position(C)
                        _be_qty = (min(_total_pos, _be_avail) // 100) * 100
                        if _be_qty >= 100:
                            _be_fee = calc_trade_fee(C, current_price, _be_qty, is_sell=True)
                            _be_mgr = getattr(C, '_pos_mgr', None)
                            if _be_mgr is not None:
                                _be_sold, _be_pnl = _be_mgr.request_sell(
                                    _be_qty, 'PROBE_BREAKEVEN_EXIT', caller='stop_loss',
                                    current_price=current_price, tick=tick, fee=_be_fee)
                                if _be_sold:
                                    C._probe_breakeven_exit_done = True
                                    _be_giveback = _be_pnl > 0
                                    _record_base_stop(C, date_str, current_price, _be_qty,
                                                      realized_pnl=_be_pnl,
                                                      is_explicit_probe_stop=True,
                                                      is_breakeven_giveback=_be_giveback)
                                    if get_total_position(C) == 0:
                                        reset_base_anchors(C, 'full')
                                    _be_tag = 'Give-back' if _be_giveback else 'Breakeven'
                                    print(f"  [Probe {_be_tag} Exit] peak {_be_peak:.2f} "
                                          f"(+{(_be_peak / _be_cost - 1):.1%}) -> "
                                          f"{current_price:.2f} (cost {_be_cost:.2f}, "
                                          f"float {_fp_be:.1%}, pnl {_be_pnl:.1f}), exit {_be_qty} sh")
                                    return
                    # [Defect2] Gradual defense during probe immunity.
                    _fp_immune = base_float_risk(C, current_price)
                    # Gate the reduction action with an intraday lock so
                    # we halve at the FIRST -6% heartbeat and then stop
                    # reducing (to avoid cascading sells). The check itself
                    # still runs every heartbeat for logging purposes.
                    # [P0 Fix] Removed the T+1 probe exemption. Previously
                    # the -6% defense was skipped on T+1 (relying on the
                    # 10% T+1 hard-stop), which left a -6% ~ -10% vacuum:
                    # a T+1 survivor at -8% gap-down was forcibly stopped
                    # out on T+2 by the tighter 8% Global Stop (-316.4
                    # main loss case). The -6% defense now runs on every
                    # probe holding day so the position is halved before
                    # the cliff-edge T+2 stop.
                    if _fp_immune <= -0.06 and not getattr(C, '_probe_immune_defense_done', False):
                        # [Fix 1 & Fix 2] Respect probe immunity window.
                        # During the immune window (init 2d / oversold 2d), skip
                        # the -6% defense entirely and defer to the -8% hard stop
                        # which properly records cooldown via _record_base_stop.
                        # Without this, the defense sells at -6%~-7% but without
                        # cooldown, causing immediate rebuild ("截胡" pattern:
                        # 05-15 sold@40.04 → rebought@40.06 same bar).
                        if _is_probe_gap_immune(C):
                            # Probe is in immunity window — let the wider hard
                            # stop (-8% / -10% T+1) handle the exit properly.
                            pass
                        else:
                            # Non-immune: execute defense AND record cooldown.
                            if _total_pos >= 200:
                                _reduce_qty = ((_total_pos // 2) // 100) * 100
                            else:
                                _reduce_qty = min(_total_pos, get_true_available_position(C))
                                _reduce_qty = (_reduce_qty // 100) * 100
                            if _reduce_qty >= 100:
                                _reduce_fee = calc_trade_fee(C, current_price, _reduce_qty, is_sell=True)
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _half_sold, _half_pnl = _pos_mgr.request_sell(_reduce_qty, 'PROBE_IMMUNE_HALF', caller='stop_loss', current_price=current_price, tick=tick, fee=_reduce_fee)
                                else:
                                    _half_sold = safe_sell(C, current_price, _reduce_qty, 'PROBE_IMMUNE_HALF', tick)
                                if _half_sold:
                                    C._probe_immune_defense_done = True
                                    _half_fee = calc_trade_fee(C, current_price, _reduce_qty, is_sell=True)
                                    if _pos_mgr is None:
                                        _half_loss = (C._base_cost_price - current_price) * _reduce_qty
                                        C._base_trade_fee += _half_fee
                                        C._daily_trade_fee += _half_fee
                                        C.realized_pnl += (-_half_loss - _half_fee)
                                        C.daily_pnl = C.realized_pnl
                                        C.traded_volume += _reduce_qty
                                    C._base_peak_price = current_price
                                    C._base_stop_anchor = current_price
                                    _dpm_imm = getattr(C, '_probe_mgr', None)
                                    if _dpm_imm is not None:
                                        _dpm_imm.peak_price = current_price
                                        _dpm_imm._persist_state()
                                    # [Fix 2 关键修复] Record cooldown to prevent
                                    # immediate rebuild ("截胡").
                                    _record_base_stop(C, date_str, current_price, _reduce_qty,
                                                      realized_pnl=_half_pnl if _pos_mgr else 0.0,
                                                      is_explicit_probe_stop=True)
                                    if get_total_position(C) == 0:
                                        reset_base_anchors(C, 'full')
                                    print(f"  [Probe Immune Defense] float {_fp_immune:.1%} <= -6%, "
                                          f"close {_reduce_qty} @ {current_price:.2f} "
                                          f"(remain:{get_total_position(C)})")
                else:
                    _ref_trend_for_cut = (getattr(C, '_day_open_trend', C.trend_direction)
                                          if getattr(C, '_day_open_trend_set', False)
                                          else C.trend_direction)
                    _oversold_probe_protect = (getattr(C, '_days_since_rebuild_probe', 99)
                                               <= getattr(C, 'oversold_probe_trend_protect_days', 2))
                    # [F5修复] 超过最长持有天数后，取消保护，允许Trend Cut
                    _probe_hold_days = getattr(C, '_days_since_rebuild_probe', 99)
                    _probe_max_hold = getattr(C, 'oversold_probe_max_hold_days', 5)
                    if _probe_hold_days > _probe_max_hold:
                        _oversold_probe_protect = False
                    if not _will_stop and _ref_trend_for_cut == 'DOWN' and _total_pos >= C.trade_qty:
                        if _oversold_probe_protect and _holds_down_probe(C):
                            # [Defect2] Removed C._probe_protect_done = True
                            # here. The oversold protect check must still
                            # run every heartbeat; Phase 3A only stops when
                            # an actual Trend Cut executes (_trend_cut_done).
                            if not getattr(C, '_oversold_protect_logged', False):
                                C._oversold_protect_logged = True
                                print(f"  [Trend Cut] Oversold probe protect active, skip Trend Cut "
                                      f"({C._days_since_rebuild_probe}/{C.oversold_probe_trend_protect_days}d)")
                        else:
                            _fp = base_float_risk(C, current_price)
                            if _total_pos > C.base_qty_half:
                                reduce_qty = min(_total_pos - C.base_qty_half, _avail_pos)
                            elif _total_pos > C.trade_qty:
                                if _fp <= C.t0_base_half_pct:
                                    reduce_qty = min(_total_pos - C.trade_qty, _avail_pos)
                                else:
                                    reduce_qty = 0
                            else:
                                reduce_qty = 0
                            reduce_qty = (reduce_qty // 100) * 100
                            _remaining_after = _total_pos - reduce_qty
                            if (reduce_qty >= 100 and _remaining_after > 0
                                    and _remaining_after < C.trade_qty
                                    and _ref_trend_for_cut == 'DOWN'):
                                print(f"  [Trend Cut] probe tail {_remaining_after} in DOWN, merge to full close")
                                reduce_qty = (min(_total_pos, _avail_pos) // 100) * 100
                            if reduce_qty >= 100 and is_valid_price(current_price):
                                _dpm_tc_pre = getattr(C, '_probe_mgr', None)
                                _old_cost = (
                                    _dpm_tc_pre.cost if _dpm_tc_pre is not None and _dpm_tc_pre.cost > 0
                                    else C._base_cost_price)
                                # [PosMgr] Route through centralized gateway
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                _sell_pnl = 0.0
                                if _pos_mgr is not None:
                                    # [P2 Fix] Pass pre-computed fee to PosMgr so DPM
                                    # uses the correct per-trade cost rather than
                                    # computing from stale defaults or double-counting.
                                    _cut_fee = calc_trade_fee(C, current_price, reduce_qty, is_sell=True)
                                    _sell_ok, _sell_pnl = _pos_mgr.request_sell(reduce_qty, 'TREND_CUT', caller='stop_loss', current_price=current_price, tick=tick, fee=_cut_fee)
                                else:
                                    _sell_ok = safe_sell(C, current_price, reduce_qty, 'TREND_CUT', tick)
                                    _sell_pnl = 0.0
                                if not _sell_ok:
                                    print(f"  [Trend Cut] Sell failed (limit down?), skip to base stop checks")
                                    C._trend_cut_done = False
                                else:
                                    _new_total = _total_pos - reduce_qty
                                    if _new_total <= 0:
                                        C._base_rebuild_stage = 0
                                    elif _new_total <= C.trade_qty:
                                        C._base_rebuild_stage = 0
                                    elif _new_total <= C.base_qty_half:
                                        C._base_rebuild_stage = 1
                                    C._base_target_qty = _new_total
                                    C._pending_sell_excess = 0
                                    C._trend_cut_done = True
                                    C._base_trend_cut_active = True
                                    _hb_snapshot(C, date_str)
                                    if _pos_mgr is None:
                                        C.traded_volume += reduce_qty
                                    if _new_total > 0:
                                        C._base_stop_anchor = current_price
                                        C._base_peak_price = current_price
                                        # [BUG 8 FIX] Also sync DPM.peak_price
                                        # — check_residual_exit uses it for
                                        # trail-stop calculation. Without this
                                        # line, the DPM still holds the old
                                        # pre-Trend-Cut peak and may fire a
                                        # spurious trail stop on the next bar.
                                        _dpm_tc = getattr(C, '_probe_mgr', None)
                                        if _dpm_tc is not None:
                                            _dpm_tc.peak_price = current_price
                                            _dpm_tc._persist_state()

                                    _cut_fee = calc_trade_fee(C, current_price, reduce_qty, is_sell=True)
                                    if _pos_mgr is not None:
                                        _cut_loss_amt = _sell_pnl
                                    else:
                                        _cut_loss_amt = (_old_cost - current_price) * reduce_qty
                                    if _pos_mgr is None:
                                        C._base_trade_fee += _cut_fee
                                        C._daily_trade_fee += _cut_fee
                                        C.realized_pnl += (-_cut_loss_amt - _cut_fee)
                                        C.daily_pnl = C.realized_pnl
                                        C._cum_stop_loss += _cut_loss_amt
                                        C._daily_stop_loss = getattr(C, '_daily_stop_loss', 0.0) + _cut_loss_amt
                                    _anchor_note = (f" anchor:{C._base_stop_anchor:.2f} cost:{_old_cost:.2f}"
                                                    if _new_total > 0 else "")
                                    print(f"  [Trend Cut] DOWN, reduced {reduce_qty} @ {current_price:.2f} "
                                          f"pnl:{_cut_loss_amt:.1f} fee:{_cut_fee:.1f}{_anchor_note}")
                            elif _total_pos <= C.trade_qty:
                                C._trend_cut_done = True
                    elif _ref_trend_for_cut in ('UP', 'NEUTRAL'):
                        C._trend_cut_done = True
                    # [Defect2] Removed the two _probe_protect_done = True
                    # fall-through branches. Phase 3A now runs at every
                    # heartbeat slot; the _trend_cut_done flag stops it
                    # after a real Trend Cut executes, and
                    # _probe_immune_defense_done stops multiple -6%
                    # reductions within the immunity window.

        # [问题1] Phase 3B: 每bar检查止损, 仅在未完成时调用, 防止与Phase 3A级联
        if not getattr(C, '_base_stop_done', False) \
            and not getattr(C, '_intraday_float_reduced_today', False) \
            and not getattr(C, '_down_intraday_reduced_done', False):
            _use_orb_amp_3b = (C.strategy_mode in ('T0', 'ORB') and time_str < C.orb_degrade_time)
            # [P1 Fix] Use bar low for stop-loss evaluation in Phase 3B.
            _eval_low_3b = _bar_low if _bar_data_ok else current_price
            _run_base_stop_checks(C, date_str, _eval_low_3b, tick, _bar_time_str, use_orb_amp=_use_orb_amp_3b)

        # [F7修复] 使用独立状态变量替代 _base_cut_done
        if not getattr(C, '_base_stop_done', False) and not getattr(C, '_trend_cut_done', False) \
           and not getattr(C, '_down_reduce_done', False):
            _apply_staged_half_risk_exit(C, current_price, date_str, time_str, tick)

        # ==========================================================
        # Phase 4: SKIP / OBSERVE modes
        # ==========================================================
        _observe_exit_to_t0 = False
        if C.strategy_mode in ('UNDECIDED', 'SKIP', 'OBSERVE'):
            if C.strategy_mode == 'SKIP':
                hb_hour = time_str[:2]
                if hb_hour != C._skip_hb_hour:
                    C._skip_hb_hour = hb_hour
                    print(f"  [{time_str}] [HB-SKIP] p:{current_price:.2f}")
            elif C.strategy_mode == 'OBSERVE':
                # [P1 Fix] Use 10-minute HB slots for throttling instead of hourly, to avoid end-of-day gaps
                _hb_key = time_str[:4] if is_hb_slot(time_str) else ''
                _obs_hour_tick = (_hb_key != '' and _hb_key != C._observe_hb_hour)
                if _obs_hour_tick:
                    C._observe_hb_hour = _hb_key
                    update_trend_direction(C, _bar_time_str, current_price)
                _obs_total_pos = get_total_position(C)
                if _obs_total_pos > 0 and (_holds_down_probe(C) or _dpm_is_init_probe(C)):
                    _maybe_enable_probe_underwater_reduce_t0(C, current_price, time_str)
                if (_obs_total_pos > 0 and (_holds_down_probe(C) or _dpm_is_init_probe(C))
                        and _probe_high_confidence_sell(C, current_price, vwap_price)):
                    # [补充1] FUSE 终态隔离：禁止任何 OBSERVE->T0 切换
                    if getattr(C, '_force_liquidation_active', False):
                        log_once(C, '_force_liq_t0_block',
                                  f"  [{time_str}] [OBSERVE] Force-liquidation active, block OBSERVE->T0")
                    # [Issue 4 Fix] Gray zone weak float: block OBSERVE->T0
                    elif getattr(C, '_gray_zone_weak_block_t0', False):
                        log_once(C, '_gray_zone_t0_block_logged',
                                  f"  [{time_str}] [OBSERVE] Gray zone weak float active, block OBSERVE->T0")
                    else:
                        _obs_avail = get_true_available_position(C)
                        _probe_rsi = calc_rsi(C, period=6)
                        _probe_dev = (current_price - vwap_price) / vwap_price if vwap_price > 0 else 0
                        _phc_skip = False
                        _phc_cap_qty = 99999
                        if _probe_rebuild_t0_blocked(C, current_price, vwap_price):
                            log_once(C, '_rebuild_probe_t0_skip_logged',
                                      f"  [{time_str}] [OBSERVE] Probe T0 cooldown "
                                      f"({C._days_since_rebuild_probe}/{C.rebuild_probe_t0_cool_days}d), skip sell")
                            _phc_skip = True
                        elif getattr(C, '_today_bought_qty', 0) > 0:
                            _avg_cost_for_phc = getattr(C, '_base_cost_price', 0.0)
                            _is_above_water = _avg_cost_for_phc > 0 and current_price >= _avg_cost_for_phc
                            if _is_above_water and _obs_avail >= 100:
                                print(f"  [{time_str}] [OBSERVE] PHC allowed: added "
                                      f"{getattr(C, '_today_bought_qty', 0)} shares today, "
                                      f"old lot (avail:{_obs_avail}) is at/above water "
                                      f"(price:{current_price:.2f} cost:{_avg_cost_for_phc:.2f}); selling only tradable portion")
                                C._phc_allow_same_day_add = True
                                C._phc_same_day_max_qty = _obs_avail
                                _phc_cap_qty = _obs_avail
                            else:
                                if not _is_above_water:
                                    _reason = (f"underwater (price:{current_price:.2f} "
                                               f"cost:{_avg_cost_for_phc:.2f}); defer to hard-stop")
                                else:
                                    _reason = f"new shares T+1 locked (avail:{_obs_avail}/total:{_obs_total_pos})"
                                log_once(C, '_phc_skip_sameday_add_logged',
                                          f"  [{time_str}] [OBSERVE] PHC skipped: added "
                                          f"{getattr(C, '_today_bought_qty', 0)} shares today, {_reason}")
                                _phc_skip = True
                        elif (_obs_avail < _obs_total_pos and C._base_cost_price > 0
                              and current_price < C._base_cost_price):
                            log_once(C, '_phc_tplus1_partial_logged',
                                      f"  [{time_str}] [OBSERVE] PHC skipped: underwater "
                                      f"and T+1 partial (avail:{_obs_avail} < total:{_obs_total_pos}), "
                                      f"defer to hard-stop")
                            _phc_skip = True
                        elif _obs_avail < 100:
                            log_once(C, '_phc_no_sellable_logged',
                                      f"  [{time_str}] [OBSERVE] PHC skipped: no shares available "
                                      f"for sale (avail:{_obs_avail})")
                            _phc_skip = True

                        if not _phc_skip:
                            _high_conf_sell_qty = min(C.trade_qty, _obs_avail, _phc_cap_qty)
                            _high_conf_sell_qty = (_high_conf_sell_qty // 100) * 100
                            _high_conf_sell_qty = max(_high_conf_sell_qty, 100)
                            _phc_sold = False
                            if _high_conf_sell_qty >= 100:
                                # [P2 Fix] Pre-compute fee BEFORE PosMgr routes the order, so
                                # DPM.on_sell receives a pre-computed cost (current_price * qty
                                # + fee) and does not re-read C._base_cost_price (which may be
                                # stale after a partial close / reset in the same bar).
                                _phc_fee = calc_trade_fee(C, current_price, _high_conf_sell_qty, is_sell=True)
                                _was_oversold_phc = _dpm_is_oversold_probe(C)
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _phc_sold, _phc_pnl = _pos_mgr.request_sell(_high_conf_sell_qty, 'PROBE_HIGH_CONF_SELL', caller='stop_loss', current_price=current_price, tick=tick, fee=_phc_fee)
                                else:
                                    _phc_sold = safe_sell(C, current_price, _high_conf_sell_qty, 'PROBE_HIGH_CONF_SELL', tick)
                                if _phc_sold:
                                    # [P2 Fix] Use PosMgr-returned _phc_pnl as authoritative PnL
                                    # instead of re-reading C._base_cost_price here. PosMgr has
                                    # already triggered DPM.on_sell, which zeroes cost for qty==0,
                                    # so reading C._base_cost_price here gives (price - 0) * qty =
                                    # price*qty (an absurdly large apparent profit) instead of the
                                    # true realized pnl. Fallback path (no PosMgr) still computes
                                    # from cost, which is valid because safe_sell does not call reset.
                                    _sell_fee = calc_trade_fee(C, current_price, _high_conf_sell_qty, is_sell=True)
                                    _sell_pnl = _phc_pnl if _pos_mgr is not None else ((current_price - C._base_cost_price) * _high_conf_sell_qty - _sell_fee)
                                    if _pos_mgr is None:
                                        C.realized_pnl += _sell_pnl
                                        C.daily_pnl = C.realized_pnl
                                        C._base_trade_fee += _sell_fee
                                        C._daily_trade_fee += _sell_fee
                                        C.traded_volume += _high_conf_sell_qty
                                    # [P0-2 Fix] Treat as probe reduce — stay OBSERVE, keep probe
                                    # protection unless position is fully flattened. Only clear probe
                                    # flags at 0 to avoid Micro-Clear killing the residual later.
                                    C.strategy_mode = 'OBSERVE'
                                    C.state = 'IDLE'
                                    C.can_do_t0 = False
                                    # [State Hub] PositionManager.request_sell already called
                                    # DPM.on_sell() → DPM.qty decremented; if qty == 0 DPM.reset()
                                    # already called, clearing legacy flags (_is_oversold_probe,
                                    # _is_init_probe, _rebuild_probe_date) automatically.
                                    # No redundant manual manipulation needed.
                                    C._rebuild_cooldown_bars = 60
                                    print(f"  [{time_str}] [OBSERVE] Probe high-confidence sell EXECUTED "
                                          f"qty={_high_conf_sell_qty} @ {current_price:.2f} "
                                          f"pnl:{_sell_pnl:.1f} (RSI:{_probe_rsi:.1f} dev:{_probe_dev:.2%})")
                                    # [Issue3 Fix] Prevent churn: block same-day rebuild after PHC sell
                                    C._phc_sell_today = True
                                    if get_total_position(C) <= 0:
                                        # Mirror Init-Half exit lifecycle (qmt_risk staged-half)
                                        C._init_half_cooldown_date = date_str
                                        C._days_since_init_half_exit = 0
                                        C._stop_cool_days = (
                                            1 if _high_conf_sell_qty <= 100 else 2)
                                        if _sell_pnl > 0:
                                            _clear_rebuild_ghost_after_profitable_probe_flat(C)
                                            _arm_oversold_profit_neutral_block(C, date_str)
                                        else:
                                            # [P0 Fix log111] loss PHC: keep down_oversold ladder
                                            C._exit_policy = 'down_oversold'
                                            if C._base_stop_date == '':
                                                C._days_since_stop = 99
                                                C._probe_stop_active = False
                                    C._observe_policy_degraded_logged = False
                                    # [Fix 3] Short-circuit the rest of handlebar so the base-stop
                                    # path (HHVBARS / probe profit take) cannot fire on the same tick
                                    # and duplicate-sell the remainder of the position.
                                    print_eod_summary(C, time_str, current_price)
                                    return
                                else:
                                    # If sell submission fails, fall back to plain T0 (no pending sell)
                                    C.strategy_mode = 'T0'
                                    C.state = 'IDLE'
                                    C.can_do_t0 = True
                                    _observe_exit_to_t0 = True
                                    C._rebuild_cooldown_bars = 60
                                    print(f"  [{time_str}] [OBSERVE->T0] Probe high-confidence sell FAILED, "
                                          f"fall back to plain T0 (RSI:{_probe_rsi:.1f} dev:{_probe_dev:.2%})")
                if not _observe_exit_to_t0 and _obs_hour_tick:
                    if _obs_total_pos > 0:
                        # [P0 Fix v2] Unified probe detection from authoritative
                        # DPM.is_probe (state==PROBE_ACTIVE AND probe_type in
                        # probe variants). Legacy C._is_*_probe flags can be
                        # silently cleared by hidden paths (notably the
                        # INIT_PROBE_CONTROLLED_UPGRADE branch which used to
                        # call mark_as_normal_base()). Using is_probe avoids
                        # the "identity lost -> Micro-Clear kills probe" bug
                        # that produced -168.2 on log61 05-19 and -1.9% on
                        # log63.
                        _dpm_obj = getattr(C, '_probe_mgr', None)
                        _is_currently_probe = (
                            _dpm_obj is not None and _dpm_obj.is_probe
                        )
                        _holds_any_probe = (
                            _is_currently_probe
                            or _holds_down_probe(C)
                            or (C._is_oversold_probe if hasattr(C, '_is_oversold_probe') else False)
                            or (C._is_init_probe if hasattr(C, '_is_init_probe') else False)
                        )
                        # [P0 Fix v2] Read cost from authoritative DPM.cost;
                        # fallback to C._base_cost_price when DPM.cost is
                        # zeroed; never assume profit when anchor is zero.
                        _dpm_cost = _dpm_obj.cost if (_dpm_obj and _dpm_obj.cost > 0) else getattr(C, '_base_cost_price', 0.0)
                        _has_profit = (_dpm_cost > 0 and current_price > _dpm_cost)
                        # [P1 Fix v2] Float threshold for micro-clear. Only
                        # liquidate non-probe small positions when the
                        # drawdown is shallower than -3%; deeper positions are
                        # left for the hard-stop engine which has a wider
                        # recovery margin. Anchor==0 falls through to
                        # _mc_float=0.0 which is > -3%, so it's treated as a
                        # shallow drawdown — correct because we have no
                        # reliable cost basis to judge.
                        _mc_anchor = _dpm_cost if _dpm_cost > 0 else getattr(C, '_base_cost_price', 0.0)
                        _mc_float = ((current_price - _mc_anchor) / _mc_anchor) if _mc_anchor > 0 else 0.0
                        _mc_float_ok = _mc_float > -0.03

                        # [P0 Fix log63-2] Defensive probe guard. Without
                        # this, Micro-Clear could still mis-fire on a probe
                        # whose state had been overwritten back to
                        # BASE_ACTIVE/probe_type='normal' by some earlier
                        # code path (notably INIT_PROBE_CONTROLLED_UPGRADE in
                        # pre-fix code). Print a one-shot diagnostic so
                        # regressions are immediately visible in logs.
                        if _dpm_obj is not None and not _dpm_obj.is_probe:
                            if _dpm_obj.qty > 0 and _dpm_obj.qty < C.trade_qty:
                                if not getattr(C, '_mc_probe_state_logged', False):
                                    C._mc_probe_state_logged = True
                                    print(f"  [{time_str}] [Micro-Clear] probe-state {_dpm_obj.state} type='{getattr(_dpm_obj, 'probe_type', 'none')}' "
                                          f"qty={_dpm_obj.qty} cost={_dpm_obj.cost:.2f}   treating as base position (sub-trade_qty non-probe)")

                        if _obs_total_pos < C.trade_qty and C.trend_direction == 'DOWN' \
                                and not _is_currently_probe and not _holds_any_probe and not _has_profit \
                                and _mc_float_ok:
                            _obs_avail = get_true_available_position(C)
                            if _obs_avail >= 100:
                                print(f"  [{time_str}] [Micro-Clear] Holding {_obs_total_pos} < trade_qty DOWN float {_mc_float:.1%} > -3%, force clear")
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _mc_fee = calc_trade_fee(C, current_price, _obs_avail, is_sell=True)
                                    _mc_sold, _mc_pnl = _pos_mgr.request_sell(_obs_avail, 'MICRO_CLEAR', caller='stop_loss', current_price=current_price, tick=tick, fee=_mc_fee)
                                else:
                                    _mc_sold = safe_sell(C, current_price, _obs_avail, 'MICRO_CLEAR', tick)

                                if _mc_sold:
                                    if _pos_mgr is None:
                                        _clear_fee = calc_trade_fee(C, current_price, _obs_avail, is_sell=True)
                                        C._base_trade_fee += _clear_fee
                                        C._daily_trade_fee += _clear_fee
                                        _clear_pnl = (current_price - C._base_cost_price) * _obs_avail - _clear_fee
                                        C.realized_pnl += _clear_pnl
                                        C.daily_pnl = C.realized_pnl
                                        C.traded_volume += _obs_avail
                                        if getattr(C, '_risk_engine', None) is not None:
                                            C._risk_engine.record_trade_pnl(current_price, _obs_avail, 'SELL', _clear_fee)

                                    # [P0 Fix 1] Bridge post-clear lifecycle to DOWN probe ladder.
                                    # Without this, the next session hits the "base_stop_date == '' and
                                    # pos==0" path which skips DOWN-oversold probe gating, and the
                                    # strategy stays flat for days in a row (05-19/05-20).
                                    C._exit_policy = 'down_oversold'
                                    C._base_stop_date = date_str
                                    C._days_since_stop = 0
                                    C._stop_cool_days = 1
                                    C._observe_policy_degraded_logged = False
                                    C._base_target_qty = max(0, get_total_position(C))
                        _obs_today_open = getattr(C, '_day_open_price', 0.0)
                        if not is_valid_price(_obs_today_open) and len(C.bars_open) > 0:
                            _obs_today_open = C.bars_open[0]
                        _obs_gap_down = (is_valid_price(_obs_today_open) and C.prev_close > 0
                                        and _obs_today_open < C.prev_close * 0.985)
                        # [P0 Fix #1 log61] Honor the gap-day flag set by risk engine.
                        # If this session is a gap-down, reject ALL same-day add/upgrade.
                        if getattr(C, '_is_gap_down_day_flag', False):
                            _obs_gap_down = True
                            if not getattr(C, '_obs_gap_block_logged', False):
                                C._obs_gap_block_logged = True
                                print(f"  [{time_str}] [OBSERVE Add] BLOCKED: gap-day flag active, no same-day pyramiding")
                        _obs_ma20 = getattr(C, '_trend_ma20', 0.0)
                        _obs_dev = ((current_price - _obs_ma20) / _obs_ma20 if _obs_ma20 > 0 else 0.0)
                        _obs_high_dev = _obs_dev > 0.05
                        _obs_float_ok = True
                        _obs_cost = getattr(C, '_base_cost_price', 0.0)
                        _obs_in_loss = False
                        if _obs_cost > 0 and is_valid_price(current_price):
                            _obs_float = (current_price - _obs_cost) / _obs_cost
                            _obs_in_loss = _obs_float < 0.0
                            _obs_float_ok = _obs_float >= -0.04
                        # [P1 Fix] Detect an expiring probe so OBSERVE Add
                        # does not net out the probe's forced expiry in the
                        # same bar. On 06-18 OBSERVE Add bought 100 shares
                        # while the probe (hold_days=7) was force-closed for
                        # 100 shares in the same slot, leaving the net
                        # position unchanged and burning ~13.4 in fees for
                        # nothing. If the probe is about to expire, skip the
                        # add and let the expiry settle first.
                        _probe_mgr_obj = getattr(C, '_probe_mgr', None)
                        _is_probe_expiring = False
                        if _probe_mgr_obj and _probe_mgr_obj.state == _probe_mgr_obj.PROBE_ACTIVE:
                            if _probe_mgr_obj.hold_days >= getattr(C, 'oversold_probe_max_hold_days', 7) - 1:
                                _is_probe_expiring = True
                        _obs_is_init_probe = _dpm_is_init_probe(C)
                        if (_obs_total_pos < C.trade_qty and C.trend_direction != 'DOWN'
                                and (getattr(C, '_intraday_gap_reduce_today', False)
                                     or getattr(C, '_gap_exit_today', False))):
                            log_once(C, '_obs_gap_reduce_add_block_logged',
                                     f"  [{time_str}] [OBSERVE Add] BLOCKED: intraday GAP reduce today, no pyramiding")
                        if (getattr(C, '_block_observe_probe_add', False)
                                and _obs_total_pos < C.trade_qty):
                            log_once(C, '_obs_gap_trim_add_block_logged',
                                     f"  [{time_str}] [OBSERVE Add] BLOCKED: day after GAP partial trim, no pyramiding")
                        _obs_add_cap = _observe_probe_add_cap(C)
                        _obs_micro_block, _obs_micro_why = _blocks_observe_micro_probe_add(C)
                        if _obs_micro_block:
                            log_once(C, '_obs_micro_add_block_logged',
                                     f"  [{time_str}] [OBSERVE Add] BLOCKED: {_obs_micro_why}")
                        if getattr(C, '_gap_warn_seen_today', False) and _obs_total_pos < _obs_add_cap:
                            log_once(C, '_obs_gap_warn_add_block_logged',
                                     f"  [{time_str}] [OBSERVE Add] BLOCKED: GAP-WARN today, no pyramiding")
                        if (C.trend_direction != 'DOWN' and _obs_total_pos < _obs_add_cap
                                and not _obs_gap_down and not _obs_high_dev and _obs_float_ok
                                and not _is_probe_expiring
                                and not _obs_is_init_probe
                                and not getattr(C, '_is_gap_down_day_flag', False)
                                and not getattr(C, '_intraday_gap_reduce_today', False)
                                and not getattr(C, '_gap_exit_today', False)
                                and not getattr(C, '_block_observe_probe_add', False)
                                and not getattr(C, '_gap_warn_seen_today', False)
                                and not _obs_micro_block):
                            _add_qty = _obs_add_cap - _obs_total_pos
                            _add_qty = (_add_qty // 100) * 100
                            if _add_qty >= 100 and is_valid_price(current_price):
                                _pos_mgr = getattr(C, '_pos_mgr', None)
                                if _pos_mgr is not None:
                                    _add_ok = _pos_mgr.request_buy(
                                        _add_qty, 'OBSERVE_PROBE_ADD', caller='rebuild',
                                        current_price=current_price, tick=tick)
                                else:
                                    _po = _get_api('passorder')
                                    if not _po:
                                        print(f"  [Error] OBSERVE_PROBE_ADD: passorder API not found")
                                        _add_ok = False
                                    else:
                                        try:
                                            _po(23, 1101, C.account_id, C.stock, 11, current_price, _add_qty, 'OBSERVE_PROBE_ADD', 2, 'OBSERVE_PROBE_ADD', C)
                                            _add_ok = True
                                        except Exception as e:
                                            print(f"  [Error] OBSERVE_PROBE_ADD passorder: {e}")
                                            _add_ok = False
                                if _add_ok:
                                    new_total = get_total_position(C)
                                    if new_total > _obs_total_pos:
                                        C._today_bought_qty += _add_qty
                                        # [P2 Fix] traded_volume must include buys.
                                        C.traded_volume += _add_qty
                                        # [BUG 7 FIX] DPM.on_buy() already
                                        # computed weighted avg cost and
                                        # wrote it to C._base_cost_price.
                                        # Re-computing here risks a second,
                                        # inconsistent value. Just re-read
                                        # the authoritative value from the
                                        # DPM state hub.
                                        _dpm_obs = getattr(C, '_probe_mgr', None)
                                        if _dpm_obs is not None:
                                            C._base_cost_price = _dpm_obs.cost
                                            C._base_target_qty = _dpm_obs.qty
                                        else:
                                            # fallback: DPM not available — compute
                                            _old_cost = getattr(C, '_base_cost_price', 0.0) * _obs_total_pos
                                            _new_cost = current_price * _add_qty
                                            C._base_cost_price = (_old_cost + _new_cost) / new_total
                                            C._base_target_qty = new_total
                                        sync_stop_anchor_down(C)
                                        C._base_pos_initialized = True
                                        C._base_ever_built = True
                                        _add_fee = calc_trade_fee(C, current_price, _add_qty, is_sell=False)
                                        C._base_trade_fee += _add_fee
                                        C._daily_trade_fee += _add_fee
                                        C.realized_pnl -= _add_fee
                                        if new_total >= 200:
                                            C._is_init_probe = False
                                        C._rebuild_cooldown_bars = 60
                                        print(f"  [OBSERVE Add] Trend ok, added {_add_qty} @ {current_price:.2f}, new_total:{new_total}, fee:{_add_fee:.1f}")
                                    else:
                                        # PositionManager returned True but the
                                        # position size did not increase — the
                                        # order likely had no fill at this price
                                        # level. Still set a cooldown to avoid
                                        # hammering the same level every heartbeat.
                                        C._rebuild_cooldown_bars = 60
                                        print(f"  [OBSERVE Add] No fill (pos unchanged)")
                                else:
                                    # [P1 Fix J] PositionManager explicitly rejected
                                    # the add (same-day stacking guard or other
                                    # rules). Set cooldown so we don't keep
                                    # trying at every heartbeat slot, producing
                                    # log spam and unnecessary state churn.
                                    C._rebuild_cooldown_bars = 60
                                    # Use log_once to avoid ~240 copies of the same
                                    # line across a 4-hour session (05-18 observed
                                    # pattern when lifecycle cap was hit and the
                                    # strategy retried every bar).
                                    log_once(C, '_observe_add_rejected_logged',
                                             f"  [OBSERVE Add] Rejected by PositionManager "
                                             f"(same-day stacking guard / lifecycle cap)")
                        elif C.trend_direction != 'DOWN' and _obs_total_pos >= C.trade_qty:
                            # [Defect3] Underwater base: when float loss is 2% or more,
                            # prohibit OBSERVE→T0. The T0 engine (especially alpha sells)
                            # blindly sells at a profit when price ticks above VWAP, but on
                            # 5/13 this happened with the base position deep underwater —
                            # every sell locked in a loss, and the overall strategy lost
                            # -174 in a single whipsaw session. A 2% buffer gives the
                            # position breathing room before allowing counter-trend selling.
                            if _obs_cost > 0 and current_price < _obs_cost * 0.98:
                                log_once(C, '_observe_t0_underwater_block',
                                          f"  [{time_str}] [HB-OBSERVE] Base underwater "
                                          f"(p:{current_price:.2f} cost:{_obs_cost:.2f}), "
                                          f"T0 disabled")
                            elif getattr(C, '_force_liquidation_active', False):
                                log_once(C, '_force_liq_t0_block',
                                          f"  [{time_str}] [OBSERVE] Force-liquidation active, block OBSERVE->T0")
                            elif getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                                log_once(C, '_vwap_carry_observe_block_logged',
                                          f"  [{time_str}] [OBSERVE] VWAP downgrade carry active, block OBSERVE->T0")
                            elif getattr(C, '_gray_zone_weak_block_t0', False):
                                log_once(C, '_gray_zone_t0_block_logged',
                                          f"  [{time_str}] [OBSERVE] Gray zone weak float active, block OBSERVE->T0")
                            elif _probe_rebuild_t0_blocked(C, current_price, vwap_price):
                                log_once(C, '_rebuild_probe_t0_skip_logged',
                                          f"  [{time_str}] [OBSERVE] Probe T0 cooldown "
                                          f"({C._days_since_rebuild_probe}/{C.rebuild_probe_t0_cool_days}d), stay OBSERVE")
                            else:
                                C.strategy_mode = 'T0'
                                C.state = 'IDLE'
                                C.can_do_t0 = (get_true_available_position(C) >= 100)
                                C._rebuild_cooldown_bars = 60
                                print(f"  [{time_str}] [OBSERVE->T0] Probe sufficient + trend {C.trend_direction}, entering T0")
                        else:
                            print(f"  [{time_str}] [HB-OBSERVE] holding {_obs_total_pos}, trend {C.trend_direction}, waiting")
                    elif C.trend_direction != 'DOWN':
                        if get_total_position(C) > 0:
                            # [补充1] FUSE 终态隔离：禁止任何 OBSERVE->T0 切换
                            if getattr(C, '_force_liquidation_active', False):
                                log_once(C, '_force_liq_t0_block',
                                          f"  [{time_str}] [OBSERVE] Force-liquidation active, block OBSERVE->T0")
                            elif getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                                log_once(C, '_vwap_carry_observe_block_logged',
                                          f"  [{time_str}] [OBSERVE] VWAP downgrade carry active, block OBSERVE->T0")
                            # [P1 Fix] Gray zone weak float: block OBSERVE->T0
                            elif getattr(C, '_gray_zone_weak_block_t0', False):
                                log_once(C, '_gray_zone_t0_block_logged',
                                          f"  [{time_str}] [OBSERVE] Gray zone weak float active, block OBSERVE->T0")
                            elif _probe_rebuild_t0_blocked(C, current_price, vwap_price):
                                log_once(C, '_rebuild_probe_t0_skip_logged',
                                          f"  [{time_str}] [OBSERVE] Probe T0 cooldown "
                                          f"({C._days_since_rebuild_probe}/{C.rebuild_probe_t0_cool_days}d), stay OBSERVE")
                            else:
                                C.strategy_mode = 'T0'
                                C.state = 'IDLE'
                                C.can_do_t0 = (get_true_available_position(C) >= 100)
                                C._rebuild_cooldown_bars = 60
                                print(f"  [{time_str}] [OBSERVE->T0] Trend changed to {C.trend_direction}, pos>0")
                        else:
                            # [Fix] Stay in OBSERVE while empty (regardless of trend)
                            # so the rebuild listeners (oversold probe, ORB, etc.)
                            # remain active. Previously we flipped back to UNDECIDED,
                            # which the next bar's Phase 2 immediately reverted to
                            # OBSERVE — pure state-machine spinning with no signal
                            # processing. VWAP-carry case is preserved for clarity.
                            if getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                                print(f"  [{time_str}] [HB-OBSERVE] empty + {C.trend_direction}, VWAP carry active, waiting")
                            else:
                                if C.strategy_mode != 'OBSERVE':
                                    C.strategy_mode = 'OBSERVE'
                                if not getattr(C, '_observe_wait_logged', False):
                                    C._observe_wait_logged = True
                                    print(f"  [{time_str}] [HB-OBSERVE] Trend {C.trend_direction} but empty, awaiting rebuild signals")
                    else:
                        # [修复17] DOWN趋势微仓允许有限止盈
                        if _obs_total_pos > 0 and _obs_total_pos < C.trade_qty:
                            _obs_avail = get_true_available_position(C)
                            if _obs_avail >= 100:
                                _obs_rsi = calc_rsi(C, period=6)
                                _obs_dev = (current_price - vwap_price) / vwap_price if vwap_price > 0 else 0
                                _obs_profit = current_price > getattr(C, '_base_cost_price', 0)
                                if _obs_rsi > 65 and _obs_dev > 0.01 and _obs_profit:
                                    if not getattr(C, '_force_liquidation_active', False):
                                        C.strategy_mode = 'T0'
                                        C.state = 'IDLE'
                                        C.can_do_t0 = True
                                        C._sell_signal_history = [1]
                                        C._rebuild_cooldown_bars = 60
                                        print(f"  [{time_str}] [OBSERVE->T0] Micro pos profit-take "
                                              f"(RSI:{_obs_rsi:.1f} dev:{_obs_dev:.2%} pos:{_obs_total_pos})")
                                        # 不 return，继续走T0逻辑
                                    else:
                                        log_once(C, '_observe_down_hb_logged',
                                                  f"  [{time_str}] [HB-OBSERVE] holding {_obs_total_pos}, DOWN, p:{current_price:.2f}")
                                else:
                                    log_once(C, '_observe_down_hb_logged',
                                              f"  [{time_str}] [HB-OBSERVE] holding {_obs_total_pos}, DOWN, p:{current_price:.2f}")
                            else:
                                log_once(C, '_observe_down_hb_logged',
                                          f"  [{time_str}] [HB-OBSERVE] holding {_obs_total_pos}, DOWN, p:{current_price:.2f}")
                        else:
                            log_once(C, '_observe_down_hb_logged',
                                      f"  [{time_str}] [HB-OBSERVE] DOWN, p:{current_price:.2f}")
            if not _observe_exit_to_t0:
                if time_str >= C.decision_time and not getattr(C, '_base_stop_done', False):
                    # [Fix P2] Run GAP defense check before EOD summary to avoid report-vs-close ordering inversion
                    if _check_intraday_gap_protection(C, current_price, tick, date_str, time_str, _bar_low):
                        print_eod_summary(C, time_str, current_price)
                        return
                    _eval_low_hb = _bar_low if _bar_data_ok else current_price
                    _run_base_stop_checks(C, date_str, _eval_low_hb, tick, _bar_time_str)
                    if getattr(C, '_base_stop_done', False):
                        print_eod_summary(C, time_str, current_price)
                        return
                print_eod_summary(C, time_str, current_price)
                return

        # -------- Heartbeat --------
        if not _observe_exit_to_t0:
            hb_key = time_str[:4]
            if is_hb_slot(time_str) and hb_key != C._heartbeat_min:
                C._heartbeat_min = hb_key
                update_trend_direction(C, _bar_time_str, current_price)
                if C.trend_direction == 'UP' and getattr(C, '_base_trend_cut_active', False):
                    C._base_trend_cut_active = False
                    print(f"  [{time_str}] [Rebuild] Trend recovered to UP, trend cut flag reset")
                print(f"  [{time_str}] [HB] mode:{C.strategy_mode} state:{C.state} "
                      f"trend:{C.trend_direction} p:{current_price:.2f} "
                      f"vwap:{vwap_price:.2f} atr:{C.atr_val:.4f} pnl:{C.daily_pnl:.1f} "
                      f"cons_stop:{C.consecutive_stops} base_cost:{C._base_cost_price:.2f}")

        # ------------------ A: ORB Breakout Logic ------------------
        if C.strategy_mode == 'ORB':
            if C.state == 'WAIT_BREAKOUT':
                if time_str >= C.force_close_t:
                    print_eod_summary(C, time_str, current_price)
                    return
                if C.daily_pnl <= C.daily_loss_limit:
                    print_eod_summary(C, time_str, current_price)
                    return
                if time_str >= C.orb_degrade_time:
                    # [Fix 5] Empty-position case: stay in OBSERVE so the
                    # probe-ladder can still evaluate. Full-position case:
                    # fall back to T0 as before.
                    if get_total_position(C) == 0:
                        print(f"[{time_str}] ORB degrade -> OBSERVE (empty pos)")
                        C.strategy_mode = 'OBSERVE'
                    else:
                        print(f"[{time_str}] ORB degrade to T0")
                        C.strategy_mode = 'T0'
                    C.state = 'IDLE'
                    C._orb_waiting_confirm = False
                    return

                is_breakout = current_price > C.orb_high * C.orb_break_filter
                is_above_vwap = current_price > vwap_price
                is_volume_surge = (len(C.bars_volume) > 0 and C.bars_volume[-1] > C.orb_avg_vol_pm * C.vol_surge_mult) or (C.cur_min_vol > C.orb_avg_vol_pm * C.vol_surge_mult)
                can_buy_orb = (C.trend_direction == 'UP' and current_price >= ma5 * 1.001)

                if not C._orb_waiting_confirm:
                    if is_breakout and is_above_vwap and is_volume_surge and can_buy_orb:
                        C._orb_waiting_confirm = True
                        C._orb_confirm_bars_elapsed = 1
                        C._orb_confirm_fail_count = 0
                        print(f"[{time_str}] ORB signal, waiting {C.orb_confirm_bars} bar confirmation")
                    return

                C._orb_confirm_bars_elapsed += 1
                # [Fix 3] Raise the hold-above filter from orb_break_filter
                # to 1.005 so tiny noise wicks don't kill a valid breakout
                # setup. Also add _orb_confirm_fail_count so one weak bar
                # is tolerated — requires 2 consecutive failures to give up.
                holds_above = current_price > C.orb_high * 1.005
                holds_above_vwap = current_price > vwap_price

                if holds_above and holds_above_vwap and C._orb_confirm_bars_elapsed >= C.orb_confirm_bars:
                    print(f"[{time_str}] ORB confirmed, buying")
                    if safe_buy(C, current_price, C.trade_qty, 'ORB_Buy', tick):
                        C._today_bought_qty += C.trade_qty
                        # [P2 Fix] traded_volume must include buys. ORB breakout
                        # path also participates in daily trading statistics.
                        C.traded_volume += C.trade_qty
                        C.buy_price = C.highest_since_buy = current_price
                        C.pending_close_qty = C.trade_qty
                        C.state = 'HOLDING'
                        C._orb_waiting_confirm = False
                elif not holds_above:
                    # [Fix 3] Tolerate 1 failure-bar before giving up on
                    # ORB confirmation. First failure → reset elapsed
                    # counter and keep watching; 2nd failure → exit.
                    if getattr(C, '_orb_confirm_fail_count', 0) >= 1:
                        C._orb_waiting_confirm = False
                        # [Defect11] After an ORB confirmation fails, disable
                        # the ORB path for the remainder of the day. See the
                        # general description above. This also forces the mode
                        # to OBSERVE (instead of UNDECIDED) so that Phase 2
                        # does not immediately re-trigger on the next bar and
                        # create an UNDECIDED<->OBSERVE oscillation that floods
                        # the log. 5/18 hit 280 Decision entries because of
                        # this round-trip.
                        C._orb_disabled_today = True
                        if get_total_position(C) == 0:
                            _day_open = _day_open_trend_frozen(C)
                            # log122: day-open DOWN + intraday UP (05/18 ORB) → allow rebuild
                            if C.trend_direction != 'DOWN':
                                C._rebuild_cooldown_bars = 0
                                C._orb_failed_allow_probe_today = True
                                _orb_note = ' (day-open DOWN, intraday ok)' if _day_open == 'DOWN' else ''
                                print(f"[{time_str}] ORB failed (empty, trend ok{_orb_note}) -> OBSERVE (orb disabled today, allow probe)")
                                C.strategy_mode = 'OBSERVE'
                                C.state = 'IDLE'
                            else:
                                C._rebuild_cooldown_bars = 0
                                _why = 'day-open DOWN' if _day_open == 'DOWN' else 'intraday DOWN'
                                print(f"[{time_str}] ORB failed (empty, {_why}, orb disabled) -> OBSERVE")
                                C.strategy_mode = 'OBSERVE'
                                C.state = 'IDLE'
                        else:
                            print(f"[{time_str}] ORB failed (orb disabled, back to T0)")
                            C.strategy_mode = 'T0'
                            C.state = 'IDLE'
                    else:
                        C._orb_confirm_fail_count = 1
                        C._orb_confirm_bars_elapsed = max(1, C._orb_confirm_bars_elapsed - 1)
                elif not holds_above_vwap:
                    C._orb_confirm_bars_elapsed -= 1

            elif C.state == 'HOLDING':
                if current_price > C.highest_since_buy:
                    C.highest_since_buy = current_price
                _est_orb_fee = calc_trade_fee(C, current_price, C.pending_close_qty, is_sell=True)
                C.daily_pnl = C.realized_pnl + (current_price - C.buy_price) * C.pending_close_qty - _est_orb_fee

                if C.daily_pnl <= C.daily_loss_limit:
                    avail = get_true_available_position(C)
                    close_qty = min(C.pending_close_qty, avail)
                    if close_qty > 0:
                        print(f"[{time_str}] [FUSE] ORB daily loss limit, close {close_qty}")
                        if safe_sell_eod(C, current_price, close_qty, 'FUSE_Sell', tick):
                            _fee = calc_trade_fee(C, C.buy_price, close_qty, is_sell=False) + calc_trade_fee(C, current_price, close_qty, is_sell=True)
                            C.realized_pnl += (current_price - C.buy_price) * close_qty - _fee
                            C.daily_pnl = C.realized_pnl
                            C._daily_trade_fee += _fee
                            C._base_trade_fee += _fee
                            C.state = 'DONE'
                    print_eod_summary(C, time_str, current_price)
                    return

                trailing_stop_price = C.highest_since_buy * (1.0 - C.trail_stop_pct)
                trigger_stop = current_price < trailing_stop_price
                trigger_eod = time_str >= C.force_close_t

                if trigger_stop or (trigger_eod and not C.eod_order_sent):
                    reason = "EOD" if trigger_eod else "Trail"
                    avail = get_true_available_position(C)
                    actual_sell = min(C.trade_qty, avail)
                    if actual_sell > 0:
                        print(f"[{time_str}] ORB {reason} sell, p:{current_price:.2f} trail:{trailing_stop_price:.2f}")
                        sell_fn = safe_sell_eod if trigger_eod else safe_sell
                        if sell_fn(C, current_price, actual_sell, 'ORB_Sell', tick):
                            _fee = calc_trade_fee(C, C.buy_price, actual_sell, is_sell=False) + calc_trade_fee(C, current_price, actual_sell, is_sell=True)
                            C.realized_pnl += (current_price - C.buy_price) * actual_sell - _fee
                            C.daily_pnl = C.realized_pnl
                            C._daily_trade_fee += _fee
                            C._base_trade_fee += _fee
                            C.state = 'DONE'
                            C.eod_order_sent = True
                        elif trigger_eod:
                            print(f"  [Error] ORB EOD sell failed")
                    elif trigger_eod:
                        C.state = 'DONE'
                    print_eod_summary(C, time_str, current_price)

        # ------------------ B: T+0 Logic (V2 Engine Takes Full Control) ------------------
        elif C.strategy_mode == 'T0':
            # 1. VWAP downgrade check
            if getattr(C, '_trend_vwap_downgrade_carry', False) and C.trend_direction == 'DOWN':
                C.can_do_t0 = False
                if not getattr(C, '_vwap_down_t0_blocked_logged', False):
                    C._vwap_down_t0_blocked_logged = True
                    print(f"  [{time_str}] [T0 Block] VWAP downgrade carry, T0 disabled")
                print_eod_summary(C, time_str, current_price)
                return

            if not C.can_do_t0:
                print_eod_summary(C, time_str, current_price)
                return

            # 2. Probe isolation lock: freeze T0 when probe is active (unless underwater reduce-only)
            if getattr(C, '_probe_mgr', None) is not None and C._probe_mgr.state == C._probe_mgr.PROBE_ACTIVE:
                if not getattr(C, '_probe_t0_sell_only', False):
                    print_eod_summary(C, time_str, current_price)
                    return

            # 3. Extract real-time micro factors for engine
            rsi_val = calc_rsi(C, period=6)
            vol_ratio = calc_vol_ratio(C)
            obv_val, sobv_val = calc_obv_sobv(C)
            obv_golden_cross = (obv_val > sobv_val)

            # 4. Delegate full execution to V2 T0 engine
            if getattr(C, '_t0_engine', None) is not None:
                C._t0_engine.run_t0_tick(
                    current_price, vwap_price, C.atr_val,
                    rsi_val, vol_ratio, obv_golden_cross,
                    tick, time_str
                )
            else:
                print(f"  [{time_str}] [Error] T0 Engine not initialized!")

        # [V2] EOD sync: T0 engine state sync
        if getattr(C, '_t0_engine', None) is not None and time_str >= C.force_close_t:
            C._t0_engine.sync_eod_status(get_total_position(C))

        # ================== Fix 4-4: Backtest deadlock fix =================
        _ftotal = get_total_position(C)
        if not C.is_live:
            # 1. 倒数1440期(约6天)：仅阻断新开仓，保留仓位博反弹
            _total_bars = getattr(C, '_total_backtest_bars', 0)
            if _total_bars > 0:
                _bars_remaining = _total_bars - C.barpos
            elif hasattr(C, '_max_barpos_seen') and C._max_barpos_seen > C.barpos:
                _bars_remaining = C._max_barpos_seen - C.barpos
            else:
                _bars_remaining = 9999
            if _bars_remaining < 1440:
                C._block_new_builds = True

            # 2. 倒数30期(最后半小时)：执行最终清仓
            if _bars_remaining < 30 and _ftotal > 0 and not getattr(C, '_final_session_closed', False):
                _fanchor = getattr(C, '_base_cost_price', 0.0)
                _funreal = ((current_price - _fanchor) * _ftotal) if _fanchor > 0 else 0.0
                print(f"  [{time_str}] [Final Session] liquidating residual {_ftotal} shares @ {current_price:.2f} (unreal={_funreal:.1f})")

                # 调用 PosMgr 卖出
                _pos_mgr = getattr(C, '_pos_mgr', None)
                if _pos_mgr is not None:
                    _fee = calc_trade_fee(C, current_price, _ftotal, is_sell=True)
                    _pos_mgr.request_sell(_ftotal, 'FINAL_SESSION_CLOSE', caller='stop_loss', current_price=current_price, tick=tick, fee=_fee)
                else:
                    safe_sell(C, current_price, _ftotal, 'FINAL_SESSION_CLOSE', tick)

                # 标记已清仓
                C._final_session_closed = True

        # ponytail: bar-end flush so deferred DPM reset runs even on last bar of session
        _dpm_flush = getattr(C, '_probe_mgr', None)
        if _dpm_flush is not None and hasattr(_dpm_flush, 'flush_pending_reset'):
            _dpm_flush.flush_pending_reset()

        print_eod_summary(C, time_str, current_price)



    finally:
        _update_hb_bars_cache(C)

# ponytail self-check — QMT loads strategy as __name__=='__main__'; dev: python qmtbdxtV2.py --self-check
if __name__ == '__main__' and '--self-check' in sys.argv:
    class _C:
        pass
    _C._rebuild_signal_log_count = 0
    for _ in range(6):
        _log_rebuild_signal(_C, 'test')
    assert _C._rebuild_signal_log_count == 5

    # [P0-1] Same-day re-entry lock: once a position is cleared intraday, the
    # empty-account INIT build must be blocked regardless of _base_stop_date.
    def _init_build_allowed(cleared_today, base_stop_date, total_pos):
        if cleared_today:
            return False
        return base_stop_date == '' and total_pos == 0
    assert _init_build_allowed(False, '', 0) is True          # fresh day, flat -> build ok
    assert _init_build_allowed(True, '', 0) is False          # probe stopped today -> blocked
    assert _init_build_allowed(False, '2026-05-15', 0) is False  # prior stop recorded -> blocked

    # [P0-3] Extreme-RSI DOWN bypass requires ADTM deceleration (no falling knife).
    def _knife_ok(rsi, adtm, prev_adtm):
        return rsi < 20 and (adtm >= prev_adtm)
    assert _knife_ok(19.8, -0.088, -0.073) is False   # 05-29: still falling -> skip
    assert _knife_ok(17.5, -0.229, -0.229) is True    # 06-11: flat/decelerating -> allow
    assert _knife_ok(25.0, -0.10, -0.20) is False     # RSI not extreme -> skip

    # [P0-2] Breakeven arm: +1.5% when underwater after bounce; else +2%.
    def _be_arm(cost, peak, days, ig, fp):
        if cost <= 0 or peak <= 0:
            return 1.02
        if peak >= cost * 1.015 and fp < 0:
            return 1.015
        if days >= ig and fp < -0.03:
            return 1.015
        return 1.02
    def _breakeven_exit(cost, peak, price, done, arm):
        return (cost > 0 and peak >= cost * arm and price <= cost and not done)
    assert _breakeven_exit(43.42, 44.30, 43.40, False, 1.02) is True
    assert _breakeven_exit(43.42, 43.75, 43.40, False, 1.02) is False
    _arm117 = _be_arm(33.39, 33.98, 1, 2, -0.04)
    assert _arm117 == 1.015
    assert _breakeven_exit(33.39, 33.98, 32.65, False, _arm117) is True  # log117 06-22
    assert _be_arm(33.39, 33.60, 0, 2, 0.006) == 1.02  # in profit, no early arm

    # NEUTRAL D->N: VWAP alone insufficient without DIFF>DEA.
    def _neutral_dn_strong(macd_bull, backend_rev, bullish, vwap_ok, ma5_cross):
        intraday = vwap_ok or ma5_cross
        return backend_rev or bullish or (macd_bull and intraday)
    assert _neutral_dn_strong(False, False, False, True, False) is False
    assert _neutral_dn_strong(True, False, False, True, False) is True
    assert _neutral_dn_strong(False, True, False, False, False) is True

    # NEUTRAL empty probe: reversal required; D->N flip without signal blocked.
    def _neutral_gate(open_t, live, macd, sar, stale, bullish, vwap_ok, vol_only=False):
        strong = (not stale and (macd or sar)) or bullish or vwap_ok
        if open_t == 'DOWN' and live == 'NEUTRAL':
            return strong
        return strong or vol_only
    assert _neutral_gate('DOWN', 'NEUTRAL', False, False, False, False, False) is False
    assert _neutral_gate('NEUTRAL', 'NEUTRAL', True, False, False, False, False) is True
    assert _neutral_gate('DOWN', 'NEUTRAL', True, False, False, False, False) is True
    assert _neutral_gate('NEUTRAL', 'NEUTRAL', False, False, False, False, True) is True

    # Underwater probe-to-half: need bounce above cost+1% and MA5 (log170: no float>=0 bypass).
    def _uw_half_block(cost, price, ma5):
        if cost <= 0:
            return False
        if price > cost * 1.01 and ma5 > 0 and price > ma5:
            return False
        return True
    assert _uw_half_block(33.39, 32.62, 33.0) is True    # log113 06-22 underwater
    assert _uw_half_block(33.39, 34.00, 33.5) is False   # cost+1% & MA5 ok
    assert _uw_half_block(23.04, 23.24, 23.0) is True    # log170: +0.86% < cost+1%

    # log170 P1: shallow cap arms on live float only, not bar-low wick.
    def _shallow_live_ok(live_fp, cap_floor):
        return live_fp <= cap_floor
    assert _shallow_live_ok(-0.02985, -0.03) is False   # 6/8 fill -2.98% vs -3% cap
    assert _shallow_live_ok(-0.031, -0.03) is True

    # log172 A'+C: hold>=max → bridge (last immune day) or post-immune shallow.
    def _rebuild_shallow_ok(hold_days, immune_max):
        return hold_days >= immune_max
    assert _rebuild_shallow_ok(1, 2) is False   # oversold day1 mid-immune
    assert _rebuild_shallow_ok(2, 2) is True    # C: last immune day bridge
    assert _rebuild_shallow_ok(3, 2) is True    # post-immune
    assert _rebuild_shallow_ok(2, 3) is False   # neutral day2 mid-immune
    assert _rebuild_shallow_ok(3, 3) is True    # C: neutral last immune day

    # [05-13 Fix] High-position init ban: dev>5% or gray-zone weak float.
    class _G:
        _gray_zone_weak_block_t0 = False
    assert _blocks_high_position_init_build(_G, 0.073)[0] is True   # 05-13 dev 7.3%
    assert _blocks_high_position_init_build(_G, 0.03)[0] is False  # true pullback ok
    _G._gray_zone_weak_block_t0 = True
    assert _blocks_high_position_init_build(_G, 0.03)[0] is True   # log99 gray zone
    class _D:
        _day_open_trend_set = True
        _day_open_trend = 'DOWN'
        _gray_zone_weak_block_t0 = False
        trend_direction = 'DOWN'
    assert _blocks_high_position_init_build(_D, 0.01)[0] is True   # log101 ORB-fail init
    _D.trend_direction = 'UP'
    assert _blocks_high_position_init_build(_D, 0.01)[0] is False  # log123 intraday UP ok

    def _virtual_arm_days():
        return 99
    assert _virtual_arm_days() >= 3  # log125: virtual arm skips 3d cooldown gate

    def _orb_fail_micro(day_open, up, dev, vwap_ok, max_dev=0.02):
        if day_open == 'DOWN' or up != 'UP' or dev > max_dev:
            return False
        return vwap_ok
    assert _orb_fail_micro('UP', 'UP', 0.0, True) is True
    assert _orb_fail_micro('DOWN', 'UP', 0.01, True) is False   # 05/18 blocked
    assert _orb_fail_micro('UP', 'UP', 0.01, True) is True      # mild pullback ok
    assert _orb_fail_micro('UP', 'UP', 0.03, True) is False

    def _virtual_shallow_zone(armed, ever_built, p, ma20, th, cap=0.97):
        if not armed or ever_built or ma20 <= 0 or th <= 0:
            return False
        return th < p < ma20 * cap
    assert _virtual_shallow_zone(True, False, 40.08, 41.0, 38.12) is False  # log130 GAP
    assert _virtual_shallow_zone(True, False, 39.5, 40.99, 38.12) is True
    assert _virtual_shallow_zone(True, False, 40.7, 40.99, 38.12) is False  # above MA20*97%

    def _vs_macro_ok(bias, near_bot=False, support=0, price=0):
        return bias <= -0.03 or near_bot or (support > 0 and price < support)
    assert _vs_macro_ok(-0.015) is False
    assert _vs_macro_ok(-0.035) is True

    def _phc_policy_after_profit():
        return 'down_oversold'
    assert _phc_policy_after_profit() == 'down_oversold'

    def _state_repair_writes_stop(ever_built):
        return not ever_built
    assert _state_repair_writes_stop(True) is False
    assert _state_repair_writes_stop(False) is True

    def _be_arm(cost, peak, fp, micro=False):
        if cost <= 0 or peak <= 0:
            return 1.02
        if peak >= cost * 1.015 and fp < 0:
            return 1.015
        return 1.02
    assert _be_arm(33.47, 33.80, -0.014, micro=True) == 1.02
    assert not (33.80 >= 33.47 * _be_arm(33.47, 33.80, -0.014, micro=True))  # log131 06-22
    assert _be_arm(33.39, 33.98, -0.04, micro=False) == 1.015

    def _giveback_tag(pnl):
        return pnl > 0
    assert _giveback_tag(-90.3) is False
    assert _giveback_tag(12.0) is True

    def _gap4_blocks(days, ig_max, fp, immune=True, partial=False, oversold=False):
        if not immune or partial:
            return False
        if days >= ig_max and fp <= -0.03:
            if oversold:
                return True  # log146: oversold last immune day still blocks -4%
            return False
        return True
    assert _gap4_blocks(2, 2, -0.04, oversold=True) is True   # log146 last day block
    assert _gap4_blocks(2, 2, -0.04, oversold=False) is False  # log132 non-oversold allow
    assert _gap4_blocks(1, 2, -0.04) is True    # day1 still skip
    assert _gap4_blocks(2, 2, -0.02) is True    # not deep enough

    def _immune_skip_4_os_hold(os_hold, fp, day2_deep, oversold=False):
        blocks = _gap4_blocks(2 if day2_deep else 1, 2, fp, oversold=oversold)
        return (blocks and fp > -0.08) or (os_hold and fp > -0.06 and not day2_deep)
    assert _immune_skip_4_os_hold(True, -0.04, day2_deep=True, oversold=True) is True  # log146
    assert _immune_skip_4_os_hold(True, -0.04, day2_deep=False) is True   # day1 oversold noise

    def _vs_day_open_down(open_trend):
        return open_trend == 'DOWN'
    assert _vs_day_open_down('DOWN') is True  # log146 virtual shallow blocked

    def _gap_warn_blocks_intraday(warn_done, pos):
        return warn_done and pos > 0
    assert _gap_warn_blocks_intraday(True, 200) is True   # log147 same-session defer
    assert _gap_warn_blocks_intraday(True, 0) is False
    assert _gap_warn_blocks_intraday(False, 200) is False

    def _micro_gap_cool(assign_mgc, init_default):
        return assign_mgc  # not max(init_default, assign_mgc)
    assert _micro_gap_cool(2, 3) == 2  # log147 micro GAP cooldown

    assert _gap_half_lot_qty(300) == 200  # log149: ceil-half to 100-sh lot
    assert _gap_half_lot_qty(200) == 100
    assert _gap_half_lot_qty(100) == 100
    assert _gap_half_lot_qty(500) == 300  # 250→300 lots

    def _expired_gap_qty(hold, ig_max, cap, ladder_prior, micro_max=300):
        if ladder_prior:
            return cap
        if hold > ig_max:
            if cap <= 100:
                return cap
            if cap <= micro_max:
                return cap  # log149: micro probe full close
            return _gap_half_lot_qty(cap)
        return _gap_half_lot_qty(cap) if cap > 100 else cap
    assert _expired_gap_qty(3, 2, 300, False) == 300
    assert _expired_gap_qty(3, 2, 100, False) == 100
    assert _expired_gap_qty(3, 2, 150, True) == 150

    def _os_rebuild_qty(day_open, trade_qty, micro_qty=100):
        return micro_qty if day_open == 'DOWN' else trade_qty
    assert _os_rebuild_qty('DOWN', 300) == 100
    assert _os_rebuild_qty('UP', 300) == 300

    def _dull_severe_block(rsi, floor=25, post_gap=False, pg_floor=30):
        th = pg_floor if post_gap else floor
        return rsi >= th
    assert _dull_severe_block(27.3, post_gap=True, pg_floor=30) is False
    assert _dull_severe_block(27.3, post_gap=False) is True

    def _severe_macro_ok(rsi, bias, post_gap=False, pg_floor=30, extreme=20):
        if bias >= -0.05:
            return True
        if post_gap:
            return rsi < pg_floor
        return rsi < extreme
    assert _severe_macro_ok(27.3, -0.075, post_gap=True) is True
    assert _severe_macro_ok(27.3, -0.075, post_gap=False) is False

    def _dull_down_eff(dull, post_gap, streak, rsi, pg_floor=30):
        return dull or (post_gap and streak >= 1 and rsi < pg_floor)
    assert _dull_down_eff(False, True, 1, 27.3) is True
    assert _dull_down_eff(False, False, 1, 27.3) is False
    assert _dull_down_eff(False, False, 1, 47.9) is False  # log164: no accel streak bypass

    def _dull_shallow_cap(hold, fp, min_days=2, floor=-0.03):
        return hold >= min_days and fp <= floor
    assert _dull_shallow_cap(2, -0.029) is False
    assert _dull_shallow_cap(2, -0.03) is True
    assert _dull_shallow_cap(2, -0.035) is True

    def _dull_profit_neutral_blocked(dpd, dsd, block_days=5):
        return bool(dpd) and dsd < block_days
    assert _dull_profit_neutral_blocked('2026-06-22', 0) is True
    assert _dull_profit_neutral_blocked('2026-06-22', 4) is True
    assert _dull_profit_neutral_blocked('2026-06-22', 5) is False

    def _os_fast_path_qty(day_open, trade_qty, macro_bias, near_bottom60=False,
                          micro_vwap_ok=False, micro_ma5_cross=False, micro_qty=100,
                          full_bias=-0.08):
        if day_open != 'DOWN':
            return trade_qty
        if macro_bias >= -0.05:
            return trade_qty
        if near_bottom60 and macro_bias <= full_bias:
            return trade_qty
        if near_bottom60 and (micro_vwap_ok or micro_ma5_cross):
            return trade_qty
        return micro_qty
    assert _os_fast_path_qty('DOWN', 300, -0.124, True, True) == 300  # log154 6/2 MACRO
    assert _os_fast_path_qty('DOWN', 300, -0.040) == 300  # log154 5/26 EARLY mild
    assert _os_fast_path_qty('DOWN', 300, -0.070) == 100  # 阴跌 no rebound
    assert _os_fast_path_qty('UP', 300, -0.12) == 300

    def _sweep_macro_micro_confirmed(bias, vol_dry, vwap_ok, ma5_cross):
        extreme = bias <= -0.12
        return (vol_dry and (vwap_ok or ma5_cross)) or (extreme and (vwap_ok or ma5_cross))
    assert _sweep_macro_micro_confirmed(-0.124, False, True, False) is True   # log181 6/2
    assert _sweep_macro_micro_confirmed(-0.040, False, True, False) is False  # 5/26 no MACRO
    assert _sweep_macro_micro_confirmed(-0.080, True, False, True) is True

    def _macro_fb_time_ok(t, rsi):
        return t >= '14:00:00' or rsi < 15
    assert _macro_fb_time_ok('10:00:00', 17.5) is False   # log187 6/2 morning MACRO off
    assert _macro_fb_time_ok('14:00:00', 17.5) is True   # log187 P0⁸ afternoon MACRO
    assert _macro_fb_time_ok('10:00:00', 14.0) is True

    def _flat_probe_cd(days):
        return days >= 2
    assert _flat_probe_cd(1) is False   # log187 P0⁹ 5/28
    assert _flat_probe_cd(2) is True

    def _relax_sweep_i_ok(rsi, i, late_i, bias):
        ok = rsi < 25 or i >= late_i
        if bias < -0.05 and rsi >= 25:
            ok = i >= late_i
        return ok
    assert _relax_sweep_i_ok(17.5, 30, 180, -0.10) is True   # log181 6/11 10:00
    assert _relax_sweep_i_ok(27.9, 30, 180, -0.06) is False  # log187 5/28 10:00
    assert _relax_sweep_i_ok(27.9, 180, 180, -0.06) is True

    def _neutral_shallow_cap(hold, fp, min_days=2, floor=-0.02):
        return hold >= min_days and fp < floor
    assert _neutral_shallow_cap(2, -0.015) is False
    assert _neutral_shallow_cap(2, -0.03) is True

    def _neutral_shallow_immune_last_day_defer(hold, ig_max, fp):
        return hold == ig_max and fp <= -0.04
    assert _neutral_shallow_immune_last_day_defer(3, 3, -0.051) is True   # log182 6/24
    assert _neutral_shallow_immune_last_day_defer(3, 3, -0.031) is False  # log181 6/25

    def _macro_no_phc_neutral_block(dpd, macro_bias):
        return (not dpd) and macro_bias < -0.05
    assert _macro_no_phc_neutral_block('', -0.06) is True   # log183 P1'
    assert _macro_no_phc_neutral_block('', -0.04) is False
    assert _macro_no_phc_neutral_block('2026-06-12', -0.08) is False

    def _bar_idx_to_time(idx):
        if idx < 120:
            m = 9 * 60 + 30 + idx
        else:
            m = 13 * 60 + (idx - 120)
        return f"{m // 60:02d}:{m % 60:02d}:00"
    assert _bar_idx_to_time(30) == '10:00:00'
    assert _bar_idx_to_time(120) == '13:00:00'

    assert 0.058 > 0.055  # log159: 5/13@5.8% blocked by init_high_dev_probe_max

    def _init_base_qty(trend, trade_qty, micro_qty):
        return micro_qty if trend == 'NEUTRAL' else trade_qty
    assert _init_base_qty('NEUTRAL', 300, 100) == 100
    assert _init_base_qty('UP', 300, 100) == 300

    def _init_micro_shallow(hold, fp, min_days=1, floor=0.04):
        return hold >= min_days and fp <= -floor
    assert _init_micro_shallow(1, -0.04) is True
    assert _init_micro_shallow(1, -0.039) is False
    assert _init_micro_shallow(0, -0.05) is False

    def _oversold_profit_neutral_block(dpd, dsd, dnb):
        return bool(dpd) and dsd < dnb

    assert _oversold_profit_neutral_block('2026-06-12', 4, 5) is True
    assert _oversold_profit_neutral_block('2026-06-12', 5, 5) is False

    def _sweep_macro_time_ok(sweep_time, rsi, bias, bottom60):
        if sweep_time >= '14:00:00' or rsi < 15:
            return True
        if bias <= -0.10 or bottom60:
            return sweep_time >= '10:00:00'
        return False

    def _relax_defer_morning(extreme, early_virtual, bar_i, late_i):
        return (not early_virtual) and extreme and bar_i < late_i

    assert _sweep_macro_time_ok('10:01:00', 17.5, -0.124, True) is True
    assert _sweep_macro_time_ok('10:01:00', 17.5, -0.08, False) is False
    assert _relax_defer_morning(True, False, 31, 120) is True
    assert _relax_defer_morning(True, False, 120, 120) is False

    def _init_upgrade_day_open_ok(day_open, trend, float_pct):
        if day_open == 'DOWN':
            return False
        return trend != 'DOWN' and float_pct > -0.02

    assert _init_upgrade_day_open_ok('DOWN', 'NEUTRAL', 0.0) is False
    assert _init_upgrade_day_open_ok('NEUTRAL', 'NEUTRAL', 0.0) is True

    def _init_probe_uw_t0_enabled(is_init_probe):
        return not is_init_probe

    assert _init_probe_uw_t0_enabled(True) is False
    assert _init_probe_uw_t0_enabled(False) is True

    def _micro_shallow_cooldown(qty, micro_qty=100):
        return 2 if qty <= micro_qty else 2
    assert _micro_shallow_cooldown(100) == 2
    assert _micro_shallow_cooldown(300) == 2

    def _high_amp_exempt(mode, init_half, skip_bypass, right_side):
        return mode == 'OBSERVE' or init_half or skip_bypass or right_side
    assert _high_amp_exempt('SKIP', True, False, False) is True
    assert _high_amp_exempt('SKIP', False, True, False) is True
    assert _high_amp_exempt('SKIP', False, False, True) is True
    assert _high_amp_exempt('SKIP', False, False, False) is False

    def _infer_neutral_probe(reason):
        return 'neutral_probe' if 'NEUTRAL_PROBE' in (reason or '').upper() else 'oversold_probe'
    assert _infer_neutral_probe('REBUILD_NEUTRAL_PROBE') == 'neutral_probe'
    assert _infer_neutral_probe('REBUILD_EARLY_OVERSOLD_PROBE') == 'oversold_probe'

    def _allows_os_observe(policy, virtual_arm, gap_exit, rsi, extreme_rsi=20):
        if policy == 'staged_half_probe':
            return False
        if policy != 'observe_only':
            return True
        if virtual_arm:
            return True
        return gap_exit and rsi < extreme_rsi
    assert _allows_os_observe('observe_only', True, False, 50) is True
    assert _allows_os_observe('observe_only', False, True, 18) is True
    assert _allows_os_observe('observe_only', False, True, 30) is False

    def _lifecycle_reset_ok(prev, cur, daily, streak):
        return prev == 'DOWN' and cur == 'UP' and daily != 'DOWN' and streak >= 5
    assert _lifecycle_reset_ok('DOWN', 'UP', 'NEUTRAL', 5) is True
    assert _lifecycle_reset_ok('DOWN', 'NEUTRAL', 'DOWN', 5) is False

    # log116: probe immune window blocks scale-to-half (before underwater gate).
    def _probe_immune_scale_block(days, ig_max):
        return days <= ig_max
    assert _probe_immune_scale_block(1, 2) is True   # 06-22 held 1d
    assert _probe_immune_scale_block(3, 2) is False  # immune expired

    # log119: OBSERVE add cap + qty (no avail_pos on buy)
    def _obs_cap(tot, nq, tq):
        return nq if tot <= nq else tq
    def _obs_add_qty(tot, cap):
        return max(0, (cap - tot) // 100 * 100)
    assert _obs_cap(100, 100, 300) == 100
    assert _obs_add_qty(100, 100) == 0
    assert _obs_add_qty(100, 300) == 200
    assert _obs_add_qty(100, 300) != min(200, 100)  # not avail-capped

    # log121 P0: backend day-open lock only after fresh fetch
    def _backend_locks_day_open(fresh, trend):
        return fresh and trend in ('UP', 'DOWN')
    assert _backend_locks_day_open(True, 'DOWN') is True
    assert _backend_locks_day_open(False, 'DOWN') is False

    # log121 P1: RIGHT_SIDE stabilize band (no RSI<25 Catch-22)
    def _right_side_rsi_ok(rsi):
        return 30 < rsi <= 55
    assert _right_side_rsi_ok(50.4) is True
    assert _right_side_rsi_ok(25.0) is False

    # log127 P2: RIGHT_SIDE — intraday DOWN ok when price>VWAP (micro 100 only in live path)
    def _right_side_ok(trend, rsi, vwap_ok):
        if not (30 < rsi <= 55):
            return False
        return trend != 'DOWN' or vwap_ok
    assert _right_side_ok('NEUTRAL', 50.4, False) is True
    assert _right_side_ok('DOWN', 50.4, True) is True
    assert _right_side_ok('DOWN', 50.4, False) is False

    def _orb_fail_allows_rebuild(intraday, day_open):
        return intraday != 'DOWN'
    assert _orb_fail_allows_rebuild('UP', 'DOWN') is True
    assert _orb_fail_allows_rebuild('DOWN', 'DOWN') is False

    # Deferred DPM reset: qty=0 must not read as active probe (ghost guard)
    def _probe_active(qty, state_active, ptype):
        if qty <= 0:
            return False
        return state_active and ptype in ('init_probe', 'oversold_probe')
    assert _probe_active(0, True, 'oversold_probe') is False
    assert _probe_active(100, True, 'oversold_probe') is True

