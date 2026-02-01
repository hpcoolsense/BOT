import os
import time
import threading
import traceback
import sys
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple, Any
from pathlib import Path

try:
    root_dir = Path(__file__).resolve().parent.parent
    sys.path.append(str(root_dir))
    from status import SystemStatus
except ImportError:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except Exception:
    pass

from exchanges.pacifica_api import PacificaClient
from exchanges.lighter_api import LighterClient
from notifications.telegram_notifier import TelegramNotifier

SYMBOL_DEFAULT        = os.getenv("SYMBOL", "ETH-USDT")
QUOTE_DEFAULT         = os.getenv("QUOTE", "USDT")
EDGE_THRESHOLD        = float(os.getenv("EDGE_THRESHOLD", "0.0005"))
ENGINE_TPSL_PCT       = float(os.getenv("ENGINE_TPSL_PCT", "0.01")) 
TRADE_BASE_QTY        = float(os.getenv("TRADE_BASE_QTY", "0.05"))
BASE_QTY_STEP         = float(os.getenv("BASE_QTY_STEP", "0.0001"))

VERBOSE               = True
LOOP_DELAY_MS         = int(os.getenv("ENGINE_TICK_MS", "50"))
GAP_BETWEEN_PAIRS_MS  = int(os.getenv("MIN_MS_BETWEEN_PAIRS", "800"))
ENGINE_PAPER          = os.getenv("ENGINE_PAPER", "false").lower() == "true"
NO_SLEEP_ON_SIGNAL    = os.getenv("ENGINE_NO_SLEEP_ON_SIGNAL", "true").lower() == "true"
DISABLE_GAP_THROTTLE  = os.getenv("ENGINE_DISABLE_GAP_THROTTLE", "true").lower() == "true"
FAST_JOIN_SECS        = float(os.getenv("ENGINE_FAST_JOIN_SECS", "0.03"))
WARMUP_CLIENTS_ON_START = os.getenv("ENGINE_WARMUP_CLIENTS_ON_START", "true").lower() == "true"
STATUS_POLL_SECS      = float(os.getenv("ENGINE_STATUS_POLL_SECS", "10"))

def _round_step(x: float, step: float) -> float:
    if step <= 0: return x
    n = round(x / step)
    return round(n * step, 10)

def _round_tick(px: float, tick: float) -> float:
    t = float(tick or 0.0)
    if t <= 0: return float(px)
    return round(round(float(px) / t) * t, 12)

def _best_from_book(feed) -> Tuple[Optional[float], Optional[float]]:
    try:
        bb = max((p for p, _ in getattr(feed.book, "bids", [])), default=None)
        ba = min((p for p, _ in getattr(feed.book, "asks", [])), default=None)
        return bb, ba
    except: return None, None

@dataclass
class OrderFill:
    ok: bool
    msg: str
    filled_qty: Optional[float] = None

class ArbEngine:
    def __init__(self, pac, lig, symbol=SYMBOL_DEFAULT, quote=QUOTE_DEFAULT, mode="PACIFICA"):
        self.engine_paper = ENGINE_PAPER
        self.trade_base_qty = TRADE_BASE_QTY
        self.base_qty_step = BASE_QTY_STEP
        self.tpsl_pct = ENGINE_TPSL_PCT
        self.pacifica_tick = 0.01
        self.symbol_base = symbol.split("-")[0]
        self.pac = pac
        self.lig = lig
        self.state = "idle"
        self._stop = threading.Event()
        self._thr = None
        self._lock = threading.Lock()
        self.open_active = False
        self.open_direction = None
        self.open_qty = None
        self._pac_client = None
        self._lig_client = None
        self._tg = TelegramNotifier()
        try: self.status_checker = SystemStatus()
        except: self.status_checker = None
        self._system_ready = threading.Event()
        self._system_ready.set()
        self.pairs_executed_total = 0
        self._last_pair_time_ms = 0.0

    def _maybe_build_clients(self) -> bool:
        if self.engine_paper: return True
        if self._pac_client and self._lig_client: return True
        try:
            self._pac_client = PacificaClient()
            self._lig_client = LighterClient()
            return True
        except Exception as e:
            print(f"❌ Error conectando clientes: {e}")
            return False

    def _warmup_clients(self):
        if not self.engine_paper: self._maybe_build_clients()

    def _now_ms(self) -> float:
        return time.perf_counter() * 1000.0

    def _best_bid_ask_safe(self, feed) -> Tuple[Optional[float], Optional[float]]:
        bb = getattr(feed, "best_bid", None)
        ba = getattr(feed, "best_ask", None)
        if bb is not None and ba is not None: return bb, ba
        return _best_from_book(feed)

    @staticmethod
    def _edge_p2l(pac_ask, lig_bid):
        if pac_ask and lig_bid: return (lig_bid - pac_ask) / pac_ask
        return None

    @staticmethod
    def _edge_l2p(lig_ask, pac_bid):
        if lig_ask and pac_bid: return (pac_bid - lig_ask) / lig_ask
        return None

    def _tg_send_async(self, text: str):
        threading.Thread(target=lambda: self._tg.send(text), daemon=True).start()

    # =========================================================================
    # GRID 5x5
    # =========================================================================
    def _create_grid_tpsl_5x5(self, direction, pac_ask, pac_bid, lig_ask, lig_bid):
        if self.engine_paper: return
        time.sleep(1.5)
        
        print(f"[GRID] Iniciando cálculo de Grid 5x5 para {direction}...")

        if direction.endswith("->Lig"): 
            avg_entry = (pac_ask + lig_bid) / 2.0
            pac_exit, lig_exit = "SELL", "BUY" 
            is_long_pac = True
        else: 
            avg_entry = (pac_bid + lig_ask) / 2.0
            pac_exit, lig_exit = "BUY", "SELL"
            is_long_pac = False

        NUM_SPLITS = 5
        qty_total = self.open_qty if self.open_qty else self.trade_base_qty
        qty_fragment = _round_step(qty_total / NUM_SPLITS, self.base_qty_step)
        step_pct = self.tpsl_pct / NUM_SPLITS

        print(f"[GRID] Entry Ref: {avg_entry:.4f} | Fragmento: {qty_fragment} | Step: {step_pct*100:.2f}%")

        for i in range(1, NUM_SPLITS + 1):
            current_dist = step_pct * i
            
            if is_long_pac:
                pac_tp = avg_entry * (1.0 + current_dist)
                pac_sl = avg_entry * (1.0 - current_dist)
                lig_tp = avg_entry * (1.0 - current_dist)
                lig_sl = avg_entry * (1.0 + current_dist)
            else:
                pac_tp = avg_entry * (1.0 - current_dist)
                pac_sl = avg_entry * (1.0 + current_dist)
                lig_tp = avg_entry * (1.0 + current_dist)
                lig_sl = avg_entry * (1.0 - current_dist)

            pac_tp = _round_tick(pac_tp, self.pacifica_tick)
            pac_sl = _round_tick(pac_sl, self.pacifica_tick)
            lig_tp = round(lig_tp, 4)
            lig_sl = round(lig_sl, 4)

            try:
                self._pac_client.place_limit(self.symbol_base, pac_exit, qty_fragment, pac_tp)
                self._pac_client.place_stop(self.symbol_base, pac_exit, qty_fragment, pac_sl)
            except Exception as e:
                print(f"⚠️ [Pacifica] Fallo Nivel {i}: {e}")

            try:
                self._lig_client.place_limit(self.symbol_base, lig_exit, qty_fragment, lig_tp)
                self._lig_client.place_stop(self.symbol_base, lig_exit, qty_fragment, lig_sl)
            except Exception as e:
                print(f"⚠️ [Lighter] Fallo Nivel {i}: {e}")

            if VERBOSE:
                print(f"   🧩 Nivel {i}: TP {pac_tp}|{lig_tp} - SL {pac_sl}|{lig_sl}")

        print(f"✅ Grid 5x5 Completado.")

    def _open_once(self, direction: str, pac_bid: float, pac_ask: float, lig_bid: float, lig_ask: float):
        with self._lock:
            if self.open_active: return
            self.open_active = True
        
        self.open_direction = direction
        qty = _round_step(self.trade_base_qty, self.base_qty_step)
        self.open_qty = qty

        if direction.endswith("->Lig"):
            pac_side, lig_side = "BUY", "SELL"
            lig_px = lig_bid
        else:
            pac_side, lig_side = "SELL", "BUY"
            lig_px = lig_ask

        if not self.engine_paper and self._maybe_build_clients():
            t1 = threading.Thread(target=self._pac_client.place_market, kwargs={"symbol": self.symbol_base, "side": pac_side, "base_qty": qty})
            t2 = threading.Thread(target=self._lig_client.place_market, kwargs={"symbol": self.symbol_base, "side": lig_side, "qty_base": qty, "avg_exec_px": lig_px})
            t1.start(); t2.start()
            if FAST_JOIN_SECS > 0: t1.join(FAST_JOIN_SECS); t2.join(FAST_JOIN_SECS)

        self._tg_send_async(f"🚀 <b>OPEN {direction}</b> | Qty: {qty}")
        print(f"🔒 POSICIÓN ABIERTA ({direction}). Iniciando Grid 5x5...")

        threading.Thread(
            target=self._create_grid_tpsl_5x5, 
            args=(direction, pac_ask, pac_bid, lig_ask, lig_bid), 
            daemon=True
        ).start()

        self.pairs_executed_total += 1
        self._last_pair_time_ms = self._now_ms()

    def _close_once(self):
        with self._lock:
            if not self.open_active: return
            self.open_active = False
        
        if not self.engine_paper and self._maybe_build_clients():
            try: self._lig_client.cancel_all_orders(self.symbol_base)
            except: pass
            
            if self.open_direction and self.open_direction.endswith("->Lig"):
                pac_side, lig_side = "SELL", "BUY"
                lig_px = 10000.0
            else:
                pac_side, lig_side = "BUY", "SELL"
                lig_px = 0.0

            qty = self.open_qty or self.trade_base_qty
            t1 = threading.Thread(target=self._pac_client.place_market, kwargs={"symbol": self.symbol_base, "side": pac_side, "base_qty": qty})
            t2 = threading.Thread(target=self._lig_client.place_market, kwargs={"symbol": self.symbol_base, "side": lig_side, "qty_base": qty, "avg_exec_px": lig_px})
            t1.start(); t2.start()

        self._tg_send_async("🏁 <b>CLOSE ALL</b> (Market)")

    def _status_loop(self):
        if not self.status_checker: return
        while not self._stop.is_set():
            try:
                if self.status_checker.is_system_ready(): self._system_ready.set()
                else: self._system_ready.clear()
            except: pass
            time.sleep(STATUS_POLL_SECS)

    def start(self):
        if self._thr and self._thr.is_alive(): return
        self._stop.clear()
        if WARMUP_CLIENTS_ON_START: self._warmup_clients()
        threading.Thread(target=self._status_loop, daemon=True).start()
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()
        print(f"[ENGINE] Running Grid 5x5 Mode...")

    def stop(self): self._stop.set()

    # Pegar esto al final de trader/engine.py, reemplazando la función _loop existente
    def _loop(self):
        self.state = "running"
        base_sleep = max(0.0, LOOP_DELAY_MS / 1000.0)
        lbl = "Pac"

        while not self._stop.is_set():
            try:
                # 1. Chequeo de Estado del Sistema
                if not self._system_ready.is_set():
                    time.sleep(0.1); continue

                # 2. Throttle entre pares
                if (not DISABLE_GAP_THROTTLE) and self._last_pair_time_ms:
                    if (self._now_ms() - self._last_pair_time_ms) < float(GAP_BETWEEN_PAIRS_MS):
                        time.sleep(0.01); continue

                # 3. Obtener Precios
                pac_bid, pac_ask = self._best_bid_ask_safe(self.pac)
                lig_bid, lig_ask = self._best_bid_ask_safe(self.lig)

                if None in (pac_bid, pac_ask, lig_bid, lig_ask):
                    time.sleep(base_sleep); continue

                # 4. Lógica de Apertura (SOLO SI NO HAY POSICIÓN)
                if not self.open_active:
                    e_p2l = self._edge_p2l(pac_ask, lig_bid)
                    e_l2p = self._edge_l2p(lig_ask, pac_bid)

                    if e_p2l and e_p2l >= EDGE_THRESHOLD:
                        self._open_once(f"{lbl}->Lig", pac_bid, pac_ask, lig_bid, lig_ask)
                        if NO_SLEEP_ON_SIGNAL: continue
                    
                    elif e_l2p and e_l2p >= EDGE_THRESHOLD:
                        self._open_once(f"Lig->{lbl}", pac_bid, pac_ask, lig_bid, lig_ask)
                        if NO_SLEEP_ON_SIGNAL: continue
                
                # NOTA: Borramos toda la lógica de "FAIL-SAFE" antigua aquí porque
                # ahora el Grid gestiona los cierres automáticamente.

                time.sleep(base_sleep)

            except Exception:
                traceback.print_exc()
                time.sleep(1.0)
