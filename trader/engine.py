import os
import time
import threading
import importlib
import uuid
import traceback
import sys
from dataclasses import dataclass
from typing import Optional, Tuple, Any, Callable
from pathlib import Path

# ==============================================================================
# 0. IMPORTAR STATUS.PY DESDE LA RAÍZ
# ==============================================================================
try:
    root_dir = Path(__file__).resolve().parent.parent
    sys.path.append(str(root_dir))
    from status import SystemStatus
    print(f"[ENGINE] Módulo 'status.py' importado correctamente desde {root_dir}")
except ImportError:
    print("❌ [CRITICAL] No se encontró 'status.py' en la carpeta raíz.")
    sys.exit(1)

# === Cargar .env ===
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
    if os.getenv("TRADE_BASE_QTY") is None:
        here = Path(__file__).resolve()
        for up in (here.parents[1], here.parents[2], here.parents[3] if len(here.parents) > 3 else None):
            if up and (up / ".env").exists():
                load_dotenv(dotenv_path=(up / ".env"), override=True)
                break
except Exception:
    pass

from feeds.pacifica import PacificaFeed
from feeds.lighter import LighterFeed
from notifications.telegram_notifier import TelegramNotifier
from notifications.cycle_tracker import CyclePnlTracker

# ===== CONFIGURACIÓN GLOBAL ESTATICA =====
SYMBOL_DEFAULT        = os.getenv("SYMBOL", "ETH-USDT")
QUOTE_DEFAULT         = os.getenv("QUOTE", "USDT")
EDGE_THRESHOLD        = float(os.getenv("EDGE_THRESHOLD", "0.0005"))
CLOSE_EDGE_TARGET     = float(os.getenv("CLOSE_EDGE_TARGET", "0.0003"))

# FAIL-SAFE
TPSL_FAILSAFE_PCT     = float(os.getenv("TPSL_SLIP", "0.001"))

VERBOSE               = True

LOOP_DELAY_MS         = int(os.getenv("ENGINE_TICK_MS", "50"))
GAP_BETWEEN_PAIRS_MS  = int(os.getenv("MIN_MS_BETWEEN_PAIRS", "800"))
DUAL_TIMEOUT_MS       = int(os.getenv("DUAL_TIMEOUT_MS", "800"))
ENGINE_ONE_SHOT       = os.getenv("ENGINE_ONE_SHOT", "false").lower() == "true"
ENGINE_MAX_PAIRS_ENV  = os.getenv("ENGINE_MAX_PAIRS", "").strip()
ENGINE_PAPER          = os.getenv("ENGINE_PAPER", "false").lower() == "true"
ENGINE_ALLOW_REAL     = os.getenv("ENGINE_ALLOW_REAL", "true").lower() == "true"

# Low Latency toggles
USE_PERF_COUNTER_LOOP   = os.getenv("ENGINE_USE_PERF_COUNTER_LOOP", "true").lower() == "true"
NO_SLEEP_ON_SIGNAL      = os.getenv("ENGINE_NO_SLEEP_ON_SIGNAL", "true").lower() == "true"
DISABLE_GAP_THROTTLE    = os.getenv("ENGINE_DISABLE_GAP_THROTTLE", "true").lower() == "true"
FAST_JOIN_SECS          = float(os.getenv("ENGINE_FAST_JOIN_SECS", "0.03"))
WARMUP_CLIENTS_ON_START = os.getenv("ENGINE_WARMUP_CLIENTS_ON_START", "true").lower() == "true"

# Status polling (saca el costo del status.py del hot loop)
STATUS_POLL_SECS        = float(os.getenv("ENGINE_STATUS_POLL_SECS", "10"))

def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    n = round(x / step)
    return round(n * step, 10)

def _invert_side(side: str) -> str:
    return "SELL" if (side or "").upper() == "BUY" else "BUY"

def _parse_symbol(sym: str, quote_fallback: str = "USDT") -> Tuple[str, str]:
    s = (sym or "").upper().replace("/", "-").strip()
    if "-" in s:
        b, q = s.split("-", 1)
        return (b.strip() or "ETH"), (q.strip() or quote_fallback.upper())
    return s or "ETH", quote_fallback.upper()

def _round_tick(px: float, tick: float) -> float:
    t = float(tick or 0.0)
    if t <= 0:
        return float(px)
    return round(round(float(px) / t) * t, 12)

def _resolve_client_ctor(module_name: str, candidates: Tuple[str, ...]) -> Optional[Callable[[], Any]]:
    try:
        mod = importlib.import_module(module_name)
    except Exception:
        return None
    for cname in candidates:
        ctor = getattr(mod, cname, None)
        if callable(ctor):
            return ctor
    return None

def _best_from_book(feed) -> Tuple[Optional[float], Optional[float]]:
    try:
        bb = max((p for p, _ in getattr(feed.book, "bids", [])), default=None)
        ba = min((p for p, _ in getattr(feed.book, "asks", [])), default=None)
        return bb, ba
    except Exception:
        return None, None

@dataclass
class OrderFill:
    ok: bool
    msg: str
    order_id: Optional[str] = None
    filled_qty: Optional[float] = None
    avg_px: Optional[float] = None
    raw: Optional[Any] = None

class ArbEngine:
    """
    MISMA LÓGICA / MISMAS FUNCIONES,
    pero hot-path más rápido:
      - status_checker afuera del loop (thread + Event)
      - apertura/cierre "ATAQUE" sin bloquear (join corto FAST_JOIN_SECS)
      - warmup de clientes opcional al start
      - loop con perf_counter y micro-sleep (o sin sleep en señal si NO_SLEEP_ON_SIGNAL)
    """

    def __init__(self, pac: Any, lig: LighterFeed, symbol: str = SYMBOL_DEFAULT, quote: str = QUOTE_DEFAULT, mode: str = "PACIFICA"):
        self.engine_paper       = ENGINE_PAPER
        self.engine_allow_real  = ENGINE_ALLOW_REAL
        self.engine_one_shot    = ENGINE_ONE_SHOT

        self.trade_base_qty = float(os.getenv("TRADE_BASE_QTY", "0.0028"))
        self.base_qty_step  = float(os.getenv("BASE_QTY_STEP", "0.0001"))

        self.enable_tpsl                        = True
        self.tpsl_pct                           = float(os.getenv("ENGINE_TPSL_PCT", "0.005"))
        self.tpsl_limit_offset_pct              = float(os.getenv("ENGINE_TPSL_LIMIT_OFFSET_PCT", "0.0005"))

        # >>> RESOLUCIÓN DE PRECIO DE PACIFICA (2 DECIMALES) <<<
        self.pacifica_tick = 0.01

        self.tpsl_retry_n        = int(os.getenv("ENGINE_TPSL_RETRY_N", "5"))
        self.tpsl_retry_delay_ms = int(os.getenv("ENGINE_TPSL_RETRY_DELAY_MS", "250"))

        self.mode_target = mode.upper()

        print(f"[ENGINE] cfg qty={self.trade_base_qty} mode={self.mode_target} enable_tpsl=SIEMPRE_ACTIVO (Espejo Mode) 🔄")
        print(f"[ENGINE] Fail-Safe Slip: {TPSL_FAILSAFE_PCT*100:.2f}% | PacTick: {self.pacifica_tick}")

        self.pac = pac
        self.lig = lig
        base, q = _parse_symbol(symbol or SYMBOL_DEFAULT, quote_fallback=quote or QUOTE_DEFAULT)
        self.symbol_base = base
        self.quote = q

        self.state: str = "idle"
        self._stop = threading.Event()
        self._thr: Optional[threading.Thread] = None

        try:
            env_val = int(ENGINE_MAX_PAIRS_ENV) if ENGINE_MAX_PAIRS_ENV else None
        except Exception:
            env_val = None
        _limit = 1 if self.engine_one_shot else env_val
        self.max_pairs = None if (_limit is None or _limit <= 0) else int(_limit)
        self.pairs_executed_total = 0
        self._last_pair_time_ms = 0.0

        # === SELECCIÓN DINÁMICA DE CLIENTE ===
        if self.mode_target == "BINANCE":
            self._pac_ctor = _resolve_client_ctor("exchanges.binance_adapter", ("BinanceAdapter",))
        else:
            self._pac_ctor = _resolve_client_ctor("exchanges.pacifica_api", ("PacificaAPI", "PacificaClient"))

        self._lig_ctor = _resolve_client_ctor("exchanges.lighter_api", ("LighterAPI", "LighterClient"))
        self._pac_client = None
        self._lig_client = None

        # status.py -> thread + Event (para no pagar el costo en el loop)
        try:
            self.status_checker = SystemStatus()
        except Exception as e:
            print(f"⚠️ Error al iniciar SystemStatus: {e}")
            self.status_checker = None

        self._system_ready = threading.Event()
        self._system_ready.set()  # si no hay checker, queda siempre listo

        self.open_active: bool = False
        self.open_direction: Optional[str] = None
        self.open_qty: Optional[float] = None
        self.pac_open_side: Optional[str] = None
        self.lig_open_side_sent: Optional[str] = None

        self._master_tp: Optional[float] = None
        self._master_sl: Optional[float] = None
        self.direction: Optional[str] = None

        self._open_in_flight = False
        self._close_in_flight = False
        self._close_sent_once = False

        self._notify_enabled = os.getenv("TELEGRAM_NOTIFICATIONS", "true").lower() == "true"
        self._tg = TelegramNotifier()
        self._pnl_tracker = CyclePnlTracker()

        # lock corto solo para flags críticos
        self._state_lock = threading.Lock()

    @property
    def mode(self) -> str:
        return "CLOSE" if self.open_active else "OPEN"

    def _base_qty(self) -> float:
        return _round_step(self.trade_base_qty, self.base_qty_step)

    def _now_ms(self) -> float:
        if USE_PERF_COUNTER_LOOP:
            return time.perf_counter() * 1000.0
        return time.time() * 1000.0

    def _best_bid_ask_safe(self, feed) -> Tuple[Optional[float], Optional[float]]:
        bb = getattr(feed, "best_bid", None)
        ba = getattr(feed, "best_ask", None)
        if bb is not None and ba is not None:
            return bb, ba
        return _best_from_book(feed)

    @staticmethod
    def _edge_p2l(pac_ask: float, lig_bid: float) -> Optional[float]:
        if pac_ask and lig_bid:
            return (lig_bid - pac_ask) / pac_ask
        return None

    @staticmethod
    def _edge_l2p(lig_ask: float, pac_bid: float) -> Optional[float]:
        if lig_ask and pac_bid:
            return (pac_bid - lig_ask) / lig_ask
        return None

    def _maybe_build_clients(self) -> bool:
        if self.engine_paper:
            return True
        if not self.engine_allow_real:
            self.state = "real-disabled"
            return False
        if self._pac_client and self._lig_client:
            return True
        if not self._pac_ctor or not self._lig_ctor:
            return False
        try:
            self._pac_client = self._pac_ctor()
            self._lig_client = self._lig_ctor()
            self.state = "real-ready"
            return True
        except Exception as e:
            self.state = f"real-clients-error:{e!r}"
            return False

    def _warmup_clients(self):
        if self.engine_paper:
            return
        try:
            ok = self._maybe_build_clients()
            if VERBOSE:
                print(f"[ENGINE] warmup clients: {'OK' if ok else 'FAIL'}")
        except Exception as e:
            if VERBOSE:
                print(f"[ENGINE] warmup clients error: {e!r}")

    def _should_notify(self) -> bool:
        if self.engine_paper:
            return False
        if not self._notify_enabled:
            return False
        try:
            return self._tg.enabled()
        except Exception:
            return False

    def _tg_send_async(self, text: str):
        def _job():
            try:
                self._tg.send(text)
            except Exception:
                pass
        threading.Thread(target=_job, daemon=True).start()

    def _send_pac_market(self, side: str, qty: float) -> OrderFill:
        if self.engine_paper:
            return OrderFill(True, "paper", filled_qty=qty, avg_px=None)
        if not self._maybe_build_clients():
            return OrderFill(False, "no-clients")
        try:
            _ = self._pac_client.place_market(symbol=self.symbol_base, side=side, base_qty=qty)
            return OrderFill(True, "ok", filled_qty=qty)
        except Exception as e:
            return OrderFill(False, f"{self.mode_target.lower()} exception: {e!r}")

    def _send_lig_market(self, side: str, qty: float, px_ref: float) -> OrderFill:
        if self.engine_paper:
            return OrderFill(True, "paper", filled_qty=qty, avg_px=None)
        if not self._maybe_build_clients():
            return OrderFill(False, "no-clients")
        try:
            res = self._lig_client.place_market(symbol=self.symbol_base, side=side, qty_base=qty, avg_exec_px=px_ref)
            return OrderFill(res.get("accepted", False), "ok", filled_qty=qty, raw=res)
        except Exception as e:
            return OrderFill(False, f"lighter exception: {e!r}")

    # =========================
    # TP/SL SETTERS
    # =========================
    def _set_pac_tpsl_after_open(self, direction: str, pac_side: str, pac_bid: float, pac_ask: float, lig_bid: float, lig_ask: float) -> None:
        if VERBOSE:
            print(f"[DEBUG] >>> HILO TP/SL {self.mode_target} INICIADO. Esperando 1.5s...")
        if self.engine_paper or (not self.enable_tpsl):
            return
        if not self._maybe_build_clients():
            return
        time.sleep(1.5)

        pac_entry_theo = pac_ask if (pac_side or "").upper() == "BUY" else pac_bid
        
        # Detectar dirección de forma genérica
        is_target_long = direction.endswith("->Lig") 
        lig_entry_theo = lig_bid if is_target_long else lig_ask
        
        ref = (float(pac_entry_theo) + float(lig_entry_theo)) / 2.0

        pct = self.tpsl_pct
        is_long = (pac_side or "").upper() == "BUY"
        pos_side = "ask" if is_long else "bid"

        if is_long:
            tp_raw, sl_raw = ref * (1.0 + pct), ref * (1.0 - pct)
        else:
            tp_raw, sl_raw = ref * (1.0 - pct), ref * (1.0 + pct)

        tick = self.pacifica_tick
        master_tp = _round_tick(tp_raw, tick)
        master_sl = _round_tick(sl_raw, tick)
        self._master_tp, self._master_sl = master_tp, master_sl

        try:
            self._pac_client.set_position_tpsl(
                symbol=self.symbol_base, position_side=pos_side,
                tp_stop=master_tp, sl_stop=master_sl,
                tp_limit=master_tp, sl_limit=None,
                tp_client_order_id=str(uuid.uuid4()), sl_client_order_id=str(uuid.uuid4()),
            )
            if VERBOSE:
                print(f"[ENGINE] {self.mode_target} TP/SL OK ✅ TP={master_tp} SL={master_sl}")
        except Exception as e:
            if VERBOSE:
                print(f"[ENGINE ERROR] {self.mode_target} TPSL: {e}")

    def _set_lig_tpsl_after_open(self, direction: str, lig_side: str, pac_bid: float, pac_ask: float, lig_bid: float, lig_ask: float) -> None:
        if VERBOSE:
            print(f"[DEBUG] >>> SINCRONIZANDO PROTECCIÓN ESPEJO LIGHTER. Esperando 2.5s...")
        if self.engine_paper or (not self.enable_tpsl):
            return
        if not self._maybe_build_clients():
            return
        time.sleep(2.5)
        if self._master_tp is None or self._master_sl is None:
            return

        lig_tp_trigger, lig_sl_trigger = self._master_sl, self._master_tp
        try:
            self._lig_client.set_tpsl_grouped(symbol=self.symbol_base, side=lig_side, tp_trigger=lig_tp_trigger, sl_trigger=lig_sl_trigger)
            if VERBOSE:
                print(f"[ENGINE] Lighter OCO Grouped OK ✅")
        except Exception as e:
            if VERBOSE:
                print(f"[ENGINE ERROR] Lighter TPSL Error: {e}")
            try:
                self._lig_client.create_sl_order(self.symbol_base, _invert_side(lig_side), 0, lig_sl_trigger)
            except Exception:
                pass

    # =========================================================================
    # 🔔 NOTIFICACIONES DETALLADAS (Helper)
    # =========================================================================
    def _notify_execution(self, dex_name: str, action: str, func, *args) -> OrderFill:
        fill = func(*args)
        if self._should_notify():
            icon = "✅" if fill.ok else "⚠️"
            qty_str = f"{fill.filled_qty:.4f}" if fill.filled_qty else "?"
            msg = f"{icon} <b>{dex_name} {action}</b>: {fill.msg} ({qty_str})"
            self._tg_send_async(msg)
        return fill

    # =========================================================================
    # HOT PATH: APERTURA / CIERRE (optimizados)
    # =========================================================================
    def _open_once(self, direction: str, pac_bid: float, pac_ask: float, lig_bid: float, lig_ask: float):
        self.direction = direction

        with self._state_lock:
            if self._open_in_flight or self.open_active:
                return
            self._open_in_flight = True
            self._master_tp, self._master_sl = None, None

        qty = self._base_qty()
        self.open_qty, self.open_direction = qty, direction

        # Lógica genérica: Si direction termina en "->Lig" es compra en target
        is_target_long = direction.endswith("->Lig")

        pac_side, lig_side = ("BUY", "SELL") if is_target_long else ("SELL", "BUY")
        lig_px_ref = lig_bid if is_target_long else lig_ask
        pac_px_ref = pac_ask if is_target_long else pac_bid

        self.pac_open_side, self.lig_open_side_sent = pac_side, lig_side

        # 1) aviso oportunidad (NO bloqueante)
        if self._should_notify():
            try:
                spread_pct = abs(pac_px_ref - lig_px_ref) / pac_px_ref * 100
            except Exception:
                spread_pct = 0.0
            self._tg_send_async(
                f"🚀 <b>OPORTUNIDAD ({self.mode_target}): {direction}</b>\n"
                f"Spread: {spread_pct:.3f}%\n"
                f"Qty: {qty} | Ejecutando..."
            )

        # 2) ATAQUE: enviar ambas órdenes YA (sin bloquear el loop)
        t1 = threading.Thread(target=self._send_pac_market, args=(pac_side, qty), daemon=True)
        t2 = threading.Thread(target=self._send_lig_market, args=(lig_side, qty, lig_px_ref), daemon=True)
        t1.start(); t2.start()

        # Join corto (opcional) para “darle aire” a la red sin perder velocidad
        if FAST_JOIN_SECS > 0:
            t1.join(FAST_JOIN_SECS)
            t2.join(FAST_JOIN_SECS)

        # marcamos abierto inmediatamente (igual que tu intención: velocidad > confirmación)
        with self._state_lock:
            self.open_active = True
            self._open_in_flight = False
            self._close_sent_once = False  # habilita futuros cierres

        # 3) confirmación apertura (async)
        if self._should_notify():
            self._tg_send_async("🟢 <b>POSICIÓN ABIERTA</b>\nEsperando TP/SL o Cierre por Spread...")

        # 4) TP/SL en background (lento)
        threading.Thread(
            target=self._set_pac_tpsl_after_open,
            args=(direction, pac_side, pac_bid, pac_ask, lig_bid, lig_ask),
            daemon=True
        ).start()

        threading.Thread(
            target=self._set_lig_tpsl_after_open,
            args=(direction, lig_side, pac_bid, pac_ask, lig_bid, lig_ask),
            daemon=True
        ).start()

        # contabilización de pares (respeta el engine original)
        self.pairs_executed_total += 1
        self._last_pair_time_ms = self._now_ms()

    def _close_once(self, pac_bid: float, pac_ask: float, lig_bid: float, lig_ask: float):
        with self._state_lock:
            if self._close_in_flight or self._close_sent_once:
                return
            if not self.open_active:
                return
            self._close_in_flight, self._close_sent_once = True, True

        qty = self.open_qty or self._base_qty()

        pac_close, lig_close = _invert_side(self.pac_open_side), _invert_side(self.lig_open_side_sent)
        lig_px_ref = lig_ask if lig_close == "BUY" else lig_bid

        # 1) aviso cierre (no bloqueante)
        if self._should_notify():
            self._tg_send_async("🔻 <b>CERRANDO POSICIÓN...</b>")

        # 2) ATAQUE: enviar cierres en paralelo (threads daemon)
        t1 = threading.Thread(
            target=self._notify_execution,
            args=(self.mode_target, "CLOSE", self._send_pac_market, pac_close, qty),
            daemon=True
        )
        t2 = threading.Thread(
            target=self._notify_execution,
            args=("Lighter", "CLOSE", self._send_lig_market, lig_close, qty, lig_px_ref),
            daemon=True
        )
        t1.start(); t2.start()

        if FAST_JOIN_SECS > 0:
            t1.join(FAST_JOIN_SECS)
            t2.join(FAST_JOIN_SECS)

        # cancel-all en background (lento)
        def _cancel_job():
            try:
                if not self.engine_paper and self._maybe_build_clients() and self._lig_client:
                    self._lig_client.cancel_all_orders(self.symbol_base)
            except Exception:
                pass

        threading.Thread(target=_cancel_job, daemon=True).start()

        with self._state_lock:
            self.open_active = False
            self._close_in_flight = False

        if self._should_notify():
            self._tg_send_async("🏁 <b>CICLO COMPLETADO</b>\nSistema Flat. Buscando nueva oportunidad...")

    # =========================================================================
    # STATUS THREAD (saca status.py del hot loop)
    # =========================================================================
    def _status_loop(self):
        # si no hay checker, el engine queda siempre listo
        if not self.status_checker:
            self._system_ready.set()
            return
        while not self._stop.is_set():
            try:
                is_ready = bool(self.status_checker.is_system_ready())
                if is_ready:
                    self._system_ready.set()
                else:
                    self._system_ready.clear()
                    if VERBOSE:
                        print("[STATUS] Sistema ocupado (False). Bloqueando hot loop hasta estar Clean...")
            except Exception:
                # en caso de error, por seguridad bloqueamos
                self._system_ready.clear()
            time.sleep(max(0.5, STATUS_POLL_SECS))

    # =========================================================================
    # LOOP
    # =========================================================================
    def start(self):
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()

        if WARMUP_CLIENTS_ON_START:
            self._warmup_clients()

        threading.Thread(target=self._status_loop, daemon=True).start()

        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()
        if VERBOSE:
            print(f"[ENGINE] running (low-latency hot path) | MODE: {self.mode_target}")

    def stop(self):
        self._stop.set()

    def _loop(self):
        self.state = "running"
        base_sleep = max(0.0, LOOP_DELAY_MS / 1000.0)

        # Label para identificar target (Pac o Bin)
        lbl = "Bin" if self.mode_target == "BINANCE" else "Pac"

        while not self._stop.is_set():
            try:
                # Si status.py dice FALSE (sucio/ocupado), no operamos
                if not self._system_ready.is_set():
                    time.sleep(0.1)
                    continue

                # throttle entre pares (si no está deshabilitado)
                if (not DISABLE_GAP_THROTTLE) and self._last_pair_time_ms:
                    if (self._now_ms() - self._last_pair_time_ms) < float(GAP_BETWEEN_PAIRS_MS):
                        time.sleep(min(base_sleep, 0.01))
                        continue

                pac_bid, pac_ask = self._best_bid_ask_safe(self.pac)
                lig_bid, lig_ask = self._best_bid_ask_safe(self.lig)

                if None in (pac_bid, pac_ask, lig_bid, lig_ask):
                    time.sleep(base_sleep)
                    continue

                # =========================
                # APERTURA
                # =========================
                if (not self.open_active) and (not self._open_in_flight) and (self.pairs_executed_total < (self.max_pairs or 999999)):
                    e_p2l = self._edge_p2l(pac_ask, lig_bid)
                    e_l2p = self._edge_l2p(lig_ask, pac_bid)

                    if e_p2l is not None and e_p2l >= EDGE_THRESHOLD:
                        self._open_once(f"{lbl}->Lig", pac_bid, pac_ask, lig_bid, lig_ask)
                        if NO_SLEEP_ON_SIGNAL:
                            continue
                    elif e_l2p is not None and e_l2p >= EDGE_THRESHOLD:
                        self._open_once(f"Lig->{lbl}", pac_bid, pac_ask, lig_bid, lig_ask)
                        if NO_SLEEP_ON_SIGNAL:
                            continue

                # =========================
                # CIERRE + FAIL-SAFE (misma lógica)
                # =========================
                elif self.open_active and self.open_direction:
                    # FAIL-SAFE
                    if self._master_tp and self._master_sl:
                        force_close = False
                        reason = ""
                        safety_margin = TPSL_FAILSAFE_PCT

                        if self.pac_open_side == "BUY":  # Long en Target
                            if pac_bid > self._master_tp * (1.0 + safety_margin):
                                force_close = True
                                reason = f"FAIL-SAFE TP (Long): {pac_bid} > {self._master_tp}"
                            elif pac_bid < self._master_sl * (1.0 - safety_margin):
                                force_close = True
                                reason = f"FAIL-SAFE SL (Long): {pac_bid} < {self._master_sl}"
                        else:  # Short en Target
                            if pac_ask < self._master_tp * (1.0 - safety_margin):
                                force_close = True
                                reason = f"FAIL-SAFE TP (Short): {pac_ask} < {self._master_tp}"
                            elif pac_ask > self._master_sl * (1.0 + safety_margin):
                                force_close = True
                                reason = f"FAIL-SAFE SL (Short): {pac_ask} > {self._master_sl}"

                        if force_close:
                            print(f"🚨 [EMERGENCIA] {reason} -> EJECUTANDO CIERRE DE MERCADO")
                            if self._should_notify():
                                self._tg_send_async(f"🚨 <b>FAIL-SAFE ACTIVADO</b>\n{reason}")
                            self._close_once(pac_bid, pac_ask, lig_bid, lig_ask)
                            time.sleep(0.2)
                            continue

                    # Cierre por spread (fallback)
                    e_p2l = self._edge_p2l(pac_ask, lig_bid)
                    e_l2p = self._edge_l2p(lig_ask, pac_bid)

                    close_cond = False
                    
                    # Chequeo dinámico de dirección
                    is_target_long = self.open_direction.endswith("->Lig")
                    
                    if is_target_long: # Target->Lig
                        close_cond = (e_l2p is not None and e_l2p >= CLOSE_EDGE_TARGET)
                    else: # Lig->Target
                        close_cond = (e_p2l is not None and e_p2l >= CLOSE_EDGE_TARGET)

                    if close_cond:
                        self._close_once(pac_bid, pac_ask, lig_bid, lig_ask)
                        if NO_SLEEP_ON_SIGNAL:
                            continue

                # Sleep normal
                time.sleep(base_sleep)

            except Exception:
                if VERBOSE:
                    traceback.print_exc()
                time.sleep(0)

        self.state = "stopped"
