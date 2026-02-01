# exchanges/pacifica_api.py
from __future__ import annotations

import os
import time
import json
import requests
from typing import Dict, Any, Optional, List

import base58
from solders.keypair import Keypair  # ED25519
from requests.adapters import HTTPAdapter

# =========================
# Globals (fast + safe)
# =========================
_BASE = os.getenv("PACIFICA_REST_URL", "https://api.pacifica.fi/api/v1").strip()
_ORDER_PATH = os.getenv("PACIFICA_ORDER_PATH", "/orders/create_market").strip()

_JSON_SEPARATORS = (",", ":")


def _sort_json(value):
    # Mantengo sorting porque suele formar parte del esquema de firma
    if isinstance(value, dict):
        return {k: _sort_json(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_sort_json(x) for x in value]
    return value


def _mk_session() -> requests.Session:
    """
    Sesión HTTP optimizada para latencia:
      - keep-alive + connection pooling
      - 0 retries
      - trust_env=False (evita proxies env y lookups extra)
    """
    s = requests.Session()
    s.trust_env = False
    s.headers.update(
        {
            "Content-Type": "application/json",
            "Connection": "keep-alive",
            "Accept": "application/json",
        }
    )

    adapter = HTTPAdapter(
        pool_connections=int(os.getenv("PACIFICA_POOL_CONNECTIONS", "64")),
        pool_maxsize=int(os.getenv("PACIFICA_POOL_MAXSIZE", "64")),
        max_retries=0,
        pool_block=False,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_SESSION = _mk_session()


class PacificaClient:
    """
    Cliente Pacifica optimizado para baja latencia:
      - keep-alive + pooling
      - JSON compacto en bytes
      - time_ns ms
      - headers/URL precomputados
      - Métodos de lectura GET para status.py
    """

    def __init__(self) -> None:
        # Normalización de URL base (eliminar slash final)
        base = (_BASE or "https://api.pacifica.fi/api/v1").rstrip("/")
        
        # Rutas de escritura
        order_path = (_ORDER_PATH or "/orders/create_market").strip()
        tpsl_path = (os.getenv("PACIFICA_TPSL_PATH", "/positions/tpsl") or "/positions/tpsl").strip()

        self.base = base
        self.order_path = order_path
        self.tpsl_path = tpsl_path
        
        # URLs precomputadas para escritura
        self._url_order = f"{base}{order_path}"
        self._url_tpsl = f"{base}{tpsl_path}"

        # URLs precomputadas para lectura (status.py)
        self._url_get_orders = f"{base}/orders"
        self._url_get_positions = f"{base}/positions"
        self._url_get_trades = f"{base}/trades"

        # Credenciales
        self.account = (os.getenv("PACIFICA_ACCOUNT", "") or "").strip()
        self.agent_wallet = (os.getenv("PACIFICA_AGENT_WALLET", "") or "").strip()
        self.agent_pk_b58 = (os.getenv("PACIFICA_AGENT_PRIVATE_KEY", "") or "").strip()
        self.expiry_ms = int(os.getenv("PACIFICA_EXPIRY_MS", "30000"))

        # Operativos
        self.slippage_percent = (os.getenv("PACIFICA_SLIPPAGE_PERCENT", "0.1") or "0.1").strip()
        self.reduce_only = (os.getenv("PACIFICA_REDUCE_ONLY", "false") or "false").lower() == "true"

        # Timeouts
        self._timeout_connect = float(os.getenv("PACIFICA_TIMEOUT_CONNECT", "1.5"))
        self._timeout_read = float(os.getenv("PACIFICA_TIMEOUT_READ", "8.0"))
        self._timeout = (self._timeout_connect, self._timeout_read)

        # Headers fijos
        self._headers_market = {"Content-Type": "application/json", "type": "create_market_order"}
        self._headers_tpsl = {"Content-Type": "application/json", "type": "set_position_tpsl"}

        # Validación de entorno
        missing = []
        if not self.account: missing.append("PACIFICA_ACCOUNT")
        if not self.agent_wallet: missing.append("PACIFICA_AGENT_WALLET")
        if not self.agent_pk_b58: missing.append("PACIFICA_AGENT_PRIVATE_KEY (Base58)")
        if missing:
            raise RuntimeError(f"Pacifica: faltan variables en .env: {', '.join(missing)}")

        # Keypair ED25519 (decodificar 1 vez)
        try:
            self._agent_kp = Keypair.from_bytes(base58.b58decode(self.agent_pk_b58))
        except Exception as e:
            raise RuntimeError(f"Pacifica: AGENT_PRIVATE_KEY inválida (Base58): {e}")

        self._session = _SESSION

    # =========================================================================
    # 🔐 FIRMA Y ENVÍO (MÉTODOS PRIVADOS)
    # =========================================================================
    def _build_signature(self, op_type: str, op_data: Dict[str, Any]) -> Dict[str, Any]:
        timestamp = time.time_ns() // 1_000_000

        data_to_sign = {
            "timestamp": int(timestamp),
            "expiry_window": int(self.expiry_ms),
            "type": op_type,
            "data": op_data,
        }

        compact_bytes = json.dumps(_sort_json(data_to_sign), separators=_JSON_SEPARATORS).encode("utf-8")
        sig = self._agent_kp.sign_message(compact_bytes)
        signature_b58 = base58.b58encode(bytes(sig)).decode("ascii")

        return {
            "account": self.account,
            "agent_wallet": self.agent_wallet,
            "signature": signature_b58,
            "timestamp": int(timestamp),
            "expiry_window": int(self.expiry_ms),
            "**op_data": op_data, # Nota: asegurate que la API espera op_data aplanado o anidado. Tu código original lo aplanaba con **op_data
            **op_data
        }

    # =========================================================================
    # 📖 MÉTODOS DE LECTURA (GET) - NECESARIOS PARA STATUS.PY
    # =========================================================================

    def get_open_orders(self) -> List[Dict]:
        """GET /api/v1/orders?account=..."""
        try:
            params = {"account": self.account}
            r = self._session.get(self._url_get_orders, params=params, timeout=self._timeout)
            
            if r.status_code != 200:
                # Si falla, retornamos lista vacía o raise dependiendo de la severidad.
                # Para status.py, lanzar excepción es mejor para detectar "Sucio/Error"
                r.raise_for_status()
                
            data = r.json()
            if data.get("success"):
                return data.get("data", [])
            return []
        except Exception as e:
            print(f"[Pacifica] get_open_orders error: {e}")
            raise e

    def get_positions(self) -> List[Dict]:
        """GET /api/v1/positions?account=..."""
        try:
            params = {"account": self.account}
            r = self._session.get(self._url_get_positions, params=params, timeout=self._timeout)
            
            if r.status_code != 200:
                print(f"[Pacifica] HTTP Error {r.status_code}: {r.text}")
                r.raise_for_status()

            data = r.json()
            if data.get("success"):
                return data.get("data", [])
            return []
        except Exception as e:
            print(f"[Pacifica] get_positions error: {e}")
            raise e

    def get_recent_trades(self, symbol: str) -> List[Dict]:
        """GET /api/v1/trades?symbol=..."""
        try:
            params = {"symbol": symbol.upper(), "account": self.account}
            r = self._session.get(self._url_get_trades, params=params, timeout=self._timeout)
            
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    return data.get("data", [])
            return []
        except Exception:
            return []

    # =========================================================================
    # ⚡ MÉTODOS DE ESCRITURA (POST / SIGNED)
    # =========================================================================

    def place_market(self, *, symbol: str, side: str, base_qty: float) -> Dict[str, Any]:
        """Ejecuta orden de mercado firmada."""
        if base_qty is None or base_qty <= 0:
            raise ValueError("Pacifica.place_market: base_qty inválida")

        s_up = side.upper()
        side_str = "bid" if s_up in ("BUY", "BID", "LONG") else "ask"
        # Formato string sin notación científica
        amount_str = f"{base_qty:.8f}".rstrip("0").rstrip(".")

        op_type = "create_market_order"
        op_data = {
            "symbol": symbol,
            "amount": amount_str,
            "side": side_str,
            "slippage_percent": self.slippage_percent,
            "reduce_only": self.reduce_only,
        }

        body = self._build_signature(op_type, op_data)
        payload = json.dumps(body, separators=_JSON_SEPARATORS).encode("utf-8")

        r = self._session.post(
            self._url_order,
            data=payload,
            headers=self._headers_market,
            timeout=self._timeout,
            allow_redirects=False,
        )

        if not (200 <= r.status_code < 300):
            raise requests.HTTPError(f"create_market failed: status={r.status_code} resp={r.text[:500]}")

        return r.json()

    def set_position_tpsl(
        self,
        *,
        symbol: str,
        position_side: str,  # "bid" o "ask"
        tp_stop: float,
        sl_stop: float,
        tp_limit: Optional[float] = None,
        sl_limit: Optional[float] = None,
        tp_client_order_id: Optional[str] = None,
        sl_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Configura TP/SL firmado."""
        if not symbol:
            raise ValueError("Pacifica.set_position_tpsl: symbol requerido")

        side = (position_side or "").lower().strip()
        if side not in ("bid", "ask"):
            # Mapeo de seguridad por si envían "buy"/"sell"
            if side in ("buy", "long"): side = "bid"
            elif side in ("sell", "short"): side = "ask"
            else: raise ValueError(f"Pacifica: position_side desconocido '{position_side}'")

        op_type = "set_position_tpsl"

        take_profit = {"stop_price": f"{float(tp_stop):.8f}".rstrip("0").rstrip(".")}
        stop_loss = {"stop_price": f"{float(sl_stop):.8f}".rstrip("0").rstrip(".")}

        if tp_limit is not None:
            take_profit["limit_price"] = f"{float(tp_limit):.8f}".rstrip("0").rstrip(".")
        if sl_limit is not None:
            stop_loss["limit_price"] = f"{float(sl_limit):.8f}".rstrip("0").rstrip(".")

        if tp_client_order_id:
            take_profit["client_order_id"] = tp_client_order_id
        if sl_client_order_id:
            stop_loss["client_order_id"] = sl_client_order_id

        op_data = {
            "symbol": symbol,
            "side": side,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
        }

        body = self._build_signature(op_type, op_data)
        payload = json.dumps(body, separators=_JSON_SEPARATORS).encode("utf-8")

        r = self._session.post(
            self._url_tpsl,
            data=payload,
            headers=self._headers_tpsl,
            timeout=self._timeout,
            allow_redirects=False,
        )

        if not (200 <= r.status_code < 300):
            raise requests.HTTPError(f"set_position_tpsl failed: status={r.status_code} resp={r.text[:500]}")

        return r.json()


# Alias para compatibilidad
PacificaAPI = PacificaClient