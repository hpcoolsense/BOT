from __future__ import annotations

import os
import time
import json
import uuid
import requests
from typing import Dict, Any, Optional, List

import base58
from solders.keypair import Keypair
from requests.adapters import HTTPAdapter

_BASE = os.getenv("PACIFICA_REST_URL", "https://api.pacifica.fi/api/v1").strip()
_ORDER_CREATE_PATH = "/orders/create" 
_JSON_SEPARATORS = (",", ":")

def _sort_json(value):
    if isinstance(value, dict):
        return {k: _sort_json(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_sort_json(x) for x in value]
    return value

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.headers.update({"Content-Type": "application/json", "Connection": "keep-alive"})
    adapter = HTTPAdapter(max_retries=0)
    s.mount("https://", adapter)
    return s

_SESSION = _mk_session()

class PacificaClient:
    def __init__(self) -> None:
        self.base = (_BASE or "https://api.pacifica.fi/api/v1").rstrip("/")
        self._url_order_market = f"{self.base}/orders/create_market"
        self._url_order_create = f"{self.base}{_ORDER_CREATE_PATH}"
        self.account = (os.getenv("PACIFICA_ACCOUNT", "")).strip()
        self.agent_wallet = (os.getenv("PACIFICA_AGENT_WALLET", "")).strip()
        self.agent_pk_b58 = (os.getenv("PACIFICA_AGENT_PRIVATE_KEY", "")).strip()
        self.expiry_ms = int(os.getenv("PACIFICA_EXPIRY_MS", "30000"))
        self.slippage_percent = (os.getenv("PACIFICA_SLIPPAGE_PERCENT", "0.1")).strip()
        self.reduce_only = (os.getenv("PACIFICA_REDUCE_ONLY", "false")).lower() == "true"

        self._timeout = (1.5, 8.0)
        self._headers_market = {"Content-Type": "application/json", "type": "create_market_order"}
        self._headers_create = {"Content-Type": "application/json", "type": "create_order"}

        if not self.account: raise RuntimeError("Pacifica: Faltan credenciales")
        try:
            self._agent_kp = Keypair.from_bytes(base58.b58decode(self.agent_pk_b58))
        except Exception as e:
            raise RuntimeError(f"Pacifica KEY invalida: {e}")
        self._session = _SESSION

    def _build_signature(self, op_type: str, op_data: Dict[str, Any]) -> Dict[str, Any]:
        ts = int(time.time_ns() // 1_000_000)
        data_to_sign = {
            "timestamp": ts, "expiry_window": int(self.expiry_ms),
            "type": op_type, "data": op_data,
        }
        compact_bytes = json.dumps(_sort_json(data_to_sign), separators=_JSON_SEPARATORS).encode("utf-8")
        sig = self._agent_kp.sign_message(compact_bytes)
        return {
            "account": self.account, "agent_wallet": self.agent_wallet,
            "signature": base58.b58encode(bytes(sig)).decode("ascii"),
            "timestamp": ts, "expiry_window": int(self.expiry_ms),
            **op_data
        }

    def place_market(self, *, symbol: str, side: str, base_qty: float) -> Dict[str, Any]:
        s_up = side.upper()
        side_str = "bid" if s_up in ("BUY", "BID", "LONG") else "ask"
        op_data = {
            "symbol": symbol,
            "amount": f"{base_qty:.8f}".rstrip("0").rstrip("."),
            "side": side_str,
            "slippage_percent": self.slippage_percent,
            "reduce_only": self.reduce_only,
        }
        body = self._build_signature("create_market_order", op_data)
        payload = json.dumps(body, separators=_JSON_SEPARATORS).encode("utf-8")
        r = self._session.post(self._url_order_market, data=payload, headers=self._headers_market, timeout=self._timeout)
        return r.json()

    def place_limit(self, symbol: str, side: str, base_qty: float, price: float) -> Dict[str, Any]:
        s_up = side.upper()
        side_str = "bid" if s_up in ("BUY", "BID", "LONG") else "ask"
        op_data = {
            "symbol": symbol.upper(),
            "amount": f"{base_qty:.8f}".rstrip("0").rstrip("."),
            "price": f"{price:.2f}",
            "side": side_str,
            "tif": "GTC",
            "reduce_only": True,
            "client_order_id": str(uuid.uuid4())
        }
        return self._post_create(op_data)

    def place_stop(self, symbol: str, side: str, base_qty: float, stop_price: float) -> Dict[str, Any]:
        s_up = side.upper()
        side_str = "bid" if s_up in ("BUY", "BID", "LONG") else "ask"
        
        # STOP MARKET: Solo trigger_price, SIN price.
        op_data = {
            "symbol": symbol.upper(),
            "amount": f"{base_qty:.8f}".rstrip("0").rstrip("."),
            "trigger_price": f"{stop_price:.2f}",
            "side": side_str,
            "tif": "GTC",
            "reduce_only": True,
            "client_order_id": str(uuid.uuid4())
        }
        return self._post_create(op_data)

    def _post_create(self, op_data):
        clean = {k: v for k, v in op_data.items() if v is not None}
        body = self._build_signature("create_order", clean)
        payload = json.dumps(body, separators=_JSON_SEPARATORS).encode("utf-8")
        r = self._session.post(self._url_order_create, data=payload, headers=self._headers_create, timeout=self._timeout)
        if not r.ok: print(f"❌ Pacifica Create Error: {r.status_code} {r.text}")
        return r.json()

PacificaAPI = PacificaClient
