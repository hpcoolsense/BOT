from __future__ import annotations
import os, time, asyncio, threading
from typing import Any, Dict

async def _maybe_call(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res): return await res
    return res

def _env(n, d=""): return (os.getenv(n) or d).strip()
def _require(v, n): 
    if not v: raise RuntimeError(f"Lighter: falta {n}")
    return v

def _token_cfg(symbol: str) -> Dict[str, Any]:
    return {
        "market_index": int(_env("LIGHTER_MARKET_INDEX", "0")),
        "base_decimals": int(_env("LIGHTER_BASE_DECIMALS", "18")),
        "price_decimals": int(_env("LIGHTER_PRICE_DECIMALS", "2")),
    }

def _load_sdk():
    try:
        mod = __import__("lighter", fromlist=["SignerClient"])
        return mod, getattr(mod, "SignerClient")
    except: raise ModuleNotFoundError("No lighter SDK")

_ASYNC_LOOP = asyncio.new_event_loop()
threading.Thread(target=_ASYNC_LOOP.run_forever, daemon=True).start()

class LighterAPI:
    def __init__(self):
        self.base_url = _env("LIGHTER_REST_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
        self.priv_key = _require(_env("LIGHTER_API_KEY_PRIVATE_KEY"), "KEY")
        self.api_idx = int(_env("LIGHTER_API_KEY_INDEX", "0"))
        self.acc_idx = int(_env("LIGHTER_ACCOUNT_INDEX", "0"))
        
        self._mod, self._SignerClient = _load_sdk()
        self._client = None
        self._lock = threading.Lock()

    async def _get_client(self):
        if not self._client:
            self._client = self._SignerClient(
                url=self.base_url, 
                account_index=self.acc_idx, 
                api_private_keys={self.api_idx: self.priv_key}
            )
            await _maybe_call(self._client.create_auth_token_with_expiry, 600000)
        return self._client

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, _ASYNC_LOOP).result()

    def place_market(self, symbol: str, side: str, qty_base: float, avg_exec_px: float):
        async def _do():
            c = await self._get_client()
            cfg = _token_cfg(symbol)
            return await _maybe_call(c.create_market_order_if_slippage,
                market_index=cfg["market_index"],
                client_order_index=0,
                base_amount=int(qty_base * 10**cfg["base_decimals"]),
                ideal_price=int(avg_exec_px * 10**cfg["price_decimals"]),
                max_slippage=0.01,
                is_ask=(side.upper() in ["SELL", "SHORT"])
            )
        try: return {"accepted": True, "raw": self._run(_do())}
        except Exception as e: return {"accepted": False}

    def place_limit(self, symbol: str, side: str, qty_base: float, price: float):
        async def _do():
            c = await self._get_client()
            cfg = _token_cfg(symbol)
            from lighter.signer_client import CreateOrderTxReq
            
            # CORRECCIÓN: snake_case estricto
            req = CreateOrderTxReq(
                market_index=cfg["market_index"],
                client_order_index=int(time.time()*1000)%1000000,
                base_amount=int(qty_base * 10**cfg["base_decimals"]),
                price=int(price * 10**cfg["price_decimals"]),
                is_ask=(side.upper() in ["SELL", "SHORT"]),
                order_type=c.ORDER_TYPE_LIMIT, 
                time_in_force=c.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=True,
                trigger_price=0,
                order_expiry=0
            )
            return await c.create_order(req)
        try: return {"accepted": True, "raw": self._run(_do())}
        except Exception as e: print(f"Lighter Limit Err: {e}"); return {"accepted": False}

    def place_stop(self, symbol: str, side: str, qty_base: float, price: float):
        async def _do():
            c = await self._get_client()
            cfg = _token_cfg(symbol)
            from lighter.signer_client import CreateOrderTxReq
            
            is_ask = (side.upper() in ["SELL", "SHORT"])
            exec_px = price * 0.95 if is_ask else price * 1.05
            
            req = CreateOrderTxReq(
                market_index=cfg["market_index"],
                client_order_index=int(time.time()*1000)%1000000,
                base_amount=int(qty_base * 10**cfg["base_decimals"]),
                price=int(exec_px * 10**cfg["price_decimals"]),
                trigger_price=int(price * 10**cfg["price_decimals"]),
                is_ask=is_ask,
                order_type=c.ORDER_TYPE_STOP_LOSS_LIMIT, 
                time_in_force=c.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=True,
                order_expiry=0
            )
            return await c.create_order(req)
        try: return {"accepted": True, "raw": self._run(_do())}
        except Exception as e: print(f"Lighter Stop Err: {e}"); return {"accepted": False}

    def cancel_all_orders(self, symbol: str):
        try:
            async def _do():
                c = await self._get_client()
                await c.cancel_all_orders(market_index=_token_cfg(symbol)["market_index"])
            self._run(_do())
        except: pass

LighterClient = LighterAPI
