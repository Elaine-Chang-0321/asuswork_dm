# aocc_client.py
# -*- coding: utf-8 -*-

"""AOCC client (env-only endpoints, Vault-backed key) for the monitor project.

Design goals
- AOCC endpoints are read from environment variables (no XML required).
- AOCC token-key resolution:
    1) AOCC_TOKEN_KEY (optional override)
    2) Vault secret (KV v2) configured by secret_path_aocc (or AOCC_SECRET_PATH)
       expected field name: api_key
- Avoids dependency on get_key.py (prevents ModuleNotFoundError).

Required env (minimum)
- AOCC_GET_TOKEN_URL
- AOCC_CHAT_URL
- (optional) AOCC_GET_HISTORY_URL, AOCC_NEW_SESSION_URL
- Vault:
    VAULT_ADDR, VAULT_ROLE_ID, VAULT_SECRET_ID
    secret_path_aocc=/v1/genai_key_secret/data/prod/aocc
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional

import requests
import urllib3

# ----------------------------
# Load .env deterministically
# ----------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(_BASE_DIR, ".env"), override=False)
except Exception:
    pass

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")


# ----------------------------
# Defaults / Prompts
# ----------------------------
SYSTEM_PROMPT = (
    "You are an expert Data Engineer and Architect. "
    "Your objective is to analyze the provided code or data structures, "
    "focusing on data lineage, data quality transformations, performance, and best practices. "
    "Please provide clear, structured, and actionable technical insights based on the user's instructions."
)


# ----------------------------
# Exceptions
# ----------------------------
class AOCCClientError(RuntimeError):
    pass


class TokenError(AOCCClientError):
    pass


class SessionError(AOCCClientError):
    pass


class ChatError(AOCCClientError):
    pass


class HistoryError(AOCCClientError):
    pass


# ----------------------------
# Config (ENV-first). XML optional.
# ----------------------------
def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Read AOCC config.

    For your "方式 B": ENV is primary; XML is optional and only used if provided.
    """

    cfg: Dict[str, Any] = {"model_service": {}}

    # Optional XML support (kept for compatibility)
    if config_path and os.path.exists(config_path):
        try:
            import xml.etree.ElementTree as ET

            root = ET.parse(config_path).getroot()

            url_node = root.find("url")
            if url_node is not None:

                def _get(tag: str) -> str:
                    n = url_node.find(tag)
                    return (n.text or "").strip() if n is not None else ""

                cfg["get_token_url"] = _get("get_token_url")
                cfg["chat_url"] = _get("chat_url")
                cfg["get_history_url"] = _get("get_history_url")
                cfg["new_session_url"] = _get("new_session_url")
                cfg["assistant_id_url"] = _get("assistant_id_url")

            ms_node = root.find("model_service")
            if ms_node is not None:
                for child in list(ms_node):
                    name = (child.tag or "").strip()
                    svc = (child.text or "").strip()
                    if name:
                        cfg["model_service"][name] = svc
        except Exception as e:
            logger.warning(f"load_config: XML parse failed: {e}")

    # ENV overrides
    env_token = os.getenv("AOCC_GET_TOKEN_URL", "").strip()
    env_chat = os.getenv("AOCC_CHAT_URL", "").strip()
    env_hist = os.getenv("AOCC_GET_HISTORY_URL", "").strip()
    env_new = os.getenv("AOCC_NEW_SESSION_URL", "").strip()

    if env_token:
        cfg["get_token_url"] = env_token
    if env_chat:
        cfg["chat_url"] = env_chat
    if env_hist:
        cfg["get_history_url"] = env_hist
    if env_new:
        cfg["new_session_url"] = env_new

    # model_service (optional)
    ms_json = os.getenv("AOCC_MODEL_SERVICE_JSON", "").strip()
    if ms_json:
        try:
            obj = json.loads(ms_json)
            if isinstance(obj, dict):
                cfg["model_service"].update({str(k): str(v) for k, v in obj.items()})
        except Exception:
            pass

    return cfg


# ----------------------------
# Small helpers
# ----------------------------
def _truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _resolve_secret_path(ref: str) -> str:
    """Resolve a secret path.

    Accepts either:
    - direct path: /v1/xxx...
    - indirection: env var name such as secret_path_aocc
    """

    ref = (ref or "").strip()
    if not ref:
        return ""

    if ref.startswith("/v1/") or ref.startswith("v1/") or ref.startswith("/"):
        return ref

    return (os.getenv(ref) or ref).strip()


# ----------------------------
# Vault: User Token login + KV read (using npspo_vault_client)
# ----------------------------

def _get_aocc_key_from_vault() -> str:
    # Allow explicit override AOCC_SECRET_PATH, else default to secret_path_aocc
    ref = (os.getenv("AOCC_SECRET_PATH") or "secret_path_aocc").strip()
    path = _resolve_secret_path(ref)
    if not path:
        # fallback: direct env var (common)
        path = (os.getenv("secret_path_aocc") or "").strip()

    if not path:
        raise TokenError("Missing secret_path_aocc (or AOCC_SECRET_PATH) in environment")

    # [改用 sample4da.py 的 User Token 打法]
    try:
        from npspo_vault_client import VaultClient
        
        vault_client = VaultClient(logger=logger)
        
        # 1. 取得 User Token
        user_token = vault_client.get_user_token()
        logger.info("成功使用 User Token 登入 Vault 伺服器")
        
        # 2. 透過 User Token 與 Path 取得 Secret
        secret = vault_client.get_vault_secret(secret_path=path, token=user_token)
    except Exception as e:
        logger.error(f"從 Vault 取得 AOCC 密鑰失敗: {e}")
        raise TokenError(f"Vault Client 錯誤: {e}")

    # Expected field name per your get_key.py test output: ['api_key']
    api_key = (secret.get("api_key") or "").strip() if isinstance(secret, dict) else ""
    if not api_key:
        raise TokenError(f"Vault secret has no api_key field: keys={list(secret.keys() if isinstance(secret, dict) else type(secret))}")
    return api_key


# ----------------------------
# HTTP helper
# ----------------------------
def _aocc_request_json(method: str, url: str, **kwargs) -> Dict[str, Any]:
    """Shared AOCC HTTP request helper."""

    timeout = int(os.getenv("AOCC_TIMEOUT", "120"))
    verify_ssl = _truthy("AOCC_VERIFY_SSL", False)

    kwargs.setdefault("timeout", timeout)

    if verify_ssl:
        ca_bundle = os.getenv("AOCC_CA_BUNDLE", "").strip()
        kwargs["verify"] = ca_bundle if ca_bundle else True
    else:
        kwargs["verify"] = False

    resp = requests.request(method, url, **kwargs)
    resp.raise_for_status()

    if not resp.content:
        return {}

    try:
        return resp.json()
    except ValueError:
        raise AOCCClientError(f"Cannot parse JSON response from {url}")


# ----------------------------
# Token key resolution (ENV first, else Vault)
# ----------------------------
def _resolve_token_key() -> str:
    """Resolve AOCC token key.

    1) AOCC_TOKEN_KEY (optional)
    2) Vault (secret_path_aocc -> api_key)
    """

    key = os.getenv("AOCC_TOKEN_KEY", "").strip()
    if key:
        return key

    return _get_aocc_key_from_vault()


# ----------------------------
# Token cache
# ----------------------------
_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0}


def _token_cache_seconds() -> int:
    # default 55 minutes if unknown expiry
    return int(os.getenv("AOCC_TOKEN_CACHE_SECONDS", "3300"))


def _aocc_fetch_token() -> str:
    """Get AOCC bearer token (cached)."""

    cfg = load_config(os.getenv("AOCC_CONFIG_PATH", "").strip() or None)
    url = (cfg.get("get_token_url") or "").strip()
    if not url:
        raise TokenError("Missing AOCC_GET_TOKEN_URL")

    disable_cache = _truthy("AOCC_DISABLE_TOKEN_CACHE", False)
    now = time.time()
    if not disable_cache:
        cached = _TOKEN_CACHE.get("token")
        exp = float(_TOKEN_CACHE.get("expires_at") or 0.0)
        if cached and now < exp:
            return str(cached)

    key = _resolve_token_key()

    data = _aocc_request_json(
        "GET",
        url,
        headers={"Authorization": key, "Accept": "application/json"},
        params={"key": ""},
    )

    tok = data.get("token") or data.get("access_token")
    if not tok or not isinstance(tok, str):
        raise TokenError(f"Failed to get token: {str(data)[:300]}")

    # expiry handling
    expires_in = data.get("expires_in") or data.get("expiresIn")
    ttl = _token_cache_seconds()
    try:
        if isinstance(expires_in, (int, float)) and float(expires_in) > 0:
            ttl = max(60, int(float(expires_in)) - 60)
    except Exception:
        pass

    _TOKEN_CACHE["token"] = tok
    _TOKEN_CACHE["expires_at"] = now + float(ttl)
    return tok


# ----------------------------
# Session / History / Chat
# ----------------------------
def _aocc_new_session(token: str) -> str:
    """Create a new AOCC session_id (optional endpoint)."""

    cfg = load_config(os.getenv("AOCC_CONFIG_PATH", "").strip() or None)
    url = (cfg.get("new_session_url") or "").strip()
    if not url:
        return ""

    data = _aocc_request_json(
        "POST",
        url,
        headers={"Authorization": token, "Accept": "application/json"},
        json={},
    )

    sid = data.get("session_id") or data.get("sid") or ""
    return sid if isinstance(sid, str) else ""


def get_history(token: str, session_id: str, config: Dict[str, Any]) -> Any:
    """Fetch AOCC chat history (optional endpoint)."""

    url_base = (config.get("get_history_url") or "").strip()
    if not url_base or not session_id:
        return []

    url = f"{url_base}?session_id={session_id}"
    data = _aocc_request_json(
        "GET",
        url,
        headers={"Authorization": token, "Accept": "application/json"},
    )

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "history" in data:
            return data.get("history")
        return data
    return []


def _extract_text_from_aocc_response(data: Dict[str, Any]) -> str:
    """Extract text content from an AOCC response payload."""

    if not isinstance(data, dict):
        return ""

    content = data.get("textResponse") or data.get("text") or data.get("message") or ""
    if isinstance(content, str) and content.strip():
        return _postprocess_text(content)

    # OpenAI-like: choices[0].message.content
    try:
        c0 = (data.get("choices") or [{}])[0]
        msg = c0.get("message", {}) if isinstance(c0, dict) else {}
        mc = msg.get("content", "")
        if isinstance(mc, str) and mc.strip():
            return _postprocess_text(mc)
    except Exception:
        pass

    # fallback: try other keys
    try:
        for _, v in data.items():
            if isinstance(v, str) and v.strip():
                return _postprocess_text(v)
            if isinstance(v, dict):
                for kk in ("output_text", "final", "answer", "content"):
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip():
                        return _postprocess_text(vv)
    except Exception:
        pass

    return ""


def _postprocess_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    # remove internal think tags if any
    t = re.sub(r"<think>.*?</think>", "", t, flags=re.DOTALL | re.IGNORECASE).strip()

    # strip leading/trailing quotes/backticks
    t = re.sub(r"^\s*[\"'`]+|[\"'`]+\s*$", "", t).strip()

    # JSON string? try to extract common keys
    if t.startswith("{") and t.endswith("}"):
        try:
            j = json.loads(t)
            if isinstance(j, dict):
                for key in ("summary", "text", "answer", "result"):
                    vv = j.get(key)
                    if isinstance(vv, str) and vv.strip():
                        return vv.strip()
                strings = [v for v in j.values() if isinstance(v, str) and v.strip()]
                if strings:
                    return " ".join(strings).strip()
        except Exception:
            pass

    return t


def aocc_chat(
    model_version: str,
    message: str,
    config_path: Optional[str] = None,
    gen_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send a chat message to AOCC."""

    config = load_config(config_path)
    chat_url = (config.get("chat_url") or "").strip()
    if not chat_url:
        raise ChatError("Missing AOCC_CHAT_URL")

    token = _aocc_fetch_token()
    session_id = _aocc_new_session(token)

    temperature = 0.2
    max_tokens = 3072
    if gen_params and isinstance(gen_params, dict):
        if "temperature" in gen_params:
            try:
                temperature = float(gen_params["temperature"])
            except Exception:
                pass
        if "max_tokens" in gen_params:
            try:
                max_tokens = int(gen_params["max_tokens"])
            except Exception:
                pass

    service = os.getenv("AOCC_SERVICE", os.getenv("AOCC_PROVIDER", "")).strip()

    payload: Dict[str, Any] = {
        "session_id": session_id,
        "response_type": "normal",
        "assistant_id": "",
        "version": (model_version or "").strip() or "gpt41",
        "message": message or "",
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if service:
        payload["service"] = service

    chat_resp = _aocc_request_json(
        "POST",
        chat_url,
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )

    err = chat_resp.get("error") if isinstance(chat_resp, dict) else None
    if isinstance(err, str) and err.strip().lower() not in ("", "ok", "success", "none", "null"):
        return {"chat_response": chat_resp, "history_response": [], "error": err}

    hist = get_history(token, session_id, config) if session_id else []
    return {"chat_response": chat_resp, "history_response": hist}


# ----------------------------
# High-level entry for your project
# ----------------------------
def ask_llm(model: str, user_prompt: str, stats_markdown: str) -> str:
    """Convenience wrapper used by tasks.py."""

    model_name = (model or os.getenv("AOCC_MODEL", "")).strip() or "gpt41"
    prompt = (
        SYSTEM_PROMPT
        + "\n\n[User Prompt]\n"
        + (user_prompt or "")
        + "\n\n[Weekly Stats]\n"
        + (stats_markdown or "")
    )

    temperature = float(os.getenv("AOCC_TEMPERATURE", "0.2"))
    max_tokens = int(os.getenv("AOCC_MAX_TOKENS", "3072"))

    result = aocc_chat(
        model_version=model_name,
        message=prompt,
        config_path=None,
        gen_params={"temperature": temperature, "max_tokens": max_tokens},
    )

    if isinstance(result, dict) and result.get("error"):
        raise ChatError(str(result["error"]))

    chat_resp = result.get("chat_response") if isinstance(result, dict) else None
    if not isinstance(chat_resp, dict):
        raise ChatError(f"AOCC invalid response: {str(chat_resp)[:200]}")

    text = chat_resp.get("textResponse") or chat_resp.get("text") or chat_resp.get("message") or ""
    if not isinstance(text, str) or not text.strip():
        text = _extract_text_from_aocc_response(chat_resp)

    if not text:
        raise ChatError(f"AOCC returned empty content: {str(chat_resp)[:300]}")

    return text.strip()
