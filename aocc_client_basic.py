import os
import json
import re
from datetime import datetime  # 若未使用可視情況移除

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


SYSTEM_PROMPT = (
    "You are an expert Data Engineer and Architect. "
    "Your objective is to analyze the provided code or data structures, "
    "focusing on data lineage, data quality transformations, performance, and best practices. "
    "Please provide clear, structured, and actionable technical insights based on the user's instructions."
)


def _aocc_request_json(method: str, url: str, **kwargs) -> dict:
    """共用的 AOCC HTTP 請求 helper。"""
    kwargs.setdefault("timeout", 120)
    # AOCC 目前常用自簽憑證，所以關掉 SSL 驗證
    kwargs["verify"] = False
    resp = requests.request(method, url, **kwargs)
    resp.raise_for_status()
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError:
        raise RuntimeError(f"無法解析 {url} 的 JSON 回應")


def _aocc_fetch_token() -> str:
    url = os.getenv("AOCC_GET_TOKEN_URL", "").strip()
    key = os.getenv("AOCC_TOKEN_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("缺少 AOCC token 取得設定（AOCC_GET_TOKEN_URL / AOCC_TOKEN_KEY）")

    data = _aocc_request_json(
        "GET",
        url,
        headers={"Authorization": key, "Accept": "application/json"},
        params={"key": ""},
    )
    tok = data.get("token") or data.get("access_token")
    if not tok:
        raise RuntimeError(f"取得 AOCC token 失敗：{str(data)[:200]}")
    return tok


def _aocc_new_session(token: str) -> str:
    url = os.getenv("AOCC_NEW_SESSION_URL", "").strip()
    if not url:
        return ""
    data = _aocc_request_json(
        "POST",
        url,
        headers={"Authorization": token, "Accept": "application/json"},
        json={},
    )
    return data.get("session_id") or data.get("sid") or ""


def _extract_text_from_aocc_response(data: dict) -> str:
    """
    解析 AOCC / OpenRouter 風格回應裡真正的文字內容。
    """
    content = data.get("textResponse") or ""
    if not content:
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    if not content:
        msg = data.get("choices", [{}])[0].get("message", {})
        for k in ("reasoning", "reasoning_content", "meta", "metadata"):
            v = msg.get(k)
            if isinstance(v, str) and v.strip():
                content = v
                break
            if isinstance(v, dict):
                for kk in ("output_text", "final", "answer"):
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip():
                        content = vv
                        break
            if content:
                break
    if not content:
        ch0 = data.get("choices", [{}])[0]
        if isinstance(ch0.get("content"), list):
            pieces = []
            for c in ch0["content"]:
                if isinstance(c, dict) and c.get("type") in ("text", "output_text"):
                    txt = c.get("text") or c.get("output_text") or ""
                    if txt:
                        pieces.append(txt)
            content = "\n".join(pieces).strip()

    if content:
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL | re.IGNORECASE).strip()
        content = re.sub(r'^\s*["\'`]+|["\'`]+\s*$', "", content)
        if content.startswith("{") and content.endswith("}"):
            try:
                j = json.loads(content)
                for key in ("summary", "text", "answer"):
                    if isinstance(j.get(key), str) and j[key].strip():
                        content = j[key].strip()
                        break
                else:
                    texts = [v for v in j.values() if isinstance(v, str)]
                    if texts:
                        content = " ".join(texts).strip()
            except Exception:
                pass
    return content.strip()


def ask_llm(model: str, user_prompt: str, stats_markdown: str) -> str:
    """
    用 AOCC 產生「整體異常摘要」。
    """
    chat_url = os.getenv("AOCC_CHAT_URL", "").strip()
    if not chat_url:
        raise RuntimeError("缺少 AOCC_CHAT_URL 設定")

    model_name = (model or os.getenv("AOCC_MODEL", "")).strip() or "gpt41"
    temperature = float(os.getenv("AOCC_TEMPERATURE", "0.2"))
    max_tokens = int(os.getenv("AOCC_MAX_TOKENS", "3072"))
    service = os.getenv("AOCC_SERVICE", os.getenv("AOCC_PROVIDER", "")).strip()

    token = _aocc_fetch_token()
    session_id = _aocc_new_session(token)

    prompt = (
        SYSTEM_PROMPT
        + "\n\n[User Prompt]\n"
        + (user_prompt or "")
        + "\n\n[Weekly Stats]\n"
        + stats_markdown
    )

    payload = {
        "session_id": session_id,
        "response_type": "normal",
        "assistant_id": "",
        "version": model_name,
        "message": prompt,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if service:
        payload["service"] = service

    data = _aocc_request_json(
        "POST",
        chat_url,
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )

    err = data.get("error")
    if isinstance(err, str) and err.strip().lower() not in ("", "ok", "success", "none", "null"):
        raise RuntimeError(f"AOCC 回傳錯誤：{err}")

    content = data.get("textResponse") or data.get("text") or data.get("message") or ""
    if not isinstance(content, str) or not content.strip():
        content = _extract_text_from_aocc_response(data)
    if not content:
        raise RuntimeError(f"AOCC 回傳空內容：{str(data)[:300]}")

    return content.strip()