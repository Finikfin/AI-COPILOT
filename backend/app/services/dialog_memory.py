from __future__ import annotations

import json
import os
from typing import Any

try:
    from redis import asyncio as aioredis
except ModuleNotFoundError:
    aioredis = None

from app.utils.ollama_client import chat_json, summarize_dialog_text


class DialogMemoryService:
    def __init__(self) -> None:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = os.getenv("REDIS_PORT", "6379")
        self.redis_url = os.getenv("REDIS_URL", f"redis://{redis_host}:{redis_port}")
        self.ttl_seconds = int(os.getenv("DIALOG_TTL_SECONDS", "86400"))

    async def get_context(self, dialog_id: str) -> tuple[list[dict[str, Any]], str | None]:
        redis = await self._get_redis()
        if redis is None:
            return [], None

        messages_raw = await redis.get(self._messages_key(dialog_id))
        summary = await redis.get(self._summary_key(dialog_id))
        messages = self._decode_messages(messages_raw)
        return messages, summary

    async def append_and_summarize(self, dialog_id: str, role: str, content: str) -> str | None:
        redis = await self._get_redis()
        if redis is None:
            return None

        messages_key = self._messages_key(dialog_id)
        summary_key = self._summary_key(dialog_id)

        current_messages = self._decode_messages(await redis.get(messages_key))
        current_messages.append({"role": role, "content": content})
        await redis.set(messages_key, json.dumps(current_messages, ensure_ascii=False), ex=self.ttl_seconds)

        try:
            summary = await summarize_dialog_text(current_messages)
        except Exception:
            summary = None
        if summary is None:
            summary = self._fallback_summary(current_messages)
        await redis.set(summary_key, summary, ex=self.ttl_seconds)
        return summary

    async def reset(self, dialog_id: str) -> None:
        redis = await self._get_redis()
        if redis is None:
            return
        await redis.delete(self._messages_key(dialog_id), self._summary_key(dialog_id))

    async def _get_redis(self):
        if aioredis is None:
            return None
        try:
            redis = aioredis.from_url(self.redis_url, encoding="utf8", decode_responses=True)
            await redis.ping()
            return redis
        except Exception:
            return None

    def _messages_key(self, dialog_id: str) -> str:
        return f"dialog:{dialog_id}:messages"

    def _summary_key(self, dialog_id: str) -> str:
        return f"dialog:{dialog_id}:summary"

    def _decode_messages(self, payload: str | None) -> list[dict[str, Any]]:
        if not payload:
            return []
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [item for item in parsed if isinstance(item, dict)]

    def _fallback_summary(self, messages: list[dict[str, Any]]) -> str:
        chunks = [str(item.get("content", "")) for item in messages[-4:]]
        return "\n".join(chunk for chunk in chunks if chunk)
