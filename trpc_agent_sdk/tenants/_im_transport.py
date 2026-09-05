# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""IM message transport: real HTTP senders, chunking, rate limiting, retry.

Components:

- :class:`MessageChunker` — split long texts into platform-size chunks.
- :class:`RateLimiter` — fixed-window rate limiting (Redis with in-memory
  fallback) honouring ``ChannelConfig.rate_limit_per_minute``.
- :func:`send_with_retry` — exponential-backoff retry wrapper.
- :class:`TelegramSender` — Telegram Bot API client (sendMessage / sendPhoto).
- :class:`WeComSender` — WeCom (企业微信) app-message client with
  access-token caching.

All senders accept an injectable ``httpx.AsyncClient`` so tests can use
``httpx.MockTransport`` without network access.
"""

import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx

from trpc_agent_sdk.log import logger

RetryableFunc = Callable[[], Awaitable[bool]]
"""Async callable returning True on success, False on retryable failure."""


class MessageChunker:
    """Splits long message text into platform-size chunks."""

    PLATFORM_LIMITS: Dict[str, int] = {
        "telegram": 4096,
        "wecom": 2048,
    }
    DEFAULT_LIMIT = 4096

    @classmethod
    def limit_for(cls, channel_type: str) -> int:
        """Return the text length limit for a channel."""
        return cls.PLATFORM_LIMITS.get(channel_type, cls.DEFAULT_LIMIT)

    @staticmethod
    def split_text(text: str, limit: int) -> List[str]:
        """Split ``text`` into chunks of at most ``limit`` characters.

        Prefers cutting at newlines inside the limit; falls back to a hard
        cut. Empty/None text yields a single empty-string chunk so callers
        can still send a placeholder reply.

        Args:
            text: Message text to split.
            limit: Maximum characters per chunk.

        Returns:
            List of chunks (at least one).
        """
        text = text or ""
        if len(text) <= limit:
            return [text]

        chunks: List[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= limit:
                chunks.append(remaining)
                break
            cut = remaining.rfind("\n", 0, limit)
            if cut <= 0:
                cut = limit
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        return chunks


class RateLimiter:
    """Fixed-window per-tenant/channel rate limiter.

    Uses a Redis counter keyed ``im_rate:{tenant_id}:{channel}`` with a
    60-second window; degrades to per-process in-memory counters when Redis
    is unavailable (same pattern as ``MessageDeduplicator``).
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize the rate limiter.

        Args:
            redis_url: Redis URL for shared counters.
        """
        try:
            import redis.asyncio as redis  # noqa: F401 -- availability check
        except ImportError:
            raise ImportError("RateLimiter requires 'redis' package. "
                              "Install with: pip install redis")

        self._redis_url = redis_url
        self._redis = None
        self._redis_failed = False
        # In-memory fallback: {key: (window_start, count)}
        self._local_windows: Dict[str, Any] = {}

    async def _get_redis(self):
        """Lazy initialization of the Redis connection."""
        if self._redis is None:
            import redis.asyncio as redis

            self._redis = await redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    async def acquire(self, tenant_id: str, channel_type: str, limit_per_minute: int) -> bool:
        """Try to reserve one send slot.

        Args:
            tenant_id: Tenant the send belongs to.
            channel_type: IM channel identifier.
            limit_per_minute: Maximum sends per 60-second window.

        Returns:
            True when the send is allowed, False when rate-limited.
        """
        if limit_per_minute <= 0:
            return True
        key = f"im_rate:{tenant_id}:{channel_type}"

        if not self._redis_failed:
            try:
                redis_client = await self._get_redis()
                count = await redis_client.incr(key)
                if count == 1:
                    await redis_client.expire(key, 60)
                return count <= limit_per_minute
            except Exception as e:
                logger.warning(f"Redis rate limiting unavailable, "
                               f"falling back to in-memory: {e}")
                self._redis_failed = True

        now = time.monotonic()
        window_start, count = self._local_windows.get(key, (now, 0))
        if now - window_start >= 60:
            window_start, count = now, 0
        if count >= limit_per_minute:
            return False
        self._local_windows[key] = (window_start, count + 1)
        return True


async def send_with_retry(func: RetryableFunc,
                          *,
                          max_retries: int = 3,
                          base_delay: float = 0.5,
                          sleep: Callable[[float], Awaitable[None]] = None) -> bool:
    """Run ``func`` with exponential-backoff retries.

    Args:
        func: Async callable returning True on success / False on failure.
        max_retries: Total attempts (including the first).
        base_delay: Base delay in seconds; delay doubles each retry.
        sleep: Async sleep function (injectable for tests).

    Returns:
        True when any attempt succeeded, False after all attempts failed.
    """
    if sleep is None:
        import asyncio

        sleep = asyncio.sleep

    for attempt in range(max_retries):
        try:
            if await func():
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f"Send attempt {attempt + 1}/{max_retries} failed: {e}")
        if attempt < max_retries - 1:
            await sleep(base_delay * (2**attempt))
    return False


class TelegramSender:
    """Telegram Bot API sender.

    The bot token comes from ``ChannelConfig.api_key``.
    """

    def __init__(self,
                 bot_token: str,
                 base_url: str = "https://api.telegram.org",
                 http_client: Optional[httpx.AsyncClient] = None):
        """Initialize the Telegram sender.

        Args:
            bot_token: Bot token (``ChannelConfig.api_key``).
            base_url: API base URL (overridable for tests).
            http_client: Optional pre-built httpx client (tests inject
                ``MockTransport`` here).
        """
        self._bot_token = bot_token
        self._base_url = base_url.rstrip("/")
        self._http_client = http_client

    async def _client(self) -> httpx.AsyncClient:
        """Return the HTTP client, creating a default one if needed."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def send_text(self,
                        chat_id: str,
                        text: str,
                        reply_to_message_id: Optional[str] = None,
                        parse_mode: Optional[str] = None) -> bool:
        """Send a text message via ``sendMessage``.

        Args:
            chat_id: Target chat identifier.
            text: Message text.
            reply_to_message_id: Optional message to reply to.
            parse_mode: Optional Telegram parse mode (e.g. ``Markdown``).

        Returns:
            True on success.
        """
        client = await self._client()
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        if parse_mode:
            payload["parse_mode"] = parse_mode
        response = await client.post(f"{self._base_url}/bot{self._bot_token}/sendMessage", json=payload)
        if response.status_code != 200 or not response.json().get("ok"):
            logger.warning(f"Telegram sendMessage failed: {response.status_code} {response.text[:200]}")
            return False
        return True

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None) -> bool:
        """Send an image by URL via ``sendPhoto``.

        Args:
            chat_id: Target chat identifier.
            image_url: Public URL of the image.
            caption: Optional caption.

        Returns:
            True on success.
        """
        client = await self._client()
        payload: Dict[str, Any] = {"chat_id": chat_id, "photo": image_url}
        if caption:
            payload["caption"] = caption
        response = await client.post(f"{self._base_url}/bot{self._bot_token}/sendPhoto", json=payload)
        if response.status_code != 200 or not response.json().get("ok"):
            logger.warning(f"Telegram sendPhoto failed: {response.status_code} {response.text[:200]}")
            return False
        return True


class WeComSender:
    """WeCom (企业微信) app-message sender.

    Credential mapping from ``ChannelConfig``:
    ``bot_id`` → corp_id, ``api_key`` → corp_secret,
    ``webhook_token`` → agent_id.

    Access tokens are fetched once and cached until expiry.
    """

    def __init__(self,
                 corp_id: str,
                 corp_secret: str,
                 agent_id: str,
                 base_url: str = "https://qyapi.weixin.qq.com",
                 http_client: Optional[httpx.AsyncClient] = None):
        """Initialize the WeCom sender.

        Args:
            corp_id: Enterprise corp id.
            corp_secret: App secret used to fetch access tokens.
            agent_id: Agent id the message is sent from.
            base_url: API base URL (overridable for tests).
            http_client: Optional pre-built httpx client (tests inject
                ``MockTransport`` here).
        """
        self._corp_id = corp_id
        self._corp_secret = corp_secret
        self._agent_id = agent_id
        self._base_url = base_url.rstrip("/")
        self._http_client = http_client
        self._access_token: Optional[str] = None
        self._token_expires_at = 0.0

    async def _client(self) -> httpx.AsyncClient:
        """Return the HTTP client, creating a default one if needed."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def _get_access_token(self) -> Optional[str]:
        """Fetch (or reuse a cached) WeCom access token."""
        if self._access_token and time.monotonic() < self._token_expires_at:
            return self._access_token
        client = await self._client()
        response = await client.get(
            f"{self._base_url}/cgi-bin/gettoken",
            params={
                "corpid": self._corp_id,
                "corpsecret": self._corp_secret,
            },
        )
        data = response.json()
        if data.get("errcode") != 0 or not data.get("access_token"):
            logger.warning(f"WeCom gettoken failed: {data}")
            return None
        self._access_token = data["access_token"]
        # Refresh 5 minutes early to tolerate clock skew.
        self._token_expires_at = time.monotonic() + max(int(data.get("expires_in", 7200)) - 300, 60)
        return self._access_token

    async def send_text(self, user_id: str, text: str) -> bool:
        """Send a text app-message via ``message/send``.

        Args:
            user_id: Target WeCom user id (``touser``).
            text: Message text.

        Returns:
            True on success.
        """
        token = await self._get_access_token()
        if not token:
            return False
        client = await self._client()
        payload = {
            "touser": user_id,
            "msgtype": "text",
            "agentid": self._agent_id,
            "text": {
                "content": text
            },
        }
        response = await client.post(f"{self._base_url}/cgi-bin/message/send",
                                     params={"access_token": token},
                                     json=payload)
        data = response.json()
        if data.get("errcode") != 0:
            logger.warning(f"WeCom message/send failed: {data}")
            return False
        return True

    async def send_markdown(self, user_id: str, markdown: str) -> bool:
        """Send a markdown app-message via ``message/send``.

        WeCom markdown supports a subset of Markdown syntax (bold, links,
        quotes, code, line breaks); complex tables/images are not rendered
        client-side. Card-style responses should be composed accordingly.

        Args:
            user_id: Target WeCom user id (``touser``).
            markdown: Markdown-formatted message body.

        Returns:
            True on success.
        """
        token = await self._get_access_token()
        if not token:
            return False
        client = await self._client()
        payload = {
            "touser": user_id,
            "msgtype": "markdown",
            "agentid": self._agent_id,
            "markdown": {
                "content": markdown
            },
        }
        response = await client.post(f"{self._base_url}/cgi-bin/message/send",
                                     params={"access_token": token},
                                     json=payload)
        data = response.json()
        if data.get("errcode") != 0:
            logger.warning(f"WeCom markdown send failed: {data}")
            return False
        return True
