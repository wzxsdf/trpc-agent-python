# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant IM channel adapter implementation.

This module provides IM channel adapters that are aware of multi-tenant routing,
supporting WeCom, Telegram, WeChat, and other IM platforms with proper tenant
isolation and message routing.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional, Dict, Any, List, Callable
from dataclasses import dataclass
from datetime import datetime
import hashlib

from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.log import logger
from trpc_agent_sdk.types import Content, Part

from ._audit import (
    DECISION_IM_DUPLICATE,
    DECISION_IM_RECEIVED,
    DECISION_IM_REJECTED,
    DECISION_IM_REPLY_FAILED,
    DECISION_IM_REPLIED,
    TenantAuditLogger,
)
from ._im_transport import (MessageChunker, RateLimiter, TelegramSender, WeComSender, send_with_retry)
from ._tenant_model import Tenant
from ._tenant_store import TenantStore


@dataclass
class TenantMessage:
    """Unified message format for multi-tenant IM processing."""

    tenant_id: str
    channel_type: str  # 'wecom', 'telegram', 'wechat', etc.
    user_id: str  # Platform-specific user ID
    chat_id: str  # Platform-specific chat ID
    message_id: str  # Unique message identifier for deduplication
    content: str  # Message content
    metadata: Dict[str, Any]  # Additional platform-specific metadata
    timestamp: datetime
    is_group_chat: bool = False
    original_payload: Dict[str, Any] = None  # Original platform payload


@dataclass
class TenantResponse:
    """Unified response format for multi-tenant IM replies."""

    tenant_id: str
    channel_type: str
    user_id: str
    chat_id: str
    content: str
    message_type: str = "text"  # 'text', 'card', 'image', 'file', etc.
    metadata: Dict[str, Any] = None
    reply_to_message_id: Optional[str] = None


class TenantChannelAdapter(ABC):
    """Abstract base class for tenant-aware IM channel adapters.

    Each adapter handles platform-specific message format conversion,
    signature verification, and tenant routing.
    """

    def __init__(self, tenant_store: TenantStore, http_client: Optional[Any] = None):
        """Initialize channel adapter.

        Args:
            tenant_store: Tenant storage backend
            http_client: Optional pre-built ``httpx.AsyncClient`` (tests may
                inject a ``MockTransport``-backed client)
        """
        self._tenant_store = tenant_store
        self._http_client = http_client

    @abstractmethod
    async def handle_webhook(self, channel_type: str, payload: Dict[str, Any],
                             headers: Dict[str, Any]) -> Optional[TenantMessage]:
        """Handle incoming webhook from IM platform.

        Args:
            channel_type: Type of IM channel ('wecom', 'telegram', etc.)
            payload: Webhook payload from platform
            headers: HTTP headers for signature verification

        Returns:
            TenantMessage if successfully processed, None otherwise
        """
        pass

    @abstractmethod
    async def send_response(self, response: TenantResponse) -> bool:
        """Send response to IM platform.

        Args:
            response: Response to send

        Returns:
            True if sent successfully, False otherwise
        """
        pass

    @abstractmethod
    def generate_session_id(self, message: TenantMessage) -> str:
        """Generate session ID for a message.

        Args:
            message: Incoming tenant message

        Returns:
            Session ID for this conversation
        """
        pass


class WeComTenantAdapter(TenantChannelAdapter):
    """WeCom (企业微信) tenant-aware adapter.

    Handles WeCom webhook format, signature verification, and tenant routing.
    """

    async def handle_webhook(self, channel_type: str, payload: Dict[str, Any],
                             headers: Dict[str, Any]) -> Optional[TenantMessage]:
        """Handle WeCom webhook with tenant identification."""
        if channel_type != "wecom":
            return None

        try:
            # Extract token for tenant identification
            token = payload.get("token") or headers.get("X-WeCom-Token")

            if not token:
                logger.warning("Missing WeCom token for tenant identification")
                return None

            # Find tenant by WeCom token
            tenant = await self._find_tenant_by_wecom_token(token)
            if not tenant:
                logger.warning(f"No tenant found for WeCom token: {token}")
                return None

            # Verify webhook signature
            if not self._verify_wecom_signature(payload, tenant):
                logger.warning("Invalid WeCom webhook signature")
                return None

            # Extract message information
            message = self._extract_wecom_message(payload, tenant)
            logger.info(f"Processed WeCom message for tenant {tenant.tenant_id}")
            return message

        except Exception as e:
            logger.error(f"Error processing WeCom webhook: {e}")
            return None

    async def send_response(self, response: TenantResponse) -> bool:
        """Send response to WeCom.

        Credential mapping from ``ChannelConfig``: ``bot_id`` → corp_id,
        ``api_key`` → corp_secret, ``webhook_token`` → agent_id.
        """
        try:
            tenant = await self._tenant_store.get_tenant(response.tenant_id)
            if not tenant:
                logger.warning(f"Tenant {response.tenant_id} not found")
                return False

            wecom_config = tenant.get_channel_config("wecom")
            if not wecom_config:
                logger.warning(f"WeCom not configured for tenant {response.tenant_id}")
                return False
            if not (wecom_config.bot_id and wecom_config.api_key):
                logger.warning(f"WeCom credentials incomplete for tenant {response.tenant_id}")
                return False

            sender = WeComSender(
                corp_id=wecom_config.bot_id,
                corp_secret=wecom_config.api_key,
                agent_id=wecom_config.webhook_token or "1",
                http_client=self._http_client,
            )
            return await sender.send_text(response.user_id, response.content)

        except Exception as e:
            logger.error(f"Error sending WeCom response: {e}")
            return False

    def generate_session_id(self, message: TenantMessage) -> str:
        """Generate session ID for WeCom message.

        Strategy options:
        1. User-level: Each user has one session across all chats
        2. Chat-level: Each chat has its own session
        3. Group-level: Group chats share sessions
        """
        tenant = message.tenant_id

        # Strategy 1: User-level session
        # return f"{tenant}:wecom:user:{message.user_id}"

        # Strategy 2: Chat-level session (recommended for most cases)
        return f"{tenant}:wecom:chat:{message.chat_id}"

        # Strategy 3: Group-level sessions (for group chats)
        # if message.is_group_chat:
        #     return f"{tenant}:wecom:group:{message.chat_id}"
        # else:
        #     return f"{tenant}:wecom:user:{message.user_id}"

    async def _find_tenant_by_wecom_token(self, token: str) -> Optional[Tenant]:
        """Find tenant by WeCom webhook token."""
        # Search through all tenants to find matching WeCom token
        tenants = await self._tenant_store.list_tenants(active_only=True)

        for tenant in tenants:
            wecom_config = tenant.get_channel_config("wecom")
            if wecom_config and wecom_config.webhook_token == token:
                return tenant

        return None

    def _verify_wecom_signature(self, payload: Dict[str, Any], tenant: Tenant) -> bool:
        """Verify WeCom webhook signature."""
        try:
            wecom_config = tenant.get_channel_config("wecom")
            if not wecom_config or not wecom_config.webhook_secret:
                logger.warning("No WeCom secret configured for signature verification")
                return False

            # WeCom signature verification
            signature = payload.get("signature")
            timestamp = payload.get("timestamp")
            nonce = payload.get("nonce")

            if not all([signature, timestamp, nonce]):
                return False

            # Sort and concatenate parameters
            params = sorted([wecom_config.webhook_secret, timestamp, nonce])
            sign_str = "".join(params)

            # Calculate SHA1 signature
            calculated_signature = hashlib.sha1(sign_str.encode()).hexdigest()

            return signature == calculated_signature

        except Exception as e:
            logger.error(f"WeCom signature verification error: {e}")
            return False

    def _extract_wecom_message(self, payload: Dict[str, Any], tenant: Tenant) -> TenantMessage:
        """Extract message from WeCom webhook payload."""
        # WeCom message format parsing
        msg_type = payload.get("msg_type", "text")
        from_user = payload.get("from_user_name", "")
        to_user = payload.get("to_user_name", "")

        # Determine if group chat
        is_group_chat = from_user.startswith("$") or to_user.startswith("$")

        # Extract content based on message type
        if msg_type == "text":
            content = payload.get("content", "")
        else:
            content = f"[{msg_type} message]"

        return TenantMessage(
            tenant_id=tenant.tenant_id,
            channel_type="wecom",
            user_id=payload.get("from_user_id", ""),
            chat_id=payload.get("from_user_id", ""),
            message_id=payload.get("msg_id", f"wecom_{int(datetime.utcnow().timestamp())}"),
            content=content,
            metadata={
                "msg_type": msg_type,
                "from_user_name": from_user,
                "to_user_name": to_user,
                "agent_id": payload.get("agent_id", ""),
            },
            timestamp=datetime.utcnow(),
            is_group_chat=is_group_chat,
            original_payload=payload,
        )


class TelegramTenantAdapter(TenantChannelAdapter):
    """Telegram tenant-aware adapter.

    Handles Telegram bot API format, webhook verification, and tenant routing.
    """

    async def handle_webhook(self, channel_type: str, payload: Dict[str, Any],
                             headers: Dict[str, Any]) -> Optional[TenantMessage]:
        """Handle Telegram webhook with tenant identification."""
        if channel_type != "telegram":
            return None

        try:
            # Telegram bot token is typically in the webhook URL
            # Extract from headers or path
            bot_token = headers.get("X-Telegram-Bot-Token")

            if not bot_token:
                logger.warning("Missing Telegram bot token for tenant identification")
                return None

            # Find tenant by Telegram bot token
            tenant = await self._find_tenant_by_telegram_token(bot_token)
            if not tenant:
                logger.warning("No tenant found for Telegram bot token")
                return None

            # Extract message information
            message = self._extract_telegram_message(payload, tenant)
            logger.info(f"Processed Telegram message for tenant {tenant.tenant_id}")
            return message

        except Exception as e:
            logger.error(f"Error processing Telegram webhook: {e}")
            return None

    async def send_response(self, response: TenantResponse) -> bool:
        """Send response to Telegram via the Bot API.

        ``ChannelConfig.api_key`` holds the bot token.
        """
        try:
            tenant = await self._tenant_store.get_tenant(response.tenant_id)
            if not tenant:
                logger.warning(f"Tenant {response.tenant_id} not found")
                return False

            telegram_config = tenant.get_channel_config("telegram")
            if not telegram_config or not telegram_config.api_key:
                logger.warning(f"Telegram not configured for tenant {response.tenant_id}")
                return False

            sender = TelegramSender(
                bot_token=telegram_config.api_key,
                http_client=self._http_client,
            )
            if response.message_type == "image":
                image_url = (response.metadata or {}).get("image_url")
                if not image_url:
                    logger.warning("Telegram image response missing metadata.image_url")
                    return False
                return await sender.send_image(response.chat_id, image_url, caption=response.content)
            # text and card messages both go through sendMessage; cards use
            # Markdown formatting
            parse_mode = "Markdown" if response.message_type == "card" else None
            return await sender.send_text(
                response.chat_id,
                response.content,
                reply_to_message_id=response.reply_to_message_id,
                parse_mode=parse_mode,
            )

        except Exception as e:
            logger.error(f"Error sending Telegram response: {e}")
            return False

    def generate_session_id(self, message: TenantMessage) -> str:
        """Generate session ID for Telegram message.

        For Telegram, we typically use chat-level sessions.
        """
        tenant = message.tenant_id

        # Use chat ID for session
        return f"{tenant}:telegram:chat:{message.chat_id}"

    async def _find_tenant_by_telegram_token(self, bot_token: str) -> Optional[Tenant]:
        """Find tenant by Telegram bot token."""
        tenants = await self._tenant_store.list_tenants(active_only=True)

        for tenant in tenants:
            telegram_config = tenant.get_channel_config("telegram")
            if telegram_config and telegram_config.api_key == bot_token:
                return tenant

        return None

    def _extract_telegram_message(self, payload: Dict[str, Any], tenant: Tenant) -> TenantMessage:
        """Extract message from Telegram webhook payload."""
        message = payload.get("message", {})
        chat = message.get("chat", {})
        from_user = message.get("from", {})

        # Determine if group chat
        chat_type = chat.get("type", "private")
        is_group_chat = chat_type in ["group", "supergroup", "channel"]

        # Extract content
        text = message.get("text", "")
        if not text:
            # Handle other message types
            if "photo" in message:
                text = "[Photo message]"
            elif "document" in message:
                text = f"[Document: {message['document'].get('file_name', 'unknown')}]"
            else:
                text = f"[{message.get('message_id', 'unknown')} type message]"

        return TenantMessage(
            tenant_id=tenant.tenant_id,
            channel_type="telegram",
            user_id=str(from_user.get("id", "")),
            chat_id=str(chat.get("id", "")),
            message_id=str(message.get("message_id", "")),
            content=text,
            metadata={
                "chat_type": chat_type,
                "from_username": from_user.get("username", ""),
                "from_first_name": from_user.get("first_name", ""),
                "chat_title": chat.get("title", ""),
            },
            timestamp=datetime.utcnow(),
            is_group_chat=is_group_chat,
            original_payload=payload,
        )


class MessageDeduplicator:
    """Message deduplication for multi-tenant IM processing.

    Prevents duplicate processing of the same message across multiple
    tenant instances or webhook retries.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize message deduplicator.

        Args:
            redis_url: Redis URL for deduplication storage
        """
        try:
            import redis.asyncio as redis
        except ImportError:
            raise ImportError("MessageDeduplicator requires 'redis' package. "
                              "Install with: pip install redis")

        self._redis_url = redis_url
        self._redis: Optional[redis.Redis] = None
        # ponytail: in-memory fallback when Redis is unavailable — per-process only,
        # switch to a real Redis if cross-node dedup matters
        self._local_seen: dict[str, float] = {}
        self._redis_failed = False

    async def _get_redis(self):
        """Lazy initialization of Redis connection."""
        if self._redis is None:
            import redis.asyncio as redis

            self._redis = await redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    async def is_duplicate(self, message: TenantMessage) -> bool:
        """Check if message is a duplicate.

        Args:
            message: Message to check

        Returns:
            True if duplicate, False if new message
        """
        dedup_key = self._generate_dedup_key(message)

        # Fall back to in-memory dedup if Redis is unreachable or incompatible
        if not self._redis_failed:
            try:
                redis_client = await self._get_redis()

                # Use Redis SETNX for atomic deduplication
                exists = await redis_client.set(dedup_key, "1", ex=3600, nx=True)

                # If exists is False, key already exists (duplicate)
                is_dup = exists is not True
            except Exception as e:
                logger.warning(f"Redis dedup unavailable, falling back to in-memory: {e}")
                self._redis_failed = True
                is_dup = self._local_is_duplicate(dedup_key)
        else:
            is_dup = self._local_is_duplicate(dedup_key)

        if is_dup:
            logger.debug(f"Duplicate message detected: {dedup_key}")
        else:
            logger.debug(f"New message processed: {dedup_key}")

        return is_dup

    def _local_is_duplicate(self, dedup_key: str) -> bool:
        """In-memory deduplication fallback with 1-hour TTL."""
        import time

        now = time.time()
        # Purge expired entries
        self._local_seen = {k: t for k, t in self._local_seen.items() if now - t < 3600}

        if dedup_key in self._local_seen:
            return True

        self._local_seen[dedup_key] = now
        return False

    def _generate_dedup_key(self, message: TenantMessage) -> str:
        """Generate deduplication key for message."""
        # Key format: dedup:{tenant_id}:{channel_type}:{message_id}
        return f"dedup:{message.tenant_id}:{message.channel_type}:{message.message_id}"

    async def cleanup_old_messages(self, older_than_hours: int = 24):
        """Clean up old deduplication keys.

        Args:
            older_than_hours: Remove keys older than this many hours
        """
        redis_client = await self._get_redis()

        # Redis handles TTL automatically, but we can force cleanup
        pattern = "dedup:*"
        count = 0

        async for key in redis_client.scan_iter(match=pattern):
            ttl = await redis_client.ttl(key)
            if ttl == -1:  # No expiration set
                await redis_client.expire(key, older_than_hours * 3600)
                count += 1

        logger.info(f"Cleaned up {count} old deduplication keys")


class TenantChannelManager:
    """Main manager for multi-tenant IM channel operations.

    Coordinates channel adapters, message deduplication, and tenant routing.
    """

    def __init__(self,
                 tenant_store: TenantStore,
                 redis_url: str = "redis://localhost:6379/0",
                 audit_logger_factory: Optional[Callable[[str], TenantAuditLogger]] = None):
        """Initialize tenant channel manager.

        Args:
            tenant_store: Tenant storage backend
            redis_url: Redis URL for deduplication
            audit_logger_factory: Optional factory creating a per-tenant
                :class:`~trpc_agent_sdk.tenants.TenantAuditLogger`; when set,
                webhook accept/reject/duplicate decisions are audited.
        """
        self._tenant_store = tenant_store
        self._deduplicator = MessageDeduplicator(redis_url)
        self._rate_limiter = RateLimiter(redis_url)
        self._audit_logger_factory = audit_logger_factory

        # Register channel adapters
        self._adapters = {
            "wecom": WeComTenantAdapter(tenant_store),
            "telegram": TelegramTenantAdapter(tenant_store),
        }

    async def _audit(self,
                     tenant_id: str,
                     decision: str,
                     message: Optional[TenantMessage],
                     details: Optional[Dict[str, Any]] = None,
                     error_type: str = "") -> None:
        """Write an audit entry when an audit logger factory is configured."""
        if self._audit_logger_factory is None:
            return
        audit_logger = self._audit_logger_factory(tenant_id)
        await audit_logger.log_event(
            decision=decision,
            channel=message.channel_type if message else "unknown",
            user_id=message.user_id if message else "",
            session_id=self.generate_session_id(message) if message else "",
            error_type=error_type,
            details=details or {},
        )

    async def handle_webhook(self, channel_type: str, payload: Dict[str, Any],
                             headers: Dict[str, Any]) -> Optional[TenantMessage]:
        """Handle incoming webhook from any supported IM platform.

        Args:
            channel_type: Type of IM channel
            payload: Webhook payload
            headers: HTTP headers

        Returns:
            TenantMessage if successfully processed, None otherwise
        """
        adapter = self._adapters.get(channel_type)
        if not adapter:
            logger.warning(f"Unsupported channel type: {channel_type}")
            return None

        # Process webhook through adapter
        message = await adapter.handle_webhook(channel_type, payload, headers)
        if not message:
            await self._audit(f"unknown:{channel_type}", DECISION_IM_REJECTED, None,
                              {"reason": "authentication or signature failed"})
            return None

        # Check for duplicate messages
        is_duplicate = await self._deduplicator.is_duplicate(message)
        if is_duplicate:
            logger.info(f"Duplicate message filtered: {message.message_id}")
            await self._audit(message.tenant_id, DECISION_IM_DUPLICATE, message)
            return None

        await self._audit(message.tenant_id, DECISION_IM_RECEIVED, message)
        return message

    async def send_response(self, response: TenantResponse) -> bool:
        """Send response to appropriate IM platform.

        Args:
            response: Response to send

        Returns:
            True if sent successfully, False otherwise
        """
        adapter = self._adapters.get(response.channel_type)
        if not adapter:
            logger.warning(f"Unsupported channel type: {response.channel_type}")
            return False

        # Rate limit before sending
        tenant = await self._tenant_store.get_tenant(response.tenant_id)
        limit = 60
        if tenant:
            channel_config = tenant.get_channel_config(response.channel_type)
            if channel_config:
                limit = channel_config.rate_limit_per_minute
        allowed = await self._rate_limiter.acquire(response.tenant_id, response.channel_type, limit)
        if not allowed:
            logger.warning(f"Rate limited for tenant {response.tenant_id} on {response.channel_type}")
            return False

        return await adapter.send_response(response)

    async def dispatch_agent_events(self, message: TenantMessage, events: AsyncIterator[Event]) -> List[bool]:
        """Consume agent events for an IM message and deliver the reply.

        Aggregates final text from the event stream, splits it into
        platform-size chunks and sends each chunk through
        :meth:`send_response` with retries. Delivery results are audited.

        Args:
            message: The incoming IM message being replied to.
            events: Async iterator of events from ``TenantRunner.run_async``.

        Returns:
            Per-chunk delivery results (True = delivered).
        """
        text_parts: List[str] = []
        async for event in events:
            if event.partial:
                continue
            event_text = event.get_text()
            if event_text:
                text_parts.append(event_text)

        reply_text = "".join(text_parts).strip()
        if not reply_text:
            logger.warning(f"Agent produced no reply for message {message.message_id}")
            return []

        chunks = MessageChunker.split_text(reply_text, MessageChunker.limit_for(message.channel_type))
        results: List[bool] = []
        for chunk in chunks:
            response = TenantResponse(
                tenant_id=message.tenant_id,
                channel_type=message.channel_type,
                user_id=message.user_id,
                chat_id=message.chat_id,
                content=chunk,
                reply_to_message_id=message.message_id,
            )
            delivered = await send_with_retry(lambda r=response: self.send_response(r))
            results.append(delivered)
            await self._audit(
                message.tenant_id,
                DECISION_IM_REPLIED if delivered else DECISION_IM_REPLY_FAILED,
                message,
                {} if delivered else {"chunk": chunk[:100]},
                error_type="" if delivered else "IMDeliveryError",
            )
        return results

    async def run_and_reply(self, runner, message: TenantMessage, run_config: Optional[Any] = None) -> List[bool]:
        """Run the tenant agent for an IM message and deliver its reply.

        This is the full inbound loop: IM message → agent execution →
        agent events → IM reply (chunked, rate-limited, audited).

        Args:
            runner: A :class:`~trpc_agent_sdk.tenants.TenantRunner` (or any
                object exposing ``run_async`` with keyword-only args).
            message: The incoming IM message.
            run_config: Optional run configuration.

        Returns:
            Per-chunk delivery results (True = delivered).
        """
        session_id = self.generate_session_id(message)
        new_message = Content(parts=[Part.from_text(text=message.content)])
        events = runner.run_async(
            user_id=message.user_id,
            session_id=session_id,
            new_message=new_message,
            run_config=run_config or RunConfig(),
        )
        return await self.dispatch_agent_events(message, events)

    def generate_session_id(self, message: TenantMessage) -> str:
        """Generate session ID using appropriate adapter.

        Args:
            message: Message to generate session ID for

        Returns:
            Session ID for this conversation
        """
        adapter = self._adapters.get(message.channel_type)
        if not adapter:
            logger.warning(f"Unsupported channel type: {message.channel_type}")
            return f"{message.tenant_id}:unknown:{message.chat_id}"

        return adapter.generate_session_id(message)

    def register_adapter(self, channel_type: str, adapter: TenantChannelAdapter):
        """Register a custom channel adapter.

        Args:
            channel_type: Channel type identifier
            adapter: Adapter instance
        """
        self._adapters[channel_type] = adapter
        logger.info(f"Registered custom adapter for {channel_type}")

    def get_supported_channels(self) -> List[str]:
        """Get list of supported channel types.

        Returns:
            List of channel type identifiers
        """
        return list(self._adapters.keys())
