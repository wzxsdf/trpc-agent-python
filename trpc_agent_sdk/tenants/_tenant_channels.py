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
import hmac
import time

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
from ._tenant_context import TenantContext
from ._tenant_governance import check_im_user_allowed
from ._im_transport import (MessageChunker, RateLimiter, RedisFallbackMixin, TelegramSender, WeComSender,
                            send_with_retry)
from ._tenant_model import Tenant
from ._wecom_crypto import WeComCrypto, WeComCryptoError, parse_callback_xml
from ._tenant_store import TenantStore
from ._tenant_telemetry import TRACER, extract_trace_headers, get_tenant_metrics


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


def _get_header(headers: Dict[str, Any], name: str) -> Optional[str]:
    """Look up an HTTP header case-insensitively.

    Webhook callers may pass raw WSGI/ASGI headers with arbitrary casing;
    the previous exact-match ``headers.get(...)`` silently missed them.
    """
    if not headers:
        return None
    lowered = name.lower()
    for key, value in headers.items():
        if str(key).lower() == lowered:
            return value
    return None


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


def _resolve_session_strategy(tenant: Optional[Tenant], channel_type: str) -> str:
    """Return the configured session strategy for a channel ("chat"/"user"/"group")."""
    config = tenant.get_channel_config(channel_type) if tenant else None
    strategy = getattr(config, "session_strategy", None) or "chat"
    return strategy if strategy in ("chat", "user", "group") else "chat"


def _strategy_session_id(message: TenantMessage, channel_type: str) -> str:
    """Build a session id honoring the tenant's session_strategy.

    Extractors store the resolved strategy in ``message.metadata`` so this
    stays synchronous (tenant lookup happens during webhook handling).
    """
    strategy = message.metadata.get("session_strategy", "chat")
    if strategy == "user":
        return f"{message.tenant_id}:{channel_type}:user:{message.user_id}"
    if strategy == "group" and message.is_group_chat:
        return f"{message.tenant_id}:{channel_type}:group:{message.chat_id}"
    return f"{message.tenant_id}:{channel_type}:chat:{message.chat_id}"


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
            token = payload.get("token") or _get_header(headers, "X-WeCom-Token")

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
                agent_id=wecom_config.agent_id or wecom_config.webhook_token or "1",
                http_client=self._http_client,
            )
            if response.message_type == "card":
                # Card responses are rendered as WeCom markdown messages.
                return await sender.send_markdown(response.user_id, response.content)
            return await sender.send_text(response.user_id, response.content)

        except Exception as e:
            logger.error(f"Error sending WeCom response: {e}")
            return False

    def generate_session_id(self, message: TenantMessage) -> str:
        """Generate session ID for WeCom message.

        Honors the tenant's ``ChannelConfig.session_strategy``
        ("chat" / "user" / "group"); defaults to chat-level sessions.
        """
        return _strategy_session_id(message, "wecom")

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

            # Constant-time comparison to prevent timing attacks
            return hmac.compare_digest(str(signature), calculated_signature)

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
                "session_strategy": _resolve_session_strategy(tenant, "wecom"),
            },
            timestamp=datetime.utcnow(),
            is_group_chat=is_group_chat,
            original_payload=payload,
        )

    # ---- Real WeCom self-built-app callback protocol ----------------------
    # The admin console's "接收消息" feature: GET echostr URL verification and
    # POST encrypted-XML message callbacks (see _wecom_crypto for the wire
    # format). Routed by plaintext ToUserName (corp_id -> ChannelConfig.bot_id).

    async def handle_wecom_callback(self, xml_body: str, query: Dict[str, str]) -> Optional[TenantMessage]:
        """Handle a real WeCom self-built-app message callback (encrypted XML).

        The outer ``<ToUserName>`` (plaintext corp_id) locates candidate
        tenants via ``ChannelConfig.bot_id``; each candidate's Token /
        EncodingAESKey then verifies ``msg_signature`` and decrypts the
        payload. The decrypted embedded receiveid is checked against the
        corp_id (tamper protection), and same-corp multi-app setups are
        disambiguated by ``AgentID`` when ``ChannelConfig.agent_id`` is set.

        Args:
            xml_body: Raw POST body ``<xml><ToUserName/><Encrypt/></xml>``.
            query: Callback query params with ``msg_signature``/``timestamp``/``nonce``.

        Returns:
            TenantMessage, or None when routing or verification failed.
        """
        msg_signature = query.get("msg_signature")
        timestamp = query.get("timestamp")
        nonce = query.get("nonce")
        if not all([msg_signature, timestamp, nonce]):
            logger.warning("WeCom callback missing msg_signature/timestamp/nonce")
            return None

        try:
            to_user_name, encrypt = parse_callback_xml(xml_body)
        except WeComCryptoError as e:
            logger.warning(f"Invalid WeCom callback body: {e}")
            return None

        for tenant in await self._find_tenants_by_corp_id(to_user_name):
            wecom_config = tenant.get_channel_config("wecom")
            try:
                crypto = WeComCrypto(
                    token=wecom_config.webhook_token or "",
                    encoding_aes_key=wecom_config.webhook_secret or "",
                    receive_id=to_user_name,
                )
            except (ImportError, WeComCryptoError) as e:
                # webhook_secret is the EncodingAESKey under the real
                # protocol; a bad value must fail loudly here.
                logger.error(f"WeCom crypto unavailable for tenant {tenant.tenant_id}: {e}")
                return None

            if not crypto.verify_signature(msg_signature, timestamp, nonce, encrypt):
                continue
            try:
                xml_msg = crypto.decrypt(encrypt)
            except WeComCryptoError as e:
                logger.warning(f"WeCom callback decryption failed for tenant {tenant.tenant_id}: {e}")
                continue

            fields = self._parse_callback_message_xml(xml_msg)
            configured_agent_id = wecom_config.agent_id
            if configured_agent_id and fields.get("AgentID") and str(fields["AgentID"]) != str(configured_agent_id):
                continue  # another app of the same corp
            return self._callback_message_from_fields(fields, tenant)

        logger.warning(f"No tenant verified the WeCom callback for corp '{to_user_name}'")
        return None

    async def verify_wecom_url(self, query: Dict[str, str]) -> Optional[str]:
        """Handle the GET callback-URL verification (echostr) request.

        The query carries no corp_id, so tenants with a WeCom channel are
        probed in order; the first one whose Token/EncodingAESKey verifies
        the signature wins.

        Args:
            query: Callback query params with ``msg_signature``/``timestamp``/
                ``nonce``/``echostr``.

        Returns:
            The decrypted echo plain text to return verbatim in the HTTP
            response, or None when verification failed.
        """
        msg_signature = query.get("msg_signature")
        timestamp = query.get("timestamp")
        nonce = query.get("nonce")
        echostr = query.get("echostr")
        if not all([msg_signature, timestamp, nonce, echostr]):
            logger.warning("WeCom URL verification missing query parameters")
            return None

        tenants = await self._tenant_store.list_tenants(active_only=True)
        for tenant in tenants:
            wecom_config = tenant.get_channel_config("wecom")
            if not wecom_config or not (wecom_config.webhook_token and wecom_config.webhook_secret):
                continue
            try:
                crypto = WeComCrypto(wecom_config.webhook_token, wecom_config.webhook_secret)
            except (ImportError, WeComCryptoError) as e:
                logger.error(f"WeCom crypto unavailable for tenant {tenant.tenant_id}: {e}")
                return None
            try:
                return crypto.verify_url(msg_signature, timestamp, nonce, echostr)
            except WeComCryptoError:
                continue

        logger.warning("No tenant verified the WeCom URL verification request")
        return None

    async def _find_tenants_by_corp_id(self, corp_id: str) -> List[Tenant]:
        """Return active tenants whose WeCom ``bot_id`` equals ``corp_id``."""
        result: List[Tenant] = []
        for tenant in await self._tenant_store.list_tenants(active_only=True):
            wecom_config = tenant.get_channel_config("wecom")
            if wecom_config and wecom_config.bot_id == corp_id:
                result.append(tenant)
        return result

    @staticmethod
    def _parse_callback_message_xml(xml_msg: str) -> Dict[str, str]:
        """Flatten a decrypted callback message XML into a str dict."""
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(xml_msg)
        except ET.ParseError as e:
            logger.warning(f"Decrypted WeCom message is not valid XML: {e}")
            return {}
        return {child.tag: (child.text or "").strip() for child in root}

    def _callback_message_from_fields(self, fields: Dict[str, str], tenant: Tenant) -> TenantMessage:
        """Build a TenantMessage from a decrypted WeCom callback message.

        Real-protocol group semantics: app-chat messages carry a ``ChatId``
        (unlike the simulated protocol's ``$``-prefix convention); private
        messages fall back to the sender's userid as the chat id.
        """
        msg_type = fields.get("MsgType", "text")
        from_user = fields.get("FromUserName", "")
        chat_id = fields.get("ChatId", "")
        is_group_chat = bool(chat_id)

        if msg_type == "text":
            content = fields.get("Content", "")
        elif msg_type == "event":
            content = f"[event:{fields.get('Event', 'unknown')}]"
        else:
            content = f"[{msg_type} message]"

        message_id = fields.get("MsgId", "")
        if not message_id:
            # Event callbacks carry no MsgId; derive one from the create time.
            message_id = f"wecom_evt_{fields.get('CreateTime') or int(datetime.utcnow().timestamp())}"

        create_time = fields.get("CreateTime", "")
        if create_time.isdigit():
            timestamp = datetime.utcfromtimestamp(int(create_time))
        else:
            timestamp = datetime.utcnow()

        metadata: Dict[str, Any] = {
            "msg_type": msg_type,
            "to_user_name": fields.get("ToUserName", ""),
            "agent_id": fields.get("AgentID", ""),
            "session_strategy": _resolve_session_strategy(tenant, "wecom"),
        }
        if msg_type == "event":
            metadata["event"] = fields.get("Event", "")
            metadata["event_key"] = fields.get("EventKey", "")

        return TenantMessage(
            tenant_id=tenant.tenant_id,
            channel_type="wecom",
            user_id=from_user,
            chat_id=chat_id or from_user,
            message_id=message_id,
            content=content,
            metadata=metadata,
            timestamp=timestamp,
            is_group_chat=is_group_chat,
            original_payload=fields,
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
            bot_token = _get_header(headers, "X-Telegram-Bot-Token")

            if not bot_token:
                logger.warning("Missing Telegram bot token for tenant identification")
                return None

            # Find tenant by Telegram bot token
            tenant = await self._find_tenant_by_telegram_token(bot_token)
            if not tenant:
                logger.warning("No tenant found for Telegram bot token")
                return None

            # Verify the webhook secret token. Telegram sends the value
            # configured via ``setWebhook(secret_token=...)`` in the
            # ``X-Telegram-Bot-Api-Secret-Token`` header. Verification is
            # enforced when the tenant has a ``webhook_secret`` configured;
            # without it the bot token itself is the only credential, which
            # is transmitted in plaintext and must not be trusted alone.
            telegram_config = tenant.get_channel_config("telegram")
            secret = telegram_config.webhook_secret if telegram_config else None
            if secret:
                provided = _get_header(headers, "X-Telegram-Bot-Api-Secret-Token")
                if not provided or not hmac.compare_digest(provided, secret):
                    logger.warning("Telegram webhook secret token verification failed "
                                   f"for tenant {tenant.tenant_id}")
                    return None
            else:
                logger.warning(
                    "Tenant %s has no Telegram webhook_secret configured; "
                    "skipping secret-token verification", tenant.tenant_id)

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

        Honors the tenant's ``ChannelConfig.session_strategy``
        ("chat" / "user" / "group"); defaults to chat-level sessions.
        """
        return _strategy_session_id(message, "telegram")

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
                "session_strategy": _resolve_session_strategy(tenant, "telegram"),
            },
            timestamp=datetime.utcnow(),
            is_group_chat=is_group_chat,
            original_payload=payload,
        )


class MessageDeduplicator(RedisFallbackMixin):
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
        self._init_fallback_state()
        # In-memory fallback when Redis is unavailable — per-process only,
        # switch to a real Redis if cross-node dedup matters
        self._local_seen: dict[str, float] = {}

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
        if not self._redis_down():
            try:
                redis_client = await self._get_redis()

                # Use Redis SETNX for atomic deduplication
                exists = await redis_client.set(dedup_key, "1", ex=3600, nx=True)

                # If exists is False, key already exists (duplicate)
                is_dup = exists is not True
            except Exception as e:
                logger.warning(f"Redis dedup unavailable, falling back to in-memory: {e}")
                self._mark_redis_failed()
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

        message = await adapter.handle_webhook(channel_type, payload, headers)
        return await self._accept_message(channel_type, message, headers)

    async def handle_wecom_callback(self, xml_body: str, query: Dict[str, str]) -> Optional[TenantMessage]:
        """Handle a real WeCom (企业微信) self-built-app message callback.

        Decrypts and routes the callback via the WeCom adapter, then runs
        the full inbound pipeline (whitelist / dedup / audit). HTTP callers
        should answer ``"success"`` immediately and process the returned
        message in the background (WeCom enforces a 5-second response
        deadline).

        Args:
            xml_body: Raw POST body ``<xml><ToUserName/><Encrypt/></xml>``.
            query: Callback query params with ``msg_signature``/``timestamp``/``nonce``.

        Returns:
            TenantMessage if accepted, None otherwise.
        """
        adapter = self._adapters.get("wecom")
        if not isinstance(adapter, WeComTenantAdapter):
            logger.warning("WeCom callback received but the wecom adapter is not a WeComTenantAdapter")
            return None
        message = await adapter.handle_wecom_callback(xml_body, query)
        return await self._accept_message("wecom", message, {})

    async def verify_wecom_url(self, query: Dict[str, str]) -> Optional[str]:
        """Verify the WeCom callback URL (GET echostr challenge).

        Args:
            query: Query params with ``msg_signature``/``timestamp``/``nonce``/
                ``echostr``.

        Returns:
            The decrypted echo plain text to return verbatim, or None when
            verification failed.
        """
        adapter = self._adapters.get("wecom")
        if not isinstance(adapter, WeComTenantAdapter):
            logger.warning("WeCom URL verification received but the wecom adapter is not a WeComTenantAdapter")
            return None
        return await adapter.verify_wecom_url(query)

    async def _accept_message(self, channel_type: str, message: Optional[TenantMessage],
                              headers: Dict[str, Any]) -> Optional[TenantMessage]:
        """Run the shared inbound pipeline (span / whitelist / dedup / audit).

        Args:
            channel_type: IM channel the message arrived on.
            message: Message produced by the channel adapter (None when the
                adapter rejected it).
            headers: HTTP headers of the inbound request.

        Returns:
            The accepted message, or None when any stage rejected it.
        """
        # Root span for the inbound IM request; continues an upstream trace
        # when the platform/gateway forwarded a W3C traceparent header.
        with extract_trace_headers(headers), TRACER.start_as_current_span("tenant.im.callback") as span:
            span.set_attribute("tenant.channel", channel_type)
            if not message:
                span.set_attribute("tenant.accepted", False)
                await self._audit(f"unknown:{channel_type}", DECISION_IM_REJECTED, None,
                                  {"reason": "authentication or signature failed"})
                return None

            span.set_attribute("tenant.id", message.tenant_id)
            span.set_attribute("tenant.accepted", True)
            span.set_attribute("tenant.message_id", message.message_id)

            # Enforce the tenant's IM user whitelist (ChannelConfig.allowed_users).
            tenant = await self._tenant_store.get_tenant(message.tenant_id)
            if tenant is None:
                logger.warning(f"Tenant {message.tenant_id} vanished before user check")
                span.set_attribute("tenant.accepted", False)
                await self._audit(message.tenant_id, DECISION_IM_REJECTED, message, {"reason": "tenant not found"})
                return None
            if not check_im_user_allowed(TenantContext(tenant), message.channel_type, message.user_id):
                logger.warning(f"IM user {message.user_id} not allowed for tenant {message.tenant_id}")
                span.set_attribute("tenant.accepted", False)
                await self._audit(message.tenant_id, DECISION_IM_REJECTED, message,
                                  {"reason": "user not in allowed_users"})
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
        metrics = get_tenant_metrics()
        with TRACER.start_as_current_span("tenant.im.reply") as span:
            span.set_attribute("tenant.id", message.tenant_id)
            span.set_attribute("tenant.channel", message.channel_type)
            span.set_attribute("tenant.message_id", message.message_id)
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
                metrics.incr(
                    "tenant_im_reply_total", {
                        "tenant_id": message.tenant_id,
                        "channel": message.channel_type,
                        "result": "success" if delivered else "failed",
                    })
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
