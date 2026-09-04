# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant routing and identification.

This module provides tenant identification and routing capabilities,
supporting multiple strategies for extracting tenant information
from incoming requests and mapping them to the appropriate tenant
configuration.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
import hashlib
import hmac

from trpc_agent_sdk.log import logger

from ._tenant_store import TenantStore
from ._tenant_model import Tenant


class TenantIdentificationStrategy(Enum):
    """Strategies for identifying tenants from requests."""

    HTTP_HEADER = "http_header"  # X-Tenant-ID header
    API_KEY = "api_key"  # API key mapping
    SUBDOMAIN = "subdomain"  # Subdomain-based identification
    PATH_PREFIX = "path_prefix"  # URL path prefix
    IM_WEBHOOK_TOKEN = "im_webhook_token"  # IM platform webhook token
    CUSTOM = "custom"  # Custom identification function


class TenantExtractionError(Exception):
    """Exception raised when tenant identification fails."""

    def __init__(self, message: str, strategy: str, details: Optional[Dict] = None):
        """Initialize tenant extraction error.

        Args:
            message: Error message
            strategy: Identification strategy that failed
            details: Additional error details
        """
        super().__init__(message)
        self.strategy = strategy
        self.details = details or {}


class TenantIdentifier(ABC):
    """Abstract base class for tenant identification strategies."""

    @abstractmethod
    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from request.

        Args:
            request: Incoming request (HTTP, webhook, etc.)
            metadata: Additional metadata for identification

        Returns:
            Tenant ID if found, None otherwise

        Raises:
            TenantExtractionError: If extraction fails with clear reason
        """
        pass

    @abstractmethod
    def get_strategy(self) -> TenantIdentificationStrategy:
        """Get the identification strategy type."""
        pass


class HttpHeaderIdentifier(TenantIdentifier):
    """Identify tenant from HTTP header."""

    def __init__(self, header_name: str = "X-Tenant-ID"):
        """Initialize HTTP header identifier.

        Args:
            header_name: Header name containing tenant ID
        """
        self._header_name = header_name

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from HTTP header."""
        try:
            # Handle different request types
            if hasattr(request, "headers"):
                # FastAPI/Starlette request
                tenant_id = request.headers.get(self._header_name)
            elif isinstance(request, dict):
                # Raw dict-based request
                tenant_id = request.get("headers", {}).get(self._header_name)
            elif metadata and "headers" in metadata:
                tenant_id = metadata["headers"].get(self._header_name)
            else:
                return None

            if tenant_id:
                logger.debug(f"Extracted tenant ID from header: {tenant_id}")
                return tenant_id.strip()

            return None
        except Exception as e:
            logger.error(f"Failed to extract tenant ID from header: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return HTTP header strategy."""
        return TenantIdentificationStrategy.HTTP_HEADER


class ApiKeyIdentifier(TenantIdentifier):
    """Identify tenant from API key mapping."""

    def __init__(self, api_key_tenant_mapping: Dict[str, str]):
        """Initialize API key identifier.

        Args:
            api_key_tenant_mapping: Dictionary mapping API keys to tenant IDs
        """
        self._api_key_mapping = api_key_tenant_mapping

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from API key."""
        try:
            # Extract API key from different locations
            api_key = None

            if hasattr(request, "headers"):
                # Authorization header: Bearer <api_key>
                auth_header = request.headers.get("Authorization", "")
                if auth_header.startswith("Bearer "):
                    api_key = auth_header[7:]  # Remove "Bearer " prefix
                else:
                    api_key = request.headers.get("X-API-Key")
            elif isinstance(request, dict):
                api_key = request.get("api_key") or request.get("headers", {}).get("X-API-Key")
            elif metadata:
                api_key = metadata.get("api_key")

            if api_key and api_key in self._api_key_mapping:
                tenant_id = self._api_key_mapping[api_key]
                logger.debug(f"Extracted tenant ID from API key: {tenant_id}")
                return tenant_id

            return None
        except Exception as e:
            logger.error(f"Failed to extract tenant ID from API key: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return API key strategy."""
        return TenantIdentificationStrategy.API_KEY


class SubdomainIdentifier(TenantIdentifier):
    """Identify tenant from subdomain."""

    def __init__(self, base_domain: str = ""):
        """Initialize subdomain identifier.

        Args:
            base_domain: Base domain to strip from host (e.g., "example.com")
        """
        self._base_domain = base_domain

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from subdomain."""
        try:
            host = None

            if hasattr(request, "headers"):
                host = request.headers.get("Host", "")
            elif isinstance(request, dict):
                host = request.get("host") or request.get("headers", {}).get("Host")
            elif metadata:
                host = metadata.get("host")

            if not host:
                return None

            # Remove port if present
            host = host.split(":")[0]

            # Remove base domain if configured
            if self._base_domain and host.endswith(self._base_domain):
                host = host[:-len(self._base_domain)].rstrip(".")

            # Extract subdomain (first part before dot)
            subdomain = host.split(".")[0] if "." in host else host

            if subdomain and subdomain != "www":
                logger.debug(f"Extracted tenant ID from subdomain: {subdomain}")
                return subdomain

            return None
        except Exception as e:
            logger.error(f"Failed to extract tenant ID from subdomain: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return subdomain strategy."""
        return TenantIdentificationStrategy.SUBDOMAIN


class PathPrefixIdentifier(TenantIdentifier):
    """Identify tenant from URL path prefix."""

    def __init__(self, prefix: str = "/t/"):
        """Initialize path prefix identifier.

        Args:
            prefix: Path prefix indicating tenant (e.g., "/t/")
        """
        self._prefix = prefix

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from path prefix."""
        try:
            path = None

            if hasattr(request, "path"):
                path = request.path
            elif isinstance(request, dict):
                path = request.get("path") or request.get("url")
            elif metadata:
                path = metadata.get("path")

            if not path or not path.startswith(self._prefix):
                return None

            # Extract tenant ID from path: /t/tenant123/... -> tenant123
            tenant_id = path[len(self._prefix):].split("/")[0]

            if tenant_id:
                logger.debug(f"Extracted tenant ID from path: {tenant_id}")
                return tenant_id

            return None
        except Exception as e:
            logger.error(f"Failed to extract tenant ID from path: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return path prefix strategy."""
        return TenantIdentificationStrategy.PATH_PREFIX


class ImWebhookTokenIdentifier(TenantIdentifier):
    """Identify tenant from IM webhook token."""

    def __init__(self, token_tenant_mapping: Dict[str, str]):
        """Initialize IM webhook token identifier.

        Args:
            token_tenant_mapping: Dictionary mapping webhook tokens to tenant IDs
        """
        self._token_mapping = token_tenant_mapping

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID from IM webhook token."""
        try:
            # Extract token from different locations depending on IM platform
            token = None

            # WeCom style: query parameter
            if isinstance(request, dict):
                token = (request.get("token") or request.get("query_params", {}).get("token")
                         or request.get("webhook_token"))
            elif metadata:
                token = metadata.get("token") or metadata.get("webhook_token")

            if token and token in self._token_mapping:
                tenant_id = self._token_mapping[token]
                logger.debug(f"Extracted tenant ID from webhook token: {tenant_id}")
                return tenant_id

            return None
        except Exception as e:
            logger.error(f"Failed to extract tenant ID from webhook token: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return IM webhook token strategy."""
        return TenantIdentificationStrategy.IM_WEBHOOK_TOKEN


class CustomIdentifier(TenantIdentifier):
    """Custom tenant identification using user-provided function."""

    def __init__(self, identifier_func: Callable[[Any, Optional[Dict]], Optional[str]]):
        """Initialize custom identifier.

        Args:
            identifier_func: Function that takes request and metadata, returns tenant ID
        """
        self._identifier_func = identifier_func

    async def extract_tenant_id(self, request: Any, metadata: Optional[Dict] = None) -> Optional[str]:
        """Extract tenant ID using custom function."""
        try:
            tenant_id = self._identifier_func(request, metadata)
            if tenant_id:
                logger.debug(f"Extracted tenant ID using custom function: {tenant_id}")
            return tenant_id
        except Exception as e:
            logger.error(f"Custom tenant identification failed: {e}")
            return None

    def get_strategy(self) -> TenantIdentificationStrategy:
        """Return custom strategy."""
        return TenantIdentificationStrategy.CUSTOM


class TenantRouter:
    """Main tenant router coordinating identification and tenant loading."""

    def __init__(self, tenant_store: TenantStore):
        """Initialize tenant router.

        Args:
            tenant_store: Tenant storage backend
        """
        self._tenant_store = tenant_store
        self._identifiers: List[TenantIdentifier] = []
        self._fallback_tenant_id: Optional[str] = None

    def add_identifier(self, identifier: TenantIdentifier) -> "TenantRouter":
        """Add an identification strategy.

        Args:
            identifier: Tenant identifier to add

        Returns:
            Self for method chaining
        """
        self._identifiers.append(identifier)
        return self

    def set_fallback_tenant(self, tenant_id: str) -> "TenantRouter":
        """Set fallback tenant for when identification fails.

        Args:
            tenant_id: Default tenant ID to use

        Returns:
            Self for method chaining
        """
        self._fallback_tenant_id = tenant_id
        return self

    async def route_request(self, request: Any, metadata: Optional[Dict] = None) -> Optional[Tenant]:
        """Route request to appropriate tenant.

        Args:
            request: Incoming request
            metadata: Additional metadata for identification

        Returns:
            Tenant object if found, None otherwise

        Raises:
            TenantExtractionError: If all identification strategies fail
        """
        # Try each identifier in order
        for identifier in self._identifiers:
            try:
                tenant_id = await identifier.extract_tenant_id(request, metadata)
                if tenant_id:
                    # Load tenant from storage
                    tenant = await self._tenant_store.get_tenant(tenant_id)
                    if tenant:
                        logger.info(f"Successfully routed request to tenant: {tenant_id}")
                        return tenant
                    else:
                        logger.warning(f"Tenant ID {tenant_id} extracted but tenant not found in storage")
            except Exception as e:
                logger.error(f"Identification strategy {identifier.get_strategy()} failed: {e}")
                continue

        # Try fallback tenant
        if self._fallback_tenant_id:
            logger.info(f"Using fallback tenant: {self._fallback_tenant_id}")
            return await self._tenant_store.get_tenant(self._fallback_tenant_id)

        # All strategies failed
        logger.error("Failed to identify tenant from request")
        return None

    async def verify_webhook_signature(
        self,
        tenant: Tenant,
        payload: bytes,
        signature: str,
        secret: Optional[str] = None,
        timestamp: Optional[str] = None,
        nonce: Optional[str] = None,
    ) -> bool:
        """Verify webhook signature for security.

        Supports two conventions:
        - WeCom-style: ``sha1(sorted([secret, timestamp, nonce]))`` when
          ``timestamp`` and ``nonce`` are provided.
        - Generic: HMAC-SHA256 over the raw payload otherwise.

        Args:
            tenant: Tenant object
            payload: Raw webhook payload
            signature: Received signature
            secret: Secret to use (overrides tenant config if provided)
            timestamp: Webhook timestamp (enables WeCom-style verification)
            nonce: Webhook nonce (enables WeCom-style verification)

        Returns:
            True if signature is valid, False otherwise
        """
        if not signature:
            return False

        # Get secret from tenant config or override
        webhook_config = tenant.get_channel_config("webhook") or {}
        webhook_secret = secret or webhook_config.get("webhook_secret")
        if not webhook_secret:
            logger.warning("No webhook secret configured for signature verification")
            return False

        try:
            if timestamp is not None and nonce is not None:
                # WeCom-style: sha1 over sorted [secret, timestamp, nonce]
                expected_signature = hashlib.sha1("".join(sorted([webhook_secret, timestamp,
                                                                  nonce])).encode()).hexdigest()
            else:
                # Generic: HMAC-SHA256 over the raw payload
                expected_signature = hmac.new(webhook_secret.encode(), payload, hashlib.sha256).hexdigest()

            # Remove potential hash prefix (e.g., "sha256=")
            if signature.startswith("sha256="):
                signature = signature[7:]

            # Constant-time comparison to prevent timing attacks
            return hmac.compare_digest(expected_signature, signature)
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            return False

    def get_supported_strategies(self) -> List[TenantIdentificationStrategy]:
        """Get list of supported identification strategies.

        Returns:
            List of strategy types currently configured
        """
        return [identifier.get_strategy() for identifier in self._identifiers]
