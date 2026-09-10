# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""WeCom (企业微信) self-built-app callback encryption/decryption.

Implements the real WeCom callback protocol used by the admin console's
"接收消息" configuration:

- ``msg_signature`` verification: ``sha1("".join(sorted([token, timestamp,
  nonce, encrypt])))`` compared with :func:`hmac.compare_digest`.
- AES-256-CBC decryption with ``key = base64.b64decode(EncodingAESKey + "=")``
  and ``iv = key[:16]`` (NOT the first ciphertext block), PKCS#7 unpadding.
- Plaintext envelope: ``random(16) + msg_len(4, big-endian) + msg + receiveid``
  where ``receiveid`` is the corp_id for self-built apps.
- URL verification: the GET ``echostr`` parameter is decrypted and the plain
  text must be returned verbatim in the HTTP response.

The ``cryptography`` package is an optional dependency (extra ``wecom``);
importing this module never fails without it, but constructing
:class:`WeComCrypto` raises an :class:`ImportError` naming the extra.
"""

import base64
import hashlib
import hmac
import os
import struct
from typing import Tuple

from trpc_agent_sdk.log import logger

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    _CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on the environment
    _CRYPTO_AVAILABLE = False

_ENCODING_AES_KEY_LENGTH = 43


class WeComCryptoError(Exception):
    """Base error for WeCom callback crypto operations."""


class WeComSignatureError(WeComCryptoError):
    """msg_signature verification failed."""


class WeComDecryptError(WeComCryptoError):
    """AES decryption failed (bad padding, malformed envelope or receiveid
    mismatch)."""


def _new_cipher(key: bytes, iv: bytes):
    return Cipher(algorithms.AES(key), modes.CBC(iv))


class WeComCrypto:
    """Encryptor/decryptor for one WeCom callback configuration.

    Args:
        token: Callback Token from the WeCom admin console (participates in
            the ``msg_signature`` computation).
        encoding_aes_key: 43-character EncodingAESKey (base64url alphabet,
            no padding character) from the admin console.
        receive_id: Expected corp_id; when set, the value embedded in the
            decrypted plaintext is verified against it (tamper protection).

    Raises:
        ImportError: ``cryptography`` is not installed (install the
            ``wecom`` extra).
        WeComCryptoError: the AES key is not 43 characters or does not
            decode to a 32-byte key.
    """

    def __init__(self, token: str, encoding_aes_key: str, receive_id: str = ""):
        if not _CRYPTO_AVAILABLE:
            raise ImportError("WeCom callback protocol requires 'cryptography'. "
                              "Install with: pip install trpc-agent-py[wecom]")

        if not token or not encoding_aes_key:
            raise WeComCryptoError("token and encoding_aes_key are required")
        if len(encoding_aes_key) != _ENCODING_AES_KEY_LENGTH:
            raise WeComCryptoError(f"EncodingAESKey must be exactly {_ENCODING_AES_KEY_LENGTH} characters, "
                                   f"got {len(encoding_aes_key)}")
        try:
            key = base64.b64decode(encoding_aes_key + "=")
        except Exception as e:
            raise WeComCryptoError(f"EncodingAESKey is not valid base64url: {e}") from e
        if len(key) != 32:
            raise WeComCryptoError(f"EncodingAESKey must decode to a 32-byte AES key, got {len(key)} bytes")

        self._token = token
        self._key = key
        self._iv = key[:16]
        self._receive_id = receive_id or ""

    @staticmethod
    def signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
        """Compute the WeCom ``msg_signature`` over the sorted parameters."""
        return hashlib.sha1("".join(sorted([token, timestamp, nonce, encrypt])).encode("utf-8")).hexdigest()

    def verify_signature(self, msg_signature: str, timestamp: str, nonce: str, encrypt: str) -> bool:
        """Verify ``msg_signature`` in constant time.

        Args:
            msg_signature: Signature from the callback request.
            timestamp: Query parameter ``timestamp``.
            nonce: Query parameter ``nonce``.
            encrypt: The ``Encrypt`` ciphertext (base64 string as sent).

        Returns:
            True when the signature matches.
        """
        expected = self.signature(self._token, timestamp, nonce, encrypt)
        return hmac.compare_digest(str(msg_signature or ""), expected)

    def decrypt(self, encrypt_b64: str) -> str:
        """Verify-free decryption of an ``Encrypt`` value.

        Args:
            encrypt_b64: Base64 ciphertext from the callback.

        Returns:
            The inner ``msg`` XML string. When ``receive_id`` was configured
            the embedded receiveid is verified against it.

        Raises:
            WeComDecryptError: on base64/AES failures, bad PKCS#7 padding,
                malformed envelope, or receiveid mismatch.
        """
        msg, receiveid = self.decrypt_with_receiveid(encrypt_b64)
        if self._receive_id and receiveid != self._receive_id:
            raise WeComDecryptError(f"receiveid mismatch: got '{receiveid}', expected '{self._receive_id}'")
        return msg

    def decrypt_with_receiveid(self, encrypt_b64: str) -> Tuple[str, str]:
        """Decrypt an ``Encrypt`` value and also return the receiveid.

        Needed for URL verification, where no expected receive_id exists.

        Raises:
            WeComDecryptError: on any decryption/envelope failure.
        """
        try:
            ciphertext = base64.b64decode(encrypt_b64)
        except Exception as e:
            raise WeComDecryptError(f"Encrypt value is not valid base64: {e}") from e

        if len(ciphertext) == 0 or len(ciphertext) % 16 != 0:
            raise WeComDecryptError(f"Ciphertext length {len(ciphertext)} is not a positive multiple of 16")

        decryptor = _new_cipher(self._key, self._iv).decryptor()
        try:
            plain = decryptor.update(ciphertext) + decryptor.finalize()
        except Exception as e:
            raise WeComDecryptError(f"AES decryption failed: {e}") from e

        # PKCS#7 unpad.
        pad_len = plain[-1]
        if not 1 <= pad_len <= 16 or plain[-pad_len:] != bytes([pad_len]) * pad_len:
            raise WeComDecryptError("Invalid PKCS#7 padding")

        plain = plain[:-pad_len]
        if len(plain) < 20:
            raise WeComDecryptError("Decrypted plaintext too short to contain the envelope")

        # Envelope: random(16) + msg_len(4, network byte order) + msg + receiveid.
        msg_len = struct.unpack(">I", plain[16:20])[0]
        if 20 + msg_len > len(plain):
            raise WeComDecryptError(f"Envelope msg_len {msg_len} exceeds payload size {len(plain) - 20}")

        msg = plain[20:20 + msg_len]
        receiveid = plain[20 + msg_len:]
        return msg.decode("utf-8"), receiveid.decode("utf-8")

    def verify_url(self, msg_signature: str, timestamp: str, nonce: str, echostr: str) -> str:
        """Handle the GET callback-URL verification request.

        Args:
            msg_signature: Query parameter ``msg_signature``.
            timestamp: Query parameter ``timestamp``.
            nonce: Query parameter ``nonce``.
            echostr: Query parameter ``echostr`` (encrypted echo).

        Returns:
            The decrypted echo plain text to return verbatim in the HTTP
            response body.

        Raises:
            WeComSignatureError: signature verification failed.
            WeComDecryptError: decryption failed.
        """
        if not self.verify_signature(msg_signature, timestamp, nonce, echostr):
            raise WeComSignatureError("URL verification signature mismatch")
        echo, _ = self.decrypt_with_receiveid(echostr)
        return echo

    def encrypt(self, plain_xml: str, timestamp: str, nonce: str, receive_id: str = "") -> Tuple[str, str]:
        """Encrypt a reply message (inverse of :meth:`decrypt`).

        Mainly exists for round-trip tests and future encrypted passive
        replies.

        Args:
            plain_xml: The ``msg`` XML (or any plain text) to encrypt.
            timestamp: Signature timestamp to bind into ``msg_signature``.
            nonce: Signature nonce to bind into ``msg_signature``.
            receive_id: Receiveid embedded in the envelope; defaults to the
                configured ``receive_id``.

        Returns:
            ``(encrypt_b64, msg_signature)`` ready for an encrypted reply.
        """
        receiveid = (receive_id or self._receive_id).encode("utf-8")
        msg = plain_xml.encode("utf-8")
        # Envelope: random(16) + msg_len(4, network byte order) + msg + receiveid.
        envelope = os.urandom(16) + struct.pack(">I", len(msg)) + msg + receiveid
        # PKCS#7 pad to the AES block size.
        pad_len = 16 - (len(envelope) % 16)
        envelope += bytes([pad_len]) * pad_len

        encryptor = _new_cipher(self._key, self._iv).encryptor()
        ciphertext = encryptor.update(envelope) + encryptor.finalize()
        encrypt_b64 = base64.b64encode(ciphertext).decode("ascii")
        return encrypt_b64, self.signature(self._token, timestamp, nonce, encrypt_b64)


def parse_callback_xml(xml_body: str) -> Tuple[str, str]:
    """Parse the outer callback XML body of a POST message callback.

    Args:
        xml_body: Raw POST body ``<xml><ToUserName/><Encrypt/></xml>``.

    Returns:
        ``(to_user_name, encrypt)`` — the plaintext corp_id and the
        encrypted payload.

    Raises:
        WeComCryptoError: when the body is not the expected callback XML.
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(xml_body)
        encrypt = (root.findtext("Encrypt") or "").strip()
        to_user_name = (root.findtext("ToUserName") or "").strip()
    except ET.ParseError as e:
        raise WeComCryptoError(f"Callback body is not valid XML: {e}") from e
    if not encrypt:
        raise WeComCryptoError("Callback XML is missing the <Encrypt> element")
    return to_user_name, encrypt


__all__ = [
    "WeComCrypto",
    "WeComCryptoError",
    "WeComSignatureError",
    "WeComDecryptError",
    "parse_callback_xml",
]

logger.debug("WeCom callback crypto module loaded (cryptography available: %s)", _CRYPTO_AVAILABLE)
