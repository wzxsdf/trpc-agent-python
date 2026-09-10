# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the WeCom callback crypto (AES-256-CBC + msg_signature)."""

import base64
import hashlib
import os
import struct

import pytest

pytest.importorskip("cryptography")

from trpc_agent_sdk.tenants._wecom_crypto import (  # noqa: E402
    _CRYPTO_AVAILABLE, WeComCrypto, WeComCryptoError, WeComDecryptError, WeComSignatureError,
)

# 43-char base64url key (decodes to 32 bytes).
AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
TOKEN = "callback_token_123"
CORP_ID = "ww1234567890abcdef"


def make_crypto(receive_id: str = CORP_ID) -> WeComCrypto:
    return WeComCrypto(token=TOKEN, encoding_aes_key=AES_KEY, receive_id=receive_id)


def manual_encrypt(crypto: WeComCrypto, xml_msg: str, receiveid: str) -> str:
    """Build an encrypted payload without using WeComCrypto.encrypt."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    msg = xml_msg.encode("utf-8")
    envelope = os.urandom(16) + struct.pack(">I", len(msg)) + msg + receiveid.encode("utf-8")
    pad_len = 16 - (len(envelope) % 16)
    envelope += bytes([pad_len]) * pad_len
    encryptor = Cipher(algorithms.AES(crypto._key), modes.CBC(crypto._iv)).encryptor()
    return base64.b64encode(encryptor.update(envelope) + encryptor.finalize()).decode("ascii")


class TestWeComCryptoConstruction:

    def test_invalid_aes_key_length_rejected(self):
        with pytest.raises(WeComCryptoError, match="43"):
            WeComCrypto(TOKEN, AES_KEY[:-1])
        with pytest.raises(WeComCryptoError, match="43"):
            WeComCrypto(TOKEN, AES_KEY + "x")

    def test_empty_credentials_rejected(self):
        with pytest.raises(WeComCryptoError):
            WeComCrypto("", AES_KEY)
        with pytest.raises(WeComCryptoError):
            WeComCrypto(TOKEN, "")

    def test_import_error_message_names_extra(self, monkeypatch):
        import trpc_agent_sdk.tenants._wecom_crypto as crypto_module

        monkeypatch.setattr(crypto_module, "_CRYPTO_AVAILABLE", False)
        with pytest.raises(ImportError, match=r"\[wecom\]"):
            WeComCrypto(TOKEN, AES_KEY)

    def test_crypto_available_flag(self):
        # This test module only runs when cryptography is installed.
        assert _CRYPTO_AVAILABLE is True


class TestSignature:

    def test_signature_matches_manual_sha1(self):
        encrypt = "some_encrypted_payload"
        expected = hashlib.sha1("".join(sorted([TOKEN, "1700000000", "nonce1", encrypt])).encode()).hexdigest()
        assert WeComCrypto.signature(TOKEN, "1700000000", "nonce1", encrypt) == expected

    def test_verify_signature_positive_and_negative(self):
        crypto = make_crypto()
        encrypt = "abc123"
        good = WeComCrypto.signature(TOKEN, "111", "nn", encrypt)
        assert crypto.verify_signature(good, "111", "nn", encrypt) is True
        assert crypto.verify_signature(good, "111", "other", encrypt) is False
        assert crypto.verify_signature(good, "111", "nn", encrypt + "tampered") is False
        assert crypto.verify_signature("", "111", "nn", encrypt) is False


class TestDecrypt:

    def test_encrypt_decrypt_roundtrip(self):
        crypto = make_crypto()
        xml_msg = "<xml><Content><![CDATA[你好，世界！ & <special>]]></Content></xml>"
        encrypt, msg_signature = crypto.encrypt(xml_msg, "1700000001", "nonceX")
        assert crypto.verify_signature(msg_signature, "1700000001", "nonceX", encrypt)
        assert crypto.decrypt(encrypt) == xml_msg

    def test_decrypt_strips_envelope(self):
        crypto = make_crypto()
        xml_msg = "<xml><MsgType>text</MsgType></xml>"
        assert crypto.decrypt(manual_encrypt(crypto, xml_msg, CORP_ID)) == xml_msg

    def test_receiveid_mismatch_rejected(self):
        crypto = make_crypto(receive_id=CORP_ID)
        encrypted = manual_encrypt(crypto, "<xml/>", "ww_other_corp")
        with pytest.raises(WeComDecryptError, match="receiveid mismatch"):
            crypto.decrypt(encrypted)

    def test_receiveid_not_checked_when_unconfigured(self):
        crypto = make_crypto(receive_id="")
        xml_msg = "<xml/>"
        assert crypto.decrypt(manual_encrypt(crypto, xml_msg, "whatever")) == xml_msg

    def test_corrupted_ciphertext_rejected(self):
        crypto = make_crypto()
        encrypted = bytearray(manual_encrypt(crypto, "<xml/>", CORP_ID).encode("ascii"))
        # Flip bits in the last block; with CBC this corrupts the padding.
        encrypted[-1] ^= 0xFF
        with pytest.raises(WeComDecryptError):
            crypto.decrypt(base64.b64encode(bytes(encrypted)).decode("ascii"))

    def test_invalid_base64_rejected(self):
        crypto = make_crypto()
        with pytest.raises(WeComDecryptError):
            crypto.decrypt("!!!not-base64!!!")

    def test_non_multiple_of_16_rejected(self):
        crypto = make_crypto()
        with pytest.raises(WeComDecryptError, match="multiple of 16"):
            crypto.decrypt(base64.b64encode(b"short").decode("ascii"))

    def test_malformed_envelope_rejected(self):
        crypto = make_crypto()
        # Valid AES block whose envelope lies about the message length.
        envelope = os.urandom(16) + struct.pack(">I", 9999) + b"x" * 11
        pad_len = 16 - (len(envelope) % 16)
        envelope += bytes([pad_len]) * pad_len
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        encryptor = Cipher(algorithms.AES(crypto._key), modes.CBC(crypto._iv)).encryptor()
        encrypt_b64 = base64.b64encode(encryptor.update(envelope) + encryptor.finalize()).decode("ascii")
        with pytest.raises(WeComDecryptError, match="msg_len"):
            crypto.decrypt(encrypt_b64)


class TestVerifyUrl:

    def test_verify_url_returns_echo(self):
        crypto = make_crypto()
        echo_plain = "echo_plain_text_12345"
        encrypt_b64, msg_signature = crypto.encrypt(echo_plain, "1700000002", "n1")
        result = crypto.verify_url(msg_signature, "1700000002", "n1", encrypt_b64)
        assert result == echo_plain

    def test_verify_url_bad_signature_rejected(self):
        crypto = make_crypto()
        encrypt_b64, _ = crypto.encrypt("echo", "1700000002", "n1")
        with pytest.raises(WeComSignatureError):
            crypto.verify_url("deadbeef", "1700000002", "n1", encrypt_b64)
