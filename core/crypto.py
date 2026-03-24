"""
core/crypto.py — AES-256-CBC encryption / decryption helpers.

Key derivation:
    PBKDF2-HMAC-SHA256(password, salt, iterations=200_000) → 32-byte key

Encryption format written to each .partN file when encrypted=True:
    [16 bytes IV][ciphertext padded to AES block boundary]

The salt is stored ONCE in the index entry (not per-part) so the same
key is reused for all parts of the same file.  A fresh salt+IV is
generated for every new upload.
"""

from __future__ import annotations

import hashlib
import os
import struct
from typing import Optional

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# ── Constants ─────────────────────────────────────────────────────────────────
PBKDF2_ITERATIONS = 200_000
AES_KEY_LEN       = 32   # 256-bit
AES_BLOCK         = 16
IV_LEN            = 16
SALT_LEN          = 32


# ── Key derivation ─────────────────────────────────────────────────────────────

def derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 32-byte AES key from a password + salt using PBKDF2."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=AES_KEY_LEN,
    )


def new_salt() -> bytes:
    return os.urandom(SALT_LEN)


def new_iv() -> bytes:
    return os.urandom(IV_LEN)


# ── PKCS7 padding ─────────────────────────────────────────────────────────────

def _pad(data: bytes) -> bytes:
    pad_len = AES_BLOCK - (len(data) % AES_BLOCK)
    return data + bytes([pad_len] * pad_len)


def _unpad(data: bytes) -> bytes:
    if not data:
        raise ValueError("Empty data after decryption")
    pad_len = data[-1]
    if pad_len < 1 or pad_len > AES_BLOCK:
        raise ValueError(f"Invalid PKCS7 padding byte: {pad_len}")
    return data[:-pad_len]


# ── File-level encrypt / decrypt ──────────────────────────────────────────────

CHUNK = 1_048_576  # 1 MB read chunks


def encrypt_file(src: str, dst: str, key: bytes, iv: bytes) -> None:
    """
    Encrypt src → dst using AES-256-CBC.
    Output format: [16-byte IV][PKCS7-padded ciphertext]
    The IV is prepended so decrypt_file only needs the key.
    """
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    enc    = cipher.encryptor()

    with open(src, "rb") as fin, open(dst, "wb") as fout:
        fout.write(iv)                         # prepend IV
        buf = b""
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            buf += chunk
            # Encrypt full blocks, keep remainder
            full = (len(buf) // AES_BLOCK) * AES_BLOCK
            if full:
                fout.write(enc.update(buf[:full]))
                buf = buf[full:]
        # Final block with padding
        fout.write(enc.update(_pad(buf)))
        fout.write(enc.finalize())


def decrypt_file(src: str, dst: str, key: bytes) -> None:
    """
    Decrypt src → dst.  Reads the prepended IV, then decrypts + unpads.
    """
    with open(src, "rb") as fin:
        iv = fin.read(IV_LEN)
        if len(iv) != IV_LEN:
            raise ValueError("Encrypted file too short — missing IV")

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        dec    = cipher.decryptor()

        with open(dst, "wb") as fout:
            buf = b""
            while True:
                chunk = fin.read(CHUNK)
                if not chunk:
                    break
                buf += chunk
                # Decrypt full blocks, keep at least one block buffered
                # so we can unpad the very last block correctly
                safe = max(0, (len(buf) // AES_BLOCK - 1) * AES_BLOCK)
                if safe:
                    fout.write(dec.update(buf[:safe]))
                    buf = buf[safe:]
            # Decrypt + unpad final buffer
            final = dec.update(buf) + dec.finalize()
            fout.write(_unpad(final))