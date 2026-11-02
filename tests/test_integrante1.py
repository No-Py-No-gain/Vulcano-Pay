
"""
Proyecto Vulcano Pay
Pruebas automatizadas - Integrante 1.

Modulos:
- exceptions.py
- sanitizer.py
"""

import argparse
import sys
from pathlib import Path

import pytest

# Permite importar los modulos desde src.
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src"),
)

from vulcano_telemetry.exceptions import (
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)

# ==========================================
# PRUEBAS DE EXCEPCIONES
# ==========================================

@pytest.mark.parametrize(
    "exception_class",
    [
        SQSConnectionError,
        QueueTimeoutError,
        CorruptedMessageError,
        WebhookSignatureError,
        WebhookTimestampError,
        WebhookDeliveryError,
    ],
)
def test_exceptions_inherit_vulcano_error(exception_class):
    """Todas las excepciones heredan de VulcanoError."""

    assert issubclass(exception_class, VulcanoError)
    assert issubclass(exception_class, Exception)


def test_vulcano_error_not_base_exception():
    """La clase base debe heredar directamente de Exception."""

    assert VulcanoError.__bases__ == (Exception,)


def test_exception_notes():
    """Las excepciones permiten agregar notas de diagnostico."""

    error = SQSConnectionError("Conexion fallida")
    error.add_note("Proveedor: LocalStack")

    assert "Proveedor: LocalStack" in error.__notes__


# ==========================================
# PRUEBAS DE SANITIZER
# ==========================================

@pytest.mark.parametrize(
    "queue_name",
    [
        "vulcano-events-queue",
        "vulcano-payments-queue",
        "vulcano-test-123-queue",
    ],
)
def test_valid_queue_names(queue_name):
    assert parse_queue_name(queue_name) == queue_name


@pytest.mark.parametrize(
    "queue_name",
    [
        "events",
        "VULCANO-events-queue",
        "vulcano_events_queue",
        "vulcano-events",
        "",
    ],
)
def test_invalid_queue_names(queue_name):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_queue_name(queue_name)


@pytest.mark.parametrize(
    "wait_time",
    [1, 5, 10, 20],
)
def test_valid_wait_time(wait_time):
    assert parse_wait_time(str(wait_time)) == wait_time


@pytest.mark.parametrize(
    "wait_time",
    ["0", "21", "-1", "abc", "1.5"],
)
def test_invalid_wait_time(wait_time):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_wait_time(wait_time)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:8000/webhook",
        "https://example.com/webhook",
        "https://api.example.com:443/events",
    ],
)
def test_valid_webhook_urls(url):
    assert parse_webhook_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "example.com/webhook",
        "http://",
        "https://example.com:abc/webhook",
        "https://example.com/invalid path",
        "",
    ],
)
def test_invalid_webhook_urls(url):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_webhook_url(url)


@pytest.mark.parametrize(
    "secret",
    [
        "1234567890abcdef",
        "vulcano-secret-key-2026",
        "A" * 32,
    ],
)
def test_valid_secret_keys(secret):
    assert parse_secret_key(secret) == secret


@pytest.mark.parametrize(
    "secret",
    [
        "",
        "short",
        "123456789012345",
    ],
)
def test_invalid_secret_keys(secret):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_secret_key(secret)
