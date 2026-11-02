
"""
Proyecto Vulcano Pay
Pruebas del Integrante 3.

Validacion de firmas HMAC-SHA256 y timestamps.
"""

import hashlib
import hmac
import time
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from fastapi.testclient import TestClient

from vulcano_telemetry.exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.webhook_crypto import (
    generate_signature,
    verify_signature,
)
from vulcano_telemetry.webhook_receiver import app

SECRET = "clave_super_segura_vulcano_2026"
PAYLOAD = b'{"payment_id":"PAY-001","amount":350}'


def make_header(secret, timestamp, payload):
    signature = generate_signature(
        secret,
        timestamp,
        payload,
    )
    return f"t={timestamp},v1={signature}"


def test_generate_signature_hmac_sha256():
    """Verifica el algoritmo criptografico utilizado."""
    timestamp = 1700000000

    message = f"{timestamp}.".encode() + PAYLOAD

    expected = hmac.new(
        SECRET.encode(),
        message,
        hashlib.sha256,
    ).hexdigest()

    result = generate_signature(
        SECRET,
        timestamp,
        PAYLOAD,
    )

    assert result == expected
    assert len(result) == 64


def test_verify_signature_valid():
    """Acepta una firma correcta."""
    timestamp = int(time.time())
    header = make_header(SECRET, timestamp, PAYLOAD)

    assert verify_signature(
        SECRET,
        header,
        PAYLOAD,
    ) is True


def test_verify_signature_invalid():
    """Rechaza una firma alterada."""
    timestamp = int(time.time())
    header = f"t={timestamp},v1={'0' * 64}"

    with pytest.raises(WebhookSignatureError):
        verify_signature(
            SECRET,
            header,
            PAYLOAD,
        )


def test_verify_signature_modified_payload():
    """Detecta modificaciones en el cuerpo HTTP."""
    timestamp = int(time.time())
    header = make_header(SECRET, timestamp, PAYLOAD)

    modified = b'{"payment_id":"PAY-001","amount":999}'

    with pytest.raises(WebhookSignatureError):
        verify_signature(
            SECRET,
            header,
            modified,
        )


def test_verify_signature_expired_timestamp():
    """Rechaza un webhook de hace una hora."""
    timestamp = int(time.time()) - 3600
    header = make_header(SECRET, timestamp, PAYLOAD)

    with pytest.raises(WebhookTimestampError):
        verify_signature(
            SECRET,
            header,
            PAYLOAD,
        )


def test_verify_signature_future_timestamp():
    """Rechaza timestamps demasiado futuros."""
    timestamp = int(time.time()) + 3600
    header = make_header(SECRET, timestamp, PAYLOAD)

    with pytest.raises(WebhookTimestampError):
        verify_signature(
            SECRET,
            header,
            PAYLOAD,
        )


@pytest.mark.parametrize(
    "header",
    [
        "",
        "firma_invalida",
        "t=123",
        "v1=abcdef",
        "t=abc,v1=abcdef",
        "t=123,v1=",
        "t=123,v1=abc,t=456",
    ],
)
def test_verify_signature_malformed_header(header):
    """Rechaza cabeceras ausentes o malformadas."""
    with pytest.raises(WebhookSignatureError):
        verify_signature(
            SECRET,
            header,
            PAYLOAD,
        )


@pytest.fixture
def http_client(monkeypatch):
    """Cliente HTTP con SQS simulado."""
    monkeypatch.setenv(
        "VULCANO_WEBHOOK_SECRET",
        SECRET,
    )
    monkeypatch.setenv(
        "VULCANO_QUEUE_NAME",
        "vulcano-events-queue",
    )

    sqs_mock = MagicMock()
    sqs_mock.get_queue_url.return_value = {
        "QueueUrl": "http://localhost:4566/queue/test"
    }

    with patch(
        "vulcano_telemetry.webhook_receiver.get_sqs_client",
        return_value=sqs_mock,
    ), patch(
        "vulcano_telemetry.webhook_receiver.send_event",
        return_value="test-message-id",
    ):
        with TestClient(app) as client:
            yield client


def test_webhook_http_202(http_client):
    """Un webhook valido recibe HTTP 202."""
    timestamp = int(time.time())
    header = make_header(SECRET, timestamp, PAYLOAD)

    response = http_client.post(
        "/webhook",
        content=PAYLOAD,
        headers={"X-Webhook-Signature": header},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert response.json()["message_id"] == "test-message-id"


def test_webhook_http_invalid_signature(http_client):
    """Una firma falsa recibe HTTP 401."""
    timestamp = int(time.time())

    response = http_client.post(
        "/webhook",
        content=PAYLOAD,
        headers={
            "X-Webhook-Signature": (
                f"t={timestamp},v1={'0' * 64}"
            )
        },
    )

    assert response.status_code == 401


def test_webhook_http_expired_timestamp(http_client):
    """Un timestamp vencido recibe HTTP 403."""
    timestamp = int(time.time()) - 3600
    header = make_header(SECRET, timestamp, PAYLOAD)

    response = http_client.post(
        "/webhook",
        content=PAYLOAD,
        headers={"X-Webhook-Signature": header},
    )

    assert response.status_code == 403


def test_webhook_http_invalid_json(http_client):
    """Un JSON incorrecto recibe HTTP 422."""
    payload = b"{json_invalido"
    timestamp = int(time.time())
    header = make_header(SECRET, timestamp, payload)

    response = http_client.post(
        "/webhook",
        content=payload,
        headers={"X-Webhook-Signature": header},
    )

    assert response.status_code == 422


def test_webhook_http_missing_signature(http_client):
    """Una solicitud sin firma recibe HTTP 401."""
    response = http_client.post(
        "/webhook",
        content=PAYLOAD,
    )

    assert response.status_code == 401


def test_webhook_http_sqs_unavailable(http_client):
    """Si SQS falla, el webhook debe responder HTTP 503."""
    timestamp = int(time.time())
    header = make_header(SECRET, timestamp, PAYLOAD)

    with patch(
        "vulcano_telemetry.webhook_receiver.send_event",
        side_effect=EndpointConnectionError(
            endpoint_url="http://localhost:4566"
        ),
    ):
        response = http_client.post(
            "/webhook",
            content=PAYLOAD,
            headers={"X-Webhook-Signature": header},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Servicio de mensajeria no disponible."
    )


def test_lifespan_translates_missing_queue_into_sqs_connection_error():
    """Un `get_queue_url` fallido en el arranque sale como SQSConnectionError.

    La traduccion es la que permite a la CLI mapear la falta de cola al codigo de salida 4:
    si el `lifespan` dejara escapar el `ClientError` de boto3, ningun `except` del dominio
    lo atraparia.
    """
    sqs_mock = MagicMock()
    sqs_mock.get_queue_url.side_effect = ClientError(
        {
            "Error": {
                "Code": "AWS.SimpleQueueService.NonExistentQueue",
                "Message": "no existe",
            }
        },
        "GetQueueUrl",
    )

    with patch(
        "vulcano_telemetry.webhook_receiver.get_sqs_client",
        return_value=sqs_mock,
    ), pytest.raises(SQSConnectionError):
        with TestClient(app):
            pass
