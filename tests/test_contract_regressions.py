"""Regresiones de contrato detectadas por el verificador independiente del PR #9.

Cada prueba fija una garantía que ya está documentada en el README o en el `epilog` de la CLI y que
una refactorización podría romper en silencio:

- el receptor responde `503` cuando el secreto HMAC falta o es demasiado corto (no `401`/`500`);
- el topic del evento respeta el orden `event_topic` → `event_type` → `payment.event`;
- el receptor construye su cliente SQS con `VULCANO_ENDPOINT_URL` (criterio 3: endpoint configurable);
- `QueueTimeoutError` sigue siendo el mismo objeto exportado por el paquete (C1 del contrato);
- `start-receiver` resuelve la cola **antes** de entregar el proceso a uvicorn, así una cola
  inexistente sale con el código 4 documentado y no con el `SystemExit(3)` de uvicorn (B1);
- `start-receiver` exporta `-q/--queue` a `VULCANO_QUEUE_NAME`, que es lo que lee el receptor (A3),
  sin pisar una elección explícita: `VULCANO_QUEUE_NAME` sólo aporta el valor por defecto.

Todo es unitario: sin Docker, sin red y sin LocalStack. Los dobles son `MagicMock` o funciones
locales que levantan las excepciones del dominio donde las levantaría el sistema real.
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

import app_operator
import vulcano_telemetry
import vulcano_telemetry.exceptions as exceptions_module
import vulcano_telemetry.webhook_receiver as receiver_module
from vulcano_telemetry.webhook_crypto import build_signature_header

SECRET = "clave-de-prueba-suficientemente-larga"
QUEUE_URL = "http://localhost:4566/000000000000/vulcano-events-queue"
MISSING_QUEUE = "vulcano-no-existe-queue"


def _sqs_double(queue_url: str = QUEUE_URL) -> MagicMock:
    """Cliente SQS simulado que resuelve cualquier nombre de cola a `queue_url`."""
    client = MagicMock()
    client.get_queue_url.return_value = {"QueueUrl": queue_url}
    return client


def _signed_post(client: TestClient, secret: str, payload: dict[str, Any]):
    """POST firmado con el mismo formato de cabecera que produce el rol 3."""
    body = json.dumps(payload).encode("utf-8")
    _timestamp, header = build_signature_header(secret, body)
    return client.post(
        "/webhook",
        content=body,
        headers={"Content-Type": "application/json", "X-Webhook-Signature": header},
    )


def _recording_sqs_double(resolved: list[str]) -> MagicMock:
    """Cliente SQS simulado que anota el `QueueName` con el que la CLI resuelve la cola."""
    client = MagicMock()

    def get_queue_url(**kwargs: Any) -> dict[str, str]:
        resolved.append(kwargs["QueueName"])
        return {"QueueUrl": QUEUE_URL}

    client.get_queue_url.side_effect = get_queue_url
    return client


@pytest.mark.parametrize("secret_value", ["", "corta"])
def test_missing_or_short_secret_returns_503(monkeypatch, secret_value: str) -> None:
    """Sin secreto utilizable el receptor no autentica a nadie: 503 y no 401/500."""
    monkeypatch.setenv("VULCANO_WEBHOOK_SECRET", secret_value)
    monkeypatch.setattr(receiver_module, "get_sqs_client", lambda *args, **kwargs: _sqs_double())
    monkeypatch.setattr(receiver_module, "send_event", MagicMock(return_value="mid-1"))

    with TestClient(receiver_module.app) as client:
        response = _signed_post(client, SECRET, {"transaction_id": "tx_1", "amount": 350.0})

    assert response.status_code == 503
    assert response.json()["detail"] == "Servicio no configurado."


def test_topic_comes_from_event_topic_then_event_type_then_default(monkeypatch) -> None:
    """`event_topic` manda; si falta se usa `event_type`; si faltan los dos, `payment.event`."""
    monkeypatch.setenv("VULCANO_WEBHOOK_SECRET", SECRET)
    monkeypatch.setattr(receiver_module, "get_sqs_client", lambda *args, **kwargs: _sqs_double())
    send_event = MagicMock(return_value="mid-1")
    monkeypatch.setattr(receiver_module, "send_event", send_event)

    bodies = [
        {"event_topic": "payment.settled"},
        {"event_type": "payment.refunded"},
        {},
    ]
    expected = ["payment.settled", "payment.refunded", "payment.event"]

    with TestClient(receiver_module.app) as client:
        for body in bodies:
            assert _signed_post(client, SECRET, body).status_code == 202

    observed = [call.kwargs["event_topic"] for call in send_event.call_args_list]
    assert observed == expected


def test_receiver_honours_vulcano_endpoint_url(monkeypatch) -> None:
    """Criterio 3: el cliente SQS del receptor se construye con el endpoint del entorno."""
    recorded: list[Any] = []
    client_double = _sqs_double()

    def fake_get_sqs_client(endpoint_url: str = "http://localhost:4566") -> MagicMock:
        recorded.append(endpoint_url)
        return client_double

    monkeypatch.setenv("VULCANO_ENDPOINT_URL", "http://127.0.0.1:4599")
    monkeypatch.setattr(receiver_module, "get_sqs_client", fake_get_sqs_client)

    with TestClient(receiver_module.app):
        pass

    assert recorded == ["http://127.0.0.1:4599"]


def test_queue_timeout_error_is_reexported_from_the_package() -> None:
    """C1: `QueueTimeoutError` es el mismo objeto en el paquete y en `exceptions`."""
    assert vulcano_telemetry.QueueTimeoutError is exceptions_module.QueueTimeoutError


def test_start_receiver_exits_4_when_the_queue_cannot_be_resolved(monkeypatch, tmp_path) -> None:
    """B1: una cola irresoluble es error de SQS (4) y nunca debe llegar a arrancar uvicorn."""
    client_double = MagicMock()
    client_double.get_queue_url.side_effect = ClientError(
        {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "no existe"}},
        "GetQueueUrl",
    )

    def uvicorn_must_not_start(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("uvicorn no debe arrancar")

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda *args, **kwargs: client_double)
    monkeypatch.setattr(app_operator.uvicorn, "run", uvicorn_must_not_start)

    exit_code = app_operator.main(
        ["start-receiver", "-q", MISSING_QUEUE, "--log-file", str(tmp_path / "r.log")]
    )

    assert exit_code == 4


def test_start_receiver_exports_the_queue_name_to_the_receiver(monkeypatch, tmp_path) -> None:
    """A3: `-q/--queue` no puede ser un argumento ignorado; el receptor lee `VULCANO_QUEUE_NAME`."""
    invocations: list[dict[str, Any]] = []
    monkeypatch.delenv("VULCANO_QUEUE_NAME", raising=False)
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda *args, **kwargs: _sqs_double())
    monkeypatch.setattr(
        app_operator.uvicorn,
        "run",
        lambda *args, **kwargs: invocations.append({"args": args, "kwargs": kwargs}),
    )

    exit_code = app_operator.main(
        ["start-receiver", "-q", "vulcano-a-queue", "--log-file", str(tmp_path / "r.log")]
    )

    assert exit_code == 0
    assert invocations, "uvicorn debía arrancar con la cola resuelta"
    assert os.environ["VULCANO_QUEUE_NAME"] == "vulcano-a-queue"


def test_queue_flag_wins_over_vulcano_queue_name_env(monkeypatch, tmp_path) -> None:
    """`VULCANO_QUEUE_NAME` es el valor por defecto; `-q` manda cuando el operador lo pasa."""
    resolved: list[str] = []
    monkeypatch.setattr(
        app_operator,
        "get_sqs_client",
        lambda *args, **kwargs: _recording_sqs_double(resolved),
    )
    monkeypatch.setattr(app_operator.uvicorn, "run", lambda *args, **kwargs: None)
    monkeypatch.setenv("VULCANO_QUEUE_NAME", "vulcano-env-queue")

    assert app_operator.main(["start-receiver", "--log-file", str(tmp_path / "env.log")]) == 0
    assert os.environ["VULCANO_QUEUE_NAME"] == "vulcano-env-queue"

    exit_code = app_operator.main(
        ["start-receiver", "-q", "vulcano-flag-queue", "--log-file", str(tmp_path / "flag.log")]
    )

    assert exit_code == 0
    assert os.environ["VULCANO_QUEUE_NAME"] == "vulcano-flag-queue"
    assert resolved == ["vulcano-env-queue", "vulcano-flag-queue"]
