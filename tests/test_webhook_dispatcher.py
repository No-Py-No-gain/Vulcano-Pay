"""Despachador de webhooks firmados y worker SQS de Vulcano Pay (rol 5).

Todo lo que se prueba acá es unitario: el destino HTTP es un servidor real en 127.0.0.1 y el
cliente SQS es un doble local, así que no hace falta Docker ni LocalStack.

Lo que se verifica, por regla del enunciado:
- la firma que produce el despachador la acepta el verificador del rol 3 (S6, contrato rol 3 ↔ rol 5);
- se firman y se envían los mismos bytes, sin re-serializar el payload (S6);
- un destino que falla no borra el mensaje de la cola: eso es lo que el `RedrivePolicy` deriva a la
  DLQ (S9);
- un cuerpo venenoso no aborta el lote ni se borra, y el mensaje válido del mismo lote sí se
  entrega (S9);
- el worker consume con long polling, `VisibilityTimeout` y `ApproximateReceiveCount`, y registra
  `trace_id`, `message_id`, `retry_count` y `latency_ms` (S5, S9).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from vulcano_telemetry.dispatcher import (
    SIGNATURE_HEADER,
    SQSDispatcher,
    dispatch_webhook,
    serialize_body,
)
from vulcano_telemetry.exceptions import WebhookDeliveryError
from vulcano_telemetry.logging_engine import OPTIONAL_FIELDS, AsyncJSONFormatter
from vulcano_telemetry.webhook_crypto import verify_signature

SECRET = "s3cr3t-de-prueba"
QUEUE_URL = "http://localhost:4566/000000000000/vulcano-events-queue"
MAX_WAIT = 5.0


class LocalTarget:
    """Destino HTTP real en 127.0.0.1 con respuesta y demora configurables.

    No se mockea `requests`: la prueba de contrato de la firma necesita ver los bytes que de verdad
    salen por el socket y la cabecera que de verdad llega al receptor.
    """

    def __init__(self) -> None:
        self.posted: list[dict[str, Any]] = []
        self.status = 200
        self.delay = 0.0
        target = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                target.posted.append({"headers": dict(self.headers), "cuerpo": body})
                if target.delay:
                    time.sleep(target.delay)
                try:
                    self.send_response(target.status)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                except (BrokenPipeError, ConnectionResetError):
                    # El cliente ya colgó la conexión: es justo lo que provoca el caso de timeout.
                    self.close_connection = True

            def log_message(self, fmt: str, *args: Any) -> None:
                """Silenciado a propósito: la salida de la suite sólo debe hablar por los asserts."""

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/webhook"
        self._thread = threading.Thread(target=self._server.serve_forever, name="destino-http", daemon=True)
        self._thread.start()

    @property
    def last_request(self) -> dict[str, Any]:
        assert self.posted, "el destino no recibió ninguna petición"
        return self.posted[-1]

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=MAX_WAIT)


class RecordCollector(logging.Handler):
    """Handler en memoria: guarda los `LogRecord` para inspeccionar los extras del contrato S5."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class FakeSQSClient:
    """Doble de SQS: no habla con AWS, sólo registra las llamadas que hace el worker."""

    def __init__(self, messages: list[dict[str, Any]] | None = None) -> None:
        self.pending = list(messages or [])
        self.receive_count: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    def receive_message(self, **arguments: Any) -> dict[str, Any]:
        self.receive_count.append(arguments)
        batch, self.pending = self.pending, []
        return {"Messages": batch} if batch else {}

    def delete_message(self, **arguments: Any) -> dict[str, Any]:
        self.delete_calls.append(arguments)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture
def target():
    """Servidor HTTP local vivo durante todo el caso; se cierra al finalizar (S6)."""
    target = LocalTarget()
    yield target
    target.close()


@pytest.fixture
def test_logger():
    """Logger aislado con un handler en memoria, sin propagar al logger raíz."""
    logger = logging.getLogger("vulcano_test_dispatcher")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers.clear()
    collector = RecordCollector()
    logger.addHandler(collector)
    yield logger, collector.records
    logger.removeHandler(collector)
    logger.handlers.clear()


def _header(request: dict[str, Any], name: str) -> str | None:
    """Valor de una cabecera HTTP recibida, sin depender de cómo la escribió el cliente."""
    for key, value in request["headers"].items():
        if key.lower() == name.lower():
            return value
    return None


def _message(
    message_id: str,
    body: str,
    receipt_handle: str,
    *,
    receive_count: int = 1,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """Mensaje SQS con la forma que devuelve Boto3 (cuerpo, ticket e intentos de recepción)."""
    message: dict[str, Any] = {
        "MessageId": message_id,
        "Body": body,
        "ReceiptHandle": receipt_handle,
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }
    if trace_id is not None:
        message["MessageAttributes"] = {"TraceId": {"StringValue": trace_id}}
    return message


def _build_worker(client: Any, target: LocalTarget, logger: logging.Logger, **options: Any) -> SQSDispatcher:
    return SQSDispatcher(client, QUEUE_URL, target.url, SECRET, logger, **options)


def test_receiver_verifies_the_signature_the_dispatcher_generates(target):
    """Contrato rol 5 ↔ rol 3: lo que firma el despachador lo acepta `verify_signature`."""
    payload = {"evento": "pago.aprobado", "monto": 1500.5, "moneda": "ARS"}

    dispatch_webhook(target.url, SECRET, payload)

    request = target.last_request
    header = _header(request, SIGNATURE_HEADER)

    assert header is not None, f"el POST no llevó la cabecera {SIGNATURE_HEADER}"
    assert verify_signature(SECRET, header, request["cuerpo"]) is True


def test_the_same_bytes_are_signed_and_sent(target):
    """El cuerpo que viaja es exactamente la serialización firmada, no una re-serialización."""
    payload = {
        "zeta": "último a propósito",
        "descripción": "Cobro con tarjeta — señas",
        "alfa": 1,
        "anidado": {"ñ": True, "lista": [3, 2, 1]},
    }

    dispatch_webhook(target.url, SECRET, payload)

    request = target.last_request

    assert request["cuerpo"] == serialize_body(payload), (
        "el cuerpo enviado no es byte a byte el que se firmó: json= de requests lo re-serializaría"
    )
    assert json.loads(request["cuerpo"]) == payload
    assert _header(request, "Content-Type") == "application/json"


def test_http_500_raises_webhook_delivery_error_with_notes(target):
    """Un 5xx es fallo de entrega y las notas deben conservar la URL y el código."""
    target.status = 500

    with pytest.raises(WebhookDeliveryError) as raised:
        dispatch_webhook(target.url, SECRET, {"evento": "pago.aprobado"})

    notes = "\n".join(raised.value.__notes__)

    assert target.url in notes, f"las notas no mencionan la URL del destino: {notes!r}"
    assert "500" in notes, f"las notas no mencionan el código HTTP 500: {notes!r}"


def test_target_timeout_raises_webhook_delivery_error(target):
    """Un destino más lento que el timeout es un fallo de entrega, no un cuelgue del worker."""
    target.delay = 0.6

    with pytest.raises(WebhookDeliveryError) as raised:
        dispatch_webhook(target.url, SECRET, {"evento": "pago.aprobado"}, timeout=0.15)

    notes = "\n".join(raised.value.__notes__)

    assert "timeout_s" in notes, f"las notas no registran el timeout usado: {notes!r}"


def test_worker_deletes_the_message_when_the_target_confirms(target, test_logger):
    """Con 2xx el mensaje se borra: ya no vuelve a la cola ni puede terminar en la DLQ."""
    logger, _ = test_logger
    client = FakeSQSClient([_message("msg-1", json.dumps({"evento": "pago.aprobado"}), "rh-1")])
    worker = _build_worker(client, target, logger)

    assert worker.process_batch() == 1, "el mensaje confirmado no se contó como entregado"
    assert len(client.delete_calls) == 1, "delete_message debe llamarse exactamente una vez"
    assert client.delete_calls[0]["ReceiptHandle"] == "rh-1"
    assert client.delete_calls[0]["QueueUrl"] == QUEUE_URL
    assert worker.delivered == 1
    assert worker.failed == 0


def test_worker_does_not_delete_when_the_target_fails(target, test_logger):
    """No borrar es lo que hace que SQS reintente y el RedrivePolicy derive a la DLQ (S9)."""
    logger, _ = test_logger
    target.status = 500
    client = FakeSQSClient([_message("msg-2", json.dumps({"evento": "pago.aprobado"}), "rh-2")])
    worker = _build_worker(client, target, logger)

    assert worker.process_batch() == 0
    assert client.delete_calls == [], "un fallo de entrega no puede borrar el mensaje: se perdería la DLQ"
    assert worker.failed == 1, "el fallo no se contabilizó"
    assert worker.delivered == 0


def test_corrupted_body_does_not_abort_the_batch_and_is_not_deleted(target, test_logger):
    """Un mensaje venenoso queda en la cola sin impedir que el resto del lote se entregue."""
    logger, _ = test_logger
    client = FakeSQSClient(
        [
            _message("corrupto", "no es json", "rh-corrupto"),
            _message("valido", json.dumps({"evento": "pago.aprobado"}), "rh-valido"),
        ]
    )
    worker = _build_worker(client, target, logger)

    delivered = worker.process_batch()

    assert delivered == 1, "un cuerpo corrupto no debe abortar el lote completo"
    assert [deleted["ReceiptHandle"] for deleted in client.delete_calls] == ["rh-valido"], (
        "sólo se borra el mensaje entregado; el corrupto debe quedar para la DLQ"
    )
    assert worker.failed == 1

    # Mismo trato para un JSON válido que no es objeto: tampoco es un evento despachable.
    client.pending = [
        _message("texto", '"texto"', "rh-texto"),
        _message("valido-2", json.dumps({"evento": "pago.rechazado"}), "rh-valido-2"),
    ]
    client.delete_calls.clear()

    assert worker.process_batch() == 1, "un JSON que no es objeto también debe aislarse, no abortar"
    assert [deleted["ReceiptHandle"] for deleted in client.delete_calls] == ["rh-valido-2"]


def test_worker_uses_long_polling_visibility_timeout_and_retry_counter(target, test_logger):
    """La recepción configura la cola y el registro lleva correlación, reintentos y latencia."""
    logger, records = test_logger
    client = FakeSQSClient(
        [
            _message(
                "msg-3",
                json.dumps({"evento": "pago.aprobado"}),
                "rh-3",
                receive_count=3,
                trace_id="trace-xyz",
            )
        ]
    )
    worker = _build_worker(client, target, logger, wait_time=7, visibility_timeout=45)

    assert worker.process_batch() == 1

    (receive_call,) = client.receive_count
    assert receive_call["WaitTimeSeconds"] == 7, "la recepción no pidió long polling"
    assert receive_call["VisibilityTimeout"] == 45, "la recepción no respetó el VisibilityTimeout de la cola"
    assert receive_call["AttributeNames"] == ["ApproximateReceiveCount"], (
        "sin ApproximateReceiveCount no hay contador de reintentos que auditar"
    )

    record = records[-1]

    assert record.getMessage() == "webhook entregado"
    assert record.trace_id == "trace-xyz"
    assert record.message_id == "msg-3"
    assert record.retry_count == 3, "retry_count debe salir del ApproximateReceiveCount"
    assert isinstance(record.latency_ms, float)
    assert "retry_count" in OPTIONAL_FIELDS

    data = json.loads(AsyncJSONFormatter().format(record))

    assert data["retry_count"] == 3, "el formatter no emite retry_count aunque llegue por extra"
