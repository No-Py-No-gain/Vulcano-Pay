"""Despachador de webhooks firmados y worker SQS de Vulcano Pay (rol 5).

`dispatch_webhook()` firma el cuerpo con HMAC-SHA256 y lo envía por POST. El worker consume la cola
con long polling, borra el mensaje **sólo** cuando el destino confirmó con 2xx y, cuando falla, lo
deja en la cola para que el `RedrivePolicy` lo derive a la DLQ tras `maxReceiveCount` intentos. No
hay reintentos en proceso: el reintento lo hace SQS con el `VisibilityTimeout`, que es el mecanismo
real y el único que puede terminar en la DLQ.

Detalle crítico: se firman y se envían **los mismos bytes**. `requests.post(..., json=payload)`
re-serializa el diccionario y rompería la firma aunque el contenido sea equivalente.

Puentes documentados con los roles 1-3 (ver W0 del documento de tareas): `consume_events` de
`core_sqs` aborta el lote completo ante un cuerpo corrupto y todavía no expone el
`ApproximateReceiveCount`, y `core_sqs` aún no tiene `delete_message`. Mientras eso no llegue, el
worker usa las primitivas de Boto3 directamente: sin aislamiento por mensaje la DLQ no puede
funcionar, porque un solo mensaje venenoso cancelaría el lote entero.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from botocore.exceptions import ClientError

from .exceptions import CorruptedMessageError, SQSConnectionError, WebhookDeliveryError
from .logging_engine import LOGGER_NAME
from .webhook_crypto import generate_signature

SIGNATURE_HEADER = "X-Webhook-Signature"
DEFAULT_TIMEOUT = 5.0
DEFAULT_WAIT_TIME = 10
DEFAULT_VISIBILITY_TIMEOUT = 30
DEFAULT_MAX_MESSAGES = 5


@dataclass(frozen=True)
class DispatchResult:
    """Resultado de una entrega confirmada por el destino."""

    status_code: int
    latency_ms: float
    timestamp: int


def serialize_body(payload: dict[str, Any]) -> bytes:
    """Bytes exactos que se firman y se envían. Una sola serialización para las dos cosas."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def build_signature_header(secret: str, timestamp: int, body: bytes) -> str:
    """Compone la cabecera `t=<timestamp>,v1=<hmac>`.

    El formato se toma del rol 3 vía `generate_signature`, que ya existe en el repo. Cuando su
    `build_signature_header` llegue (contrato W0) esta función delega y queda un único dueño del
    formato en todo el sistema.
    """
    return f"t={timestamp},v1={generate_signature(secret, timestamp, body)}"


def dispatch_webhook(
    target_url: str,
    secret: str,
    payload: dict[str, Any],
    *,
    timeout: float = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> DispatchResult:
    """Firma el payload y lo entrega por POST. Devuelve el resultado o lanza `WebhookDeliveryError`.

    Un 2xx es entrega confirmada. Cualquier otra cosa —4xx, 5xx, timeout, conexión rechazada— es un
    fallo de entrega y se informa con notas para que el registro forense conserve el contexto.
    """
    body = serialize_body(payload)
    timestamp = int(time.time())
    header = build_signature_header(secret, timestamp, body)
    http_client: Any = session if session is not None else requests

    started_at = time.perf_counter()
    try:
        response = http_client.post(
            target_url,
            data=body,
            headers={"Content-Type": "application/json", SIGNATURE_HEADER: header},
            timeout=timeout,
        )
    except requests.RequestException as err:
        latency_ms = (time.perf_counter() - started_at) * 1000
        error = WebhookDeliveryError(f"No se pudo entregar el webhook a {target_url}: {type(err).__name__}")
        error.add_note(f"detalle={err}")
        error.add_note(f"latencia_ms={latency_ms:.1f}")
        error.add_note(f"timeout_s={timeout}")
        raise error from err

    latency_ms = (time.perf_counter() - started_at) * 1000

    if not 200 <= response.status_code < 300:
        error = WebhookDeliveryError(f"El destino {target_url} respondió HTTP {response.status_code}.")
        error.add_note(f"target_url={target_url}")
        error.add_note(f"status_code={response.status_code}")
        error.add_note(f"latencia_ms={latency_ms:.1f}")
        error.add_note(f"{SIGNATURE_HEADER}={header}")
        raise error

    return DispatchResult(status_code=response.status_code, latency_ms=latency_ms, timestamp=timestamp)


class SQSDispatcher:
    """Worker que consume la cola y despacha cada evento firmado a su destino.

    Responsabilidad única por mensaje: interpretar el cuerpo, despachar y —sólo si el destino
    confirmó— borrar el mensaje. Nunca borra ante un fallo ni ante un cuerpo corrupto: dejarlo en la
    cola es exactamente lo que alimenta el contador de recepciones que usa el `RedrivePolicy`.
    """

    def __init__(
        self,
        client: Any,
        queue_url: str,
        target_url: str,
        secret: str,
        logger: logging.Logger | None = None,
        *,
        wait_time: int = DEFAULT_WAIT_TIME,
        visibility_timeout: int = DEFAULT_VISIBILITY_TIMEOUT,
        max_messages: int = DEFAULT_MAX_MESSAGES,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.client = client
        self.queue_url = queue_url
        self.target_url = target_url
        self.secret = secret
        self.logger = logger if logger is not None else logging.getLogger(LOGGER_NAME)
        self.wait_time = wait_time
        self.visibility_timeout = visibility_timeout
        self.max_messages = max_messages
        self.timeout = timeout
        self._stopped = False
        self.delivered = 0
        self.failed = 0

    def stop(self) -> None:
        """Pide el corte ordenado del bucle. La interrupción se atiende entre lotes, no a mitad de uno."""
        self._stopped = True

    @property
    def stopped(self) -> bool:
        return self._stopped

    def _receive(self) -> list[dict[str, Any]]:
        try:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time,
                VisibilityTimeout=self.visibility_timeout,
                MessageAttributeNames=["All"],
                AttributeNames=["ApproximateReceiveCount"],
            )
        except ClientError as err:
            error = SQSConnectionError("Error al recibir mensajes de SQS.")
            error.add_note(f"QueueUrl: {self.queue_url}")
            error.add_note(f"detalle={err}")
            raise error from err
        return response.get("Messages", [])

    def _delete(self, receipt_handle: str) -> None:
        try:
            self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as err:
            error = SQSConnectionError("Error al borrar un mensaje ya entregado.")
            error.add_note(f"QueueUrl: {self.queue_url}")
            error.add_note(f"detalle={err}")
            raise error from err

    def _context(self, message: dict[str, Any]) -> dict[str, Any]:
        attributes = message.get("MessageAttributes") or {}
        trace = (attributes.get("TraceId") or {}).get("StringValue")
        webhook_id = (attributes.get("WebhookId") or {}).get("StringValue")
        message_id = message.get("MessageId", "desconocido")
        receive_count = (message.get("Attributes") or {}).get("ApproximateReceiveCount", "1")
        return {
            "trace_id": trace or message_id,
            "message_id": message_id,
            # El formatter descarta los valores None: el campo sólo aparece si el emisor lo envió.
            "webhook_id": webhook_id,
            "retry_count": int(receive_count),
        }

    def process(self, message: dict[str, Any]) -> bool:
        """Procesa un mensaje. Devuelve True sólo si se entregó y se borró."""
        context = self._context(message)
        receipt_handle = message.get("ReceiptHandle", "")

        try:
            body = json.loads(message.get("Body", ""))
        except json.JSONDecodeError as err:
            # Se registra y se aísla el mensaje, igual que con un JSON que no es objeto: abortar el
            # lote acá cancelaría los mensajes válidos que vienen detrás en la misma recepción.
            error = CorruptedMessageError(f"El cuerpo del mensaje {context['message_id']} no es JSON válido.")
            error.add_note(f"detalle={err}")
            error.add_note("El mensaje queda en la cola: el RedrivePolicy lo derivará a la DLQ.")
            self.logger.error(
                "mensaje venenoso descartado para despacho",
                exc_info=error,
                extra=context,
            )
            self.failed += 1
            return False

        if not isinstance(body, dict):
            error = CorruptedMessageError(f"El cuerpo del mensaje {context['message_id']} no es un objeto JSON.")
            error.add_note("El mensaje queda en la cola: el RedrivePolicy lo derivará a la DLQ.")
            self.logger.error("mensaje venenoso descartado para despacho", exc_info=error, extra=context)
            self.failed += 1
            return False

        started_at = time.perf_counter()
        try:
            result = dispatch_webhook(self.target_url, self.secret, body, timeout=self.timeout)
        except WebhookDeliveryError as err:
            latency_ms = (time.perf_counter() - started_at) * 1000
            self.logger.error(
                "entrega fallida; el mensaje queda en la cola para el reintento de SQS",
                exc_info=err,
                extra={
                    **context,
                    "latency_ms": round(latency_ms, 1),
                    "notes": list(getattr(err, "__notes__", ())),
                },
            )
            self.failed += 1
            return False

        self._delete(receipt_handle)
        self.delivered += 1
        self.logger.info(
            "webhook entregado",
            extra={**context, "latency_ms": round(result.latency_ms, 1)},
        )
        return True

    def process_batch(self) -> int:
        """Consume un lote y devuelve cuántos mensajes quedaron entregados."""
        return sum(1 for message in self._receive() if self.process(message))

    def run(self, iterations: int | None = None) -> int:
        """Bucle principal. `iteraciones` acota la corrida para pruebas y para el modo `--once`."""
        total = 0
        rounds = 0
        while not self._stopped and (iterations is None or rounds < iterations):
            total += self.process_batch()
            rounds += 1
        return total
