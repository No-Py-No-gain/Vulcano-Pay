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
from .logging_engine import NOMBRE_LOGGER
from .webhook_crypto import generate_signature

CABECERA_FIRMA = "X-Webhook-Signature"
TIEMPO_LIMITE_POR_DEFECTO = 5.0
ESPERA_POR_DEFECTO = 10
VISIBILIDAD_POR_DEFECTO = 30
MAX_MENSAJES_POR_DEFECTO = 5


@dataclass(frozen=True)
class ResultadoDespacho:
    """Resultado de una entrega confirmada por el destino."""

    status_code: int
    latency_ms: float
    timestamp: int


def serializar_cuerpo(payload: dict[str, Any]) -> bytes:
    """Bytes exactos que se firman y se envían. Una sola serialización para las dos cosas."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def cabecera_de_firma(secret: str, timestamp: int, cuerpo: bytes) -> str:
    """Compone la cabecera `t=<timestamp>,v1=<hmac>`.

    El formato se toma del rol 3 vía `generate_signature`, que ya existe en el repo. Cuando su
    `build_signature_header` llegue (contrato W0) esta función delega y queda un único dueño del
    formato en todo el sistema.
    """
    return f"t={timestamp},v1={generate_signature(secret, timestamp, cuerpo)}"


def dispatch_webhook(
    target_url: str,
    secret: str,
    payload: dict[str, Any],
    *,
    timeout: float = TIEMPO_LIMITE_POR_DEFECTO,
    session: requests.Session | None = None,
) -> ResultadoDespacho:
    """Firma el payload y lo entrega por POST. Devuelve el resultado o lanza `WebhookDeliveryError`.

    Un 2xx es entrega confirmada. Cualquier otra cosa —4xx, 5xx, timeout, conexión rechazada— es un
    fallo de entrega y se informa con notas para que el registro forense conserve el contexto.
    """
    cuerpo = serializar_cuerpo(payload)
    timestamp = int(time.time())
    cabecera = cabecera_de_firma(secret, timestamp, cuerpo)
    cliente_http: Any = session if session is not None else requests

    inicio = time.perf_counter()
    try:
        respuesta = cliente_http.post(
            target_url,
            data=cuerpo,
            headers={"Content-Type": "application/json", CABECERA_FIRMA: cabecera},
            timeout=timeout,
        )
    except requests.RequestException as err:
        latencia_ms = (time.perf_counter() - inicio) * 1000
        error = WebhookDeliveryError(f"No se pudo entregar el webhook a {target_url}: {type(err).__name__}")
        error.add_note(f"detalle={err}")
        error.add_note(f"latencia_ms={latencia_ms:.1f}")
        error.add_note(f"timeout_s={timeout}")
        raise error from err

    latencia_ms = (time.perf_counter() - inicio) * 1000

    if not 200 <= respuesta.status_code < 300:
        error = WebhookDeliveryError(f"El destino {target_url} respondió HTTP {respuesta.status_code}.")
        error.add_note(f"target_url={target_url}")
        error.add_note(f"status_code={respuesta.status_code}")
        error.add_note(f"latencia_ms={latencia_ms:.1f}")
        error.add_note(f"{CABECERA_FIRMA}={cabecera}")
        raise error

    return ResultadoDespacho(status_code=respuesta.status_code, latency_ms=latencia_ms, timestamp=timestamp)


class DespachadorSQS:
    """Worker que consume la cola y despacha cada evento firmado a su destino.

    Responsabilidad única por mensaje: interpretar el cuerpo, despachar y —sólo si el destino
    confirmó— borrar el mensaje. Nunca borra ante un fallo ni ante un cuerpo corrupto: dejarlo en la
    cola es exactamente lo que alimenta el contador de recepciones que usa el `RedrivePolicy`.
    """

    def __init__(
        self,
        cliente: Any,
        queue_url: str,
        target_url: str,
        secret: str,
        logger: logging.Logger | None = None,
        *,
        wait_time: int = ESPERA_POR_DEFECTO,
        visibility_timeout: int = VISIBILIDAD_POR_DEFECTO,
        max_messages: int = MAX_MENSAJES_POR_DEFECTO,
        timeout: float = TIEMPO_LIMITE_POR_DEFECTO,
    ) -> None:
        self.cliente = cliente
        self.queue_url = queue_url
        self.target_url = target_url
        self.secret = secret
        self.logger = logger if logger is not None else logging.getLogger(NOMBRE_LOGGER)
        self.wait_time = wait_time
        self.visibility_timeout = visibility_timeout
        self.max_messages = max_messages
        self.timeout = timeout
        self._detenido = False
        self.entregados = 0
        self.fallidos = 0

    def stop(self) -> None:
        """Pide el corte ordenado del bucle. La interrupción se atiende entre lotes, no a mitad de uno."""
        self._detenido = True

    @property
    def detenido(self) -> bool:
        return self._detenido

    def _recibir(self) -> list[dict[str, Any]]:
        try:
            respuesta = self.cliente.receive_message(
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
        return respuesta.get("Messages", [])

    def _borrar(self, receipt_handle: str) -> None:
        try:
            self.cliente.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as err:
            error = SQSConnectionError("Error al borrar un mensaje ya entregado.")
            error.add_note(f"QueueUrl: {self.queue_url}")
            error.add_note(f"detalle={err}")
            raise error from err

    def _contexto(self, mensaje: dict[str, Any]) -> dict[str, Any]:
        atributos = mensaje.get("MessageAttributes") or {}
        traza = (atributos.get("TraceId") or {}).get("StringValue")
        message_id = mensaje.get("MessageId", "desconocido")
        recepciones = (mensaje.get("Attributes") or {}).get("ApproximateReceiveCount", "1")
        return {
            "trace_id": traza or message_id,
            "message_id": message_id,
            "retry_count": int(recepciones),
        }

    def procesar(self, mensaje: dict[str, Any]) -> bool:
        """Procesa un mensaje. Devuelve True sólo si se entregó y se borró."""
        contexto = self._contexto(mensaje)
        receipt_handle = mensaje.get("ReceiptHandle", "")

        try:
            cuerpo = json.loads(mensaje.get("Body", ""))
        except json.JSONDecodeError as err:
            # Se registra y se aísla el mensaje, igual que con un JSON que no es objeto: abortar el
            # lote acá cancelaría los mensajes válidos que vienen detrás en la misma recepción.
            error = CorruptedMessageError(f"El cuerpo del mensaje {contexto['message_id']} no es JSON válido.")
            error.add_note(f"detalle={err}")
            error.add_note("El mensaje queda en la cola: el RedrivePolicy lo derivará a la DLQ.")
            self.logger.error(
                "mensaje venenoso descartado para despacho",
                exc_info=error,
                extra=contexto,
            )
            self.fallidos += 1
            return False

        if not isinstance(cuerpo, dict):
            error = CorruptedMessageError(f"El cuerpo del mensaje {contexto['message_id']} no es un objeto JSON.")
            error.add_note("El mensaje queda en la cola: el RedrivePolicy lo derivará a la DLQ.")
            self.logger.error("mensaje venenoso descartado para despacho", exc_info=error, extra=contexto)
            self.fallidos += 1
            return False

        try:
            resultado = dispatch_webhook(self.target_url, self.secret, cuerpo, timeout=self.timeout)
        except WebhookDeliveryError as err:
            self.logger.error(
                "entrega fallida; el mensaje queda en la cola para el reintento de SQS",
                exc_info=err,
                extra={**contexto, "notes": list(getattr(err, "__notes__", ()))},
            )
            self.fallidos += 1
            return False

        self._borrar(receipt_handle)
        self.entregados += 1
        self.logger.info(
            "webhook entregado",
            extra={**contexto, "latency_ms": round(resultado.latency_ms, 1)},
        )
        return True

    def procesar_lote(self) -> int:
        """Consume un lote y devuelve cuántos mensajes quedaron entregados."""
        return sum(1 for mensaje in self._recibir() if self.procesar(mensaje))

    def ejecutar(self, iteraciones: int | None = None) -> int:
        """Bucle principal. `iteraciones` acota la corrida para pruebas y para el modo `--once`."""
        total = 0
        vueltas = 0
        while not self._detenido and (iteraciones is None or vueltas < iteraciones):
            total += self.procesar_lote()
            vueltas += 1
        return total
