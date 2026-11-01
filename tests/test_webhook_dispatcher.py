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
    CABECERA_FIRMA,
    DespachadorSQS,
    dispatch_webhook,
    serializar_cuerpo,
)
from vulcano_telemetry.exceptions import WebhookDeliveryError
from vulcano_telemetry.logging_engine import CAMPOS_OPCIONALES, AsyncJSONFormatter
from vulcano_telemetry.webhook_crypto import verify_signature

SECRETO = "s3cr3t-de-prueba"
URL_COLA = "http://localhost:4566/000000000000/vulcano-events-queue"
ESPERA_MAXIMA = 5.0


class DestinoLocal:
    """Destino HTTP real en 127.0.0.1 con respuesta y demora configurables.

    No se mockea `requests`: la prueba de contrato de la firma necesita ver los bytes que de verdad
    salen por el socket y la cabecera que de verdad llega al receptor.
    """

    def __init__(self) -> None:
        self.peticiones: list[dict[str, Any]] = []
        self.status = 200
        self.delay = 0.0
        destino = self

        class Manejador(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                largo = int(self.headers.get("Content-Length") or 0)
                cuerpo = self.rfile.read(largo)
                destino.peticiones.append({"headers": dict(self.headers), "cuerpo": cuerpo})
                if destino.delay:
                    time.sleep(destino.delay)
                try:
                    self.send_response(destino.status)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                except (BrokenPipeError, ConnectionResetError):
                    # El cliente ya colgó la conexión: es justo lo que provoca el caso de timeout.
                    self.close_connection = True

            def log_message(self, fmt: str, *args: Any) -> None:
                """Silenciado a propósito: la salida de la suite sólo debe hablar por los asserts."""

        self._servidor = ThreadingHTTPServer(("127.0.0.1", 0), Manejador)
        self.url = f"http://127.0.0.1:{self._servidor.server_address[1]}/webhook"
        self._hilo = threading.Thread(target=self._servidor.serve_forever, name="destino-http", daemon=True)
        self._hilo.start()

    @property
    def ultima_peticion(self) -> dict[str, Any]:
        assert self.peticiones, "el destino no recibió ninguna petición"
        return self.peticiones[-1]

    def cerrar(self) -> None:
        self._servidor.shutdown()
        self._servidor.server_close()
        self._hilo.join(timeout=ESPERA_MAXIMA)


class ColectorDeRegistros(logging.Handler):
    """Handler en memoria: guarda los `LogRecord` para inspeccionar los extras del contrato S5."""

    def __init__(self) -> None:
        super().__init__()
        self.registros: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.registros.append(record)


class ClienteSQSFalso:
    """Doble de SQS: no habla con AWS, sólo registra las llamadas que hace el worker."""

    def __init__(self, mensajes: list[dict[str, Any]] | None = None) -> None:
        self.pendientes = list(mensajes or [])
        self.recepciones: list[dict[str, Any]] = []
        self.borrados: list[dict[str, Any]] = []

    def receive_message(self, **argumentos: Any) -> dict[str, Any]:
        self.recepciones.append(argumentos)
        lote, self.pendientes = self.pendientes, []
        return {"Messages": lote} if lote else {}

    def delete_message(self, **argumentos: Any) -> dict[str, Any]:
        self.borrados.append(argumentos)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture
def destino():
    """Servidor HTTP local vivo durante todo el caso; se cierra al finalizar (S6)."""
    servidor = DestinoLocal()
    yield servidor
    servidor.cerrar()


@pytest.fixture
def logger_pruebas():
    """Logger aislado con un handler en memoria, sin propagar al logger raíz."""
    logger = logging.getLogger("vulcano_pruebas_dispatcher")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers.clear()
    colector = ColectorDeRegistros()
    logger.addHandler(colector)
    yield logger, colector.registros
    logger.removeHandler(colector)
    logger.handlers.clear()


def _cabecera(peticion: dict[str, Any], nombre: str) -> str | None:
    """Valor de una cabecera HTTP recibida, sin depender de cómo la escribió el cliente."""
    for clave, valor in peticion["headers"].items():
        if clave.lower() == nombre.lower():
            return valor
    return None


def _mensaje(
    mensaje_id: str,
    cuerpo: str,
    ticket: str,
    *,
    recepciones: int = 1,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """Mensaje SQS con la forma que devuelve Boto3 (cuerpo, ticket e intentos de recepción)."""
    mensaje: dict[str, Any] = {
        "MessageId": mensaje_id,
        "Body": cuerpo,
        "ReceiptHandle": ticket,
        "Attributes": {"ApproximateReceiveCount": str(recepciones)},
    }
    if trace_id is not None:
        mensaje["MessageAttributes"] = {"TraceId": {"StringValue": trace_id}}
    return mensaje


def _worker(cliente: Any, destino: DestinoLocal, logger: logging.Logger, **opciones: Any) -> DespachadorSQS:
    return DespachadorSQS(cliente, URL_COLA, destino.url, SECRETO, logger, **opciones)


def test_el_receptor_verifica_la_firma_que_genera_el_despachador(destino):
    """Contrato rol 5 ↔ rol 3: lo que firma el despachador lo acepta `verify_signature`."""
    payload = {"evento": "pago.aprobado", "monto": 1500.5, "moneda": "ARS"}

    dispatch_webhook(destino.url, SECRETO, payload)

    peticion = destino.ultima_peticion
    cabecera = _cabecera(peticion, CABECERA_FIRMA)

    assert cabecera is not None, f"el POST no llevó la cabecera {CABECERA_FIRMA}"
    assert verify_signature(SECRETO, cabecera, peticion["cuerpo"]) is True


def test_se_firman_y_envian_los_mismos_bytes(destino):
    """El cuerpo que viaja es exactamente la serialización firmada, no una re-serialización."""
    payload = {
        "zeta": "último a propósito",
        "descripción": "Cobro con tarjeta — señas",
        "alfa": 1,
        "anidado": {"ñ": True, "lista": [3, 2, 1]},
    }

    dispatch_webhook(destino.url, SECRETO, payload)

    peticion = destino.ultima_peticion

    assert peticion["cuerpo"] == serializar_cuerpo(payload), (
        "el cuerpo enviado no es byte a byte el que se firmó: json= de requests lo re-serializaría"
    )
    assert json.loads(peticion["cuerpo"]) == payload
    assert _cabecera(peticion, "Content-Type") == "application/json"


def test_un_http_500_levanta_webhook_delivery_error_con_notas(destino):
    """Un 5xx es fallo de entrega y las notas deben conservar la URL y el código."""
    destino.status = 500

    with pytest.raises(WebhookDeliveryError) as capturado:
        dispatch_webhook(destino.url, SECRETO, {"evento": "pago.aprobado"})

    notas = "\n".join(capturado.value.__notes__)

    assert destino.url in notas, f"las notas no mencionan la URL del destino: {notas!r}"
    assert "500" in notas, f"las notas no mencionan el código HTTP 500: {notas!r}"


def test_un_timeout_del_destino_levanta_webhook_delivery_error(destino):
    """Un destino más lento que el timeout es un fallo de entrega, no un cuelgue del worker."""
    destino.delay = 0.6

    with pytest.raises(WebhookDeliveryError) as capturado:
        dispatch_webhook(destino.url, SECRETO, {"evento": "pago.aprobado"}, timeout=0.15)

    notas = "\n".join(capturado.value.__notes__)

    assert "timeout_s" in notas, f"las notas no registran el timeout usado: {notas!r}"


def test_el_worker_borra_el_mensaje_cuando_el_destino_confirma(destino, logger_pruebas):
    """Con 2xx el mensaje se borra: ya no vuelve a la cola ni puede terminar en la DLQ."""
    logger, _ = logger_pruebas
    cliente = ClienteSQSFalso([_mensaje("msg-1", json.dumps({"evento": "pago.aprobado"}), "rh-1")])
    worker = _worker(cliente, destino, logger)

    assert worker.procesar_lote() == 1, "el mensaje confirmado no se contó como entregado"
    assert len(cliente.borrados) == 1, "delete_message debe llamarse exactamente una vez"
    assert cliente.borrados[0]["ReceiptHandle"] == "rh-1"
    assert cliente.borrados[0]["QueueUrl"] == URL_COLA
    assert worker.entregados == 1
    assert worker.fallidos == 0


def test_el_worker_no_borra_cuando_el_destino_falla(destino, logger_pruebas):
    """No borrar es lo que hace que SQS reintente y el RedrivePolicy derive a la DLQ (S9)."""
    logger, _ = logger_pruebas
    destino.status = 500
    cliente = ClienteSQSFalso([_mensaje("msg-2", json.dumps({"evento": "pago.aprobado"}), "rh-2")])
    worker = _worker(cliente, destino, logger)

    assert worker.procesar_lote() == 0
    assert cliente.borrados == [], "un fallo de entrega no puede borrar el mensaje: se perdería la DLQ"
    assert worker.fallidos == 1, "el fallo no se contabilizó"
    assert worker.entregados == 0


def test_un_cuerpo_corrupto_no_aborta_el_lote_ni_se_borra(destino, logger_pruebas):
    """Un mensaje venenoso queda en la cola sin impedir que el resto del lote se entregue."""
    logger, _ = logger_pruebas
    cliente = ClienteSQSFalso(
        [
            _mensaje("corrupto", "no es json", "rh-corrupto"),
            _mensaje("valido", json.dumps({"evento": "pago.aprobado"}), "rh-valido"),
        ]
    )
    worker = _worker(cliente, destino, logger)

    entregados = worker.procesar_lote()

    assert entregados == 1, "un cuerpo corrupto no debe abortar el lote completo"
    assert [borrado["ReceiptHandle"] for borrado in cliente.borrados] == ["rh-valido"], (
        "sólo se borra el mensaje entregado; el corrupto debe quedar para la DLQ"
    )
    assert worker.fallidos == 1

    # Mismo trato para un JSON válido que no es objeto: tampoco es un evento despachable.
    cliente.pendientes = [
        _mensaje("texto", '"texto"', "rh-texto"),
        _mensaje("valido-2", json.dumps({"evento": "pago.rechazado"}), "rh-valido-2"),
    ]
    cliente.borrados.clear()

    assert worker.procesar_lote() == 1, "un JSON que no es objeto también debe aislarse, no abortar"
    assert [borrado["ReceiptHandle"] for borrado in cliente.borrados] == ["rh-valido-2"]


def test_el_worker_usa_long_polling_visibility_timeout_y_el_contador_de_reintentos(destino, logger_pruebas):
    """La recepción configura la cola y el registro lleva correlación, reintentos y latencia."""
    logger, registros = logger_pruebas
    cliente = ClienteSQSFalso(
        [
            _mensaje(
                "msg-3",
                json.dumps({"evento": "pago.aprobado"}),
                "rh-3",
                recepciones=3,
                trace_id="trace-xyz",
            )
        ]
    )
    worker = _worker(cliente, destino, logger, wait_time=7, visibility_timeout=45)

    assert worker.procesar_lote() == 1

    (recepcion,) = cliente.recepciones
    assert recepcion["WaitTimeSeconds"] == 7, "la recepción no pidió long polling"
    assert recepcion["VisibilityTimeout"] == 45, "la recepción no respetó el VisibilityTimeout de la cola"
    assert recepcion["AttributeNames"] == ["ApproximateReceiveCount"], (
        "sin ApproximateReceiveCount no hay contador de reintentos que auditar"
    )

    registro = registros[-1]

    assert registro.getMessage() == "webhook entregado"
    assert registro.trace_id == "trace-xyz"
    assert registro.message_id == "msg-3"
    assert registro.retry_count == 3, "retry_count debe salir del ApproximateReceiveCount"
    assert isinstance(registro.latency_ms, float)
    assert "retry_count" in CAMPOS_OPCIONALES

    datos = json.loads(AsyncJSONFormatter().format(registro))

    assert datos["retry_count"] == 3, "el formatter no emite retry_count aunque llegue por extra"
