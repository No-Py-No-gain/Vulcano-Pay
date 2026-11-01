"""Inyección de caos del rol 6 (S11): firmas alteradas, replay y mensajes venenosos.

Los tres primeros casos son unitarios: usan `TestClient` contra la app del receptor del rol 3 y el
rechazo ocurre antes de tocar SQS, así que no necesitan LocalStack. Los dos últimos son de
integración (`VULCANO_IT=1`) y hablan con LocalStack real en http://localhost:4566.

Los códigos esperados salen del mapeo acordado en `docs/contrato-interfaces.md` —firma inválida
`401`, timestamp fuera de la ventana anti-replay `403`—, pero el enunciado (S11) pide "401
Unauthorized / 403 Forbidden". Por eso los casos de rechazo verifican pertenencia a ese conjunto y
no fijan un código único.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from httpx import Response

from vulcano_telemetry.dispatcher import DespachadorSQS
from vulcano_telemetry.exceptions import WebhookTimestampError
from vulcano_telemetry.webhook_crypto import generate_signature, verify_signature
from vulcano_telemetry.webhook_receiver import QUEUE_URL, WEBHOOK_SECRET, app

ENDPOINT_URL = os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
RUTA_WEBHOOK = "/webhook"
CABECERA_FIRMA = "X-Webhook-Signature"

# El enunciado habla de "401 Unauthorized / 403 Forbidden": el caso de caos verifica el rechazo,
# no cuál de los dos códigos eligió el receptor.
CODIGOS_DE_RECHAZO = {401, 403}

ESPERA_MAXIMA_S = 5
INTENTOS_DERIVACION = 5
NOMBRE_COLA_PRUEBA = "vulcano-test-queue"
NOMBRE_DLQ_PRUEBA = "vulcano-test-dlq"
CUERPO_VENENOSO = "esto no es json"
SECRETO_INCORRECTO = "otra_clave_que_no_es_la_del_receptor"
# Códigos con los que SQS/LocalStack avisan que la cola no existe.
COLA_INEXISTENTE = {"QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue"}


def _cliente_sqs():
    """Cliente SQS de LocalStack, con las mismas credenciales simuladas que usa el bootstrap."""
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name=os.getenv("VULCANO_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _cuerpo(evento: str) -> bytes:
    """Cuerpo crudo del webhook: exactamente los bytes que se firman y se envían."""
    return json.dumps({"evento": evento, "id": str(uuid.uuid4())}, ensure_ascii=False).encode("utf-8")


def _cabecera(secret: str, timestamp: int, cuerpo: bytes) -> str:
    """Compone `t=<timestamp>,v1=<hmac>`, el formato que interpreta `verify_signature`."""
    return f"t={timestamp},v1={generate_signature(secret, timestamp, cuerpo)}"


def _enviar(cliente: TestClient, cuerpo: bytes, cabecera: str) -> Response:
    """POST al receptor con el cuerpo y la cabecera de firma dados."""
    return cliente.post(
        RUTA_WEBHOOK,
        content=cuerpo,
        headers={"Content-Type": "application/json", CABECERA_FIRMA: cabecera},
    )


def _afirmar_rechazo(respuesta: Response, motivo: str) -> None:
    assert respuesta.status_code in CODIGOS_DE_RECHAZO, (
        f"{motivo}: se esperaba 401 o 403 y el receptor respondió {respuesta.status_code} — {respuesta.text}"
    )


@pytest.fixture
def encolado_prohibido(monkeypatch):
    """Un webhook rechazado no debe tocar SQS: si el receptor intenta encolar, el caso falla."""

    def _prohibido(*_args, **_kwargs):
        raise AssertionError("el receptor intentó encolar un webhook rechazado")

    monkeypatch.setattr("vulcano_telemetry.webhook_receiver.produce_event", _prohibido)


def _mensajes_disponibles(cliente_sqs) -> int:
    """Mensajes disponibles en la cola, según el atributo de SQS (eventual, no exacto al instante)."""
    atributos = cliente_sqs.get_queue_attributes(
        QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(atributos.get("ApproximateNumberOfMessages", "0"))


def _esperar_incremento(cliente_sqs, base: int, delta: int, intentos: int = ESPERA_MAXIMA_S) -> int:
    """Reintenta acotado hasta ver `delta` mensajes nuevos, porque el conteo de SQS es eventual."""
    ultimo = base
    for _ in range(intentos):
        ultimo = _mensajes_disponibles(cliente_sqs)
        if ultimo >= base + delta:
            return ultimo
        time.sleep(1)
    return ultimo


def _limpiar_marcador(cliente_sqs, marcador: str, intentos: int = ESPERA_MAXIMA_S) -> bool:
    """Borra sólo el mensaje que encoló esta prueba y devuelve el resto a la cola intacto.

    La recepción usa `VisibilityTimeout=0` a propósito: cualquier mensaje ajeno que se haya leído
    vuelve a estar disponible de inmediato en lugar de quedar invisible durante la ventana de la cola.
    Devuelve `True` si encontró y borró el mensaje con el marcador. No espera entre vueltas: quien
    llama ya confirmó que el mensaje está disponible, así que la búsqueda es acotada y sin pausas.
    """
    for _ in range(intentos):
        respuesta = cliente_sqs.receive_message(
            QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
        )
        mensajes = respuesta.get("Messages", [])
        for mensaje in mensajes:
            if marcador in mensaje.get("Body", ""):
                cliente_sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=mensaje["ReceiptHandle"])
                return True
        if not mensajes:
            return False
    return False


def _borrar_cola_si_existe(cliente_sqs, nombre: str) -> None:
    """Idempotencia del fixture: una corrida anterior interrumpida no debe romper esta."""
    try:
        url = cliente_sqs.get_queue_url(QueueName=nombre)["QueueUrl"]
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") not in COLA_INEXISTENTE:
            raise
        return
    cliente_sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def colas_de_caos():
    """Colas propias de la prueba de veneno: la demo (`vulcano-events-queue`) no se toca.

    `maxReceiveCount=1` y `VisibilityTimeout=0` son deliberados: el primer recibo fallido deja el
    mensaje visible otra vez y el segundo recibo ya supera el umbral, así que la derivación a la DLQ
    se observa dentro del test en vez de esperar la ventana de visibilidad de la demo.
    """
    cliente_sqs = _cliente_sqs()
    _borrar_cola_si_existe(cliente_sqs, NOMBRE_COLA_PRUEBA)
    _borrar_cola_si_existe(cliente_sqs, NOMBRE_DLQ_PRUEBA)

    url_dlq = cliente_sqs.create_queue(
        QueueName=NOMBRE_DLQ_PRUEBA, Attributes={"VisibilityTimeout": "0"}
    )["QueueUrl"]
    arn_dlq = cliente_sqs.get_queue_attributes(QueueUrl=url_dlq, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    redrive = json.dumps({"deadLetterTargetArn": arn_dlq, "maxReceiveCount": "1"})
    url_cola = cliente_sqs.create_queue(
        QueueName=NOMBRE_COLA_PRUEBA,
        Attributes={"VisibilityTimeout": "0", "RedrivePolicy": redrive},
    )["QueueUrl"]

    yield cliente_sqs, url_cola, url_dlq

    for url in (url_cola, url_dlq):
        cliente_sqs.delete_queue(QueueUrl=url)


def _esperar_derivacion(cliente_sqs, url_cola: str, url_dlq: str, intentos: int = INTENTOS_DERIVACION) -> str:
    """Devuelve el cuerpo que llegó a la DLQ, reintentando la recepción de forma acotada.

    Cada recepción con `VisibilityTimeout=0` incrementa el contador de recepciones: al superar
    `maxReceiveCount=1` el `RedrivePolicy` mueve el mensaje a la DLQ. El bucle es corto a propósito;
    si la derivación no ocurre en cinco intentos, el test falla en vez de colgarse.
    """
    for _ in range(intentos):
        cliente_sqs.receive_message(
            QueueUrl=url_cola, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0
        )
        mensajes = cliente_sqs.receive_message(
            QueueUrl=url_dlq, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
        ).get("Messages", [])
        if mensajes:
            return mensajes[0]["Body"]
        time.sleep(1)
    pytest.fail(
        f"el mensaje venenoso no llegó a {NOMBRE_DLQ_PRUEBA} en {intentos} intentos de recepción"
    )


# --- Casos unitarios: el rechazo ocurre antes de tocar SQS (S11) ---------------------------------


def test_una_firma_alterada_se_rechaza(encolado_prohibido):
    """Un solo carácter distinto en el hex de la firma invalida la autenticación del webhook."""
    cliente = TestClient(app)
    cuerpo = _cuerpo("caos.firma_alterada")
    timestamp = int(time.time())
    firma = generate_signature(WEBHOOK_SECRET, timestamp, cuerpo)
    alterada = ("0" if firma[0] != "0" else "1") + firma[1:]

    respuesta = _enviar(cliente, cuerpo, f"t={timestamp},v1={alterada}")

    assert alterada != firma, "la firma alterada quedó igual a la original: el caso no prueba nada"
    _afirmar_rechazo(respuesta, "una firma HMAC alterada debe rechazarse")


def test_un_secreto_incorrecto_se_rechaza(encolado_prohibido):
    """Cabecera bien formada pero firmada con otra clave: el receptor no puede autenticarla."""
    cliente = TestClient(app)
    cuerpo = _cuerpo("caos.secreto_incorrecto")
    timestamp = int(time.time())

    respuesta = _enviar(cliente, cuerpo, _cabecera(SECRETO_INCORRECTO, timestamp, cuerpo))

    _afirmar_rechazo(respuesta, "una firma hecha con un secreto ajeno debe rechazarse")


def test_un_timestamp_de_hace_una_hora_se_rechaza(encolado_prohibido):
    """Replay de S11: firma correcta con `t` de hace una hora, fuera de la ventana anti-replay."""
    cliente = TestClient(app)
    cuerpo = _cuerpo("caos.replay")
    timestamp = int(time.time()) - 3600

    respuesta = _enviar(cliente, cuerpo, _cabecera(WEBHOOK_SECRET, timestamp, cuerpo))

    _afirmar_rechazo(respuesta, "un webhook con timestamp de hace una hora debe rechazarse por replay")


# --- Casos de integración: LocalStack real (S9, S11) ---------------------------------------------


@pytest.mark.integration
def test_un_webhook_valido_se_acepta_y_encola():
    """Firma correcta: 202 y un mensaje nuevo disponible en la cola de la demo."""
    cliente_sqs = _cliente_sqs()
    cuerpo = _cuerpo("caos.valido")
    marcador = json.loads(cuerpo)["id"]
    antes = _mensajes_disponibles(cliente_sqs)

    respuesta = _enviar(TestClient(app), cuerpo, _cabecera(WEBHOOK_SECRET, int(time.time()), cuerpo))

    try:
        assert respuesta.status_code == 202, (
            f"un webhook con firma válida debe aceptarse con 202 y respondió {respuesta.status_code} — {respuesta.text}"
        )
        despues = _esperar_incremento(cliente_sqs, antes, 1)

        assert despues >= antes + 1, (
            f"no apareció el mensaje nuevo en la cola: disponibles antes={antes}, después={despues}"
        )
        assert _limpiar_marcador(cliente_sqs, marcador), (
            "el mensaje nuevo de la cola no es el que envió esta prueba: el marcador no apareció"
        )
    finally:
        _limpiar_marcador(cliente_sqs, marcador)


@pytest.mark.integration
def test_un_mensaje_venenoso_termina_en_la_dlq(colas_de_caos):
    """Un cuerpo que no es JSON se descarta, no se borra y el RedrivePolicy lo deriva a la DLQ."""
    cliente_sqs, url_cola, url_dlq = colas_de_caos
    cliente_sqs.send_message(QueueUrl=url_cola, MessageBody=CUERPO_VENENOSO)

    logger = logging.getLogger("vulcano_caos")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    despachador = DespachadorSQS(
        cliente_sqs,
        url_cola,
        # El destino no importa: el cuerpo corrupto se descarta antes de cualquier POST.
        "http://127.0.0.1:1/webhook",
        "secreto-de-caos",
        logger,
        wait_time=1,
        visibility_timeout=0,
    )

    entregados = despachador.procesar_lote()

    assert entregados == 0, "un cuerpo que no es JSON no puede contarse como entregado"
    assert despachador.fallidos == 1, "el mensaje venenoso no quedó registrado como fallo"
    assert despachador.entregados == 0, "no hubo entrega real: el destino no debe tocarse"

    cuerpo_en_dlq = _esperar_derivacion(cliente_sqs, url_cola, url_dlq)

    assert cuerpo_en_dlq == CUERPO_VENENOSO, (
        f"el cuerpo derivado a la DLQ no es el original intacto: {cuerpo_en_dlq!r}"
    )

    restantes = cliente_sqs.receive_message(
        QueueUrl=url_cola, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
    ).get("Messages", [])

    assert restantes == [], f"el mensaje venenoso sigue en {NOMBRE_COLA_PRUEBA}: {restantes!r}"


def test_verify_signature_levanta_webhook_timestamp_error_con_un_timestamp_viejo():
    """S11 pide el rechazo *por WebhookTimestampError*, no sólo el código HTTP del receptor.

    La firma se genera correctamente para ese timestamp, así que la única razón para rechazarla es
    la ventana anti-replay: si el orden de las validaciones se invirtiera, este test fallaría.
    """
    cuerpo = b'{"transaction_id": "tx_replay"}'
    hace_una_hora = int(time.time()) - 3600
    firma = generate_signature(WEBHOOK_SECRET, hace_una_hora, cuerpo)

    with pytest.raises(WebhookTimestampError) as capturado:
        verify_signature(WEBHOOK_SECRET, f"t={hace_una_hora},v1={firma}", cuerpo)

    assert "tolerancia" in str(capturado.value), (
        f"la excepción no describe el problema de ventana temporal: {capturado.value}"
    )
