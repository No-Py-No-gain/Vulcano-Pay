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

from vulcano_telemetry.dispatcher import SQSDispatcher
from vulcano_telemetry.exceptions import WebhookTimestampError
from vulcano_telemetry.webhook_crypto import generate_signature, verify_signature
from vulcano_telemetry.webhook_receiver import QUEUE_URL, WEBHOOK_SECRET, app

ENDPOINT_URL = os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
WEBHOOK_PATH = "/webhook"
SIGNATURE_HEADER = "X-Webhook-Signature"

# El enunciado habla de "401 Unauthorized / 403 Forbidden": el caso de caos verifica el rechazo,
# no cuál de los dos códigos eligió el receptor.
REJECTION_CODES = {401, 403}

MAX_WAIT_SECONDS = 5
REDRIVE_ATTEMPTS = 5
TEST_QUEUE_NAME = "vulcano-test-queue"
TEST_DLQ_NAME = "vulcano-test-dlq"
POISON_BODY = "esto no es json"
WRONG_SECRET = "otra_clave_que_no_es_la_del_receptor"
# Códigos con los que SQS/LocalStack avisan que la cola no existe.
MISSING_QUEUE_CODES = {"QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue"}


def _sqs_client():
    """Cliente SQS de LocalStack, con las mismas credenciales simuladas que usa el bootstrap."""
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name=os.getenv("VULCANO_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _body(event: str) -> bytes:
    """Cuerpo crudo del webhook: exactamente los bytes que se firman y se envían."""
    return json.dumps({"evento": event, "id": str(uuid.uuid4())}, ensure_ascii=False).encode("utf-8")


def _signature_header(secret: str, timestamp: int, body: bytes) -> str:
    """Compone `t=<timestamp>,v1=<hmac>`, el formato que interpreta `verify_signature`."""
    return f"t={timestamp},v1={generate_signature(secret, timestamp, body)}"


def _send(client: TestClient, body: bytes, header: str) -> Response:
    """POST al receptor con el cuerpo y la cabecera de firma dados."""
    return client.post(
        WEBHOOK_PATH,
        content=body,
        headers={"Content-Type": "application/json", SIGNATURE_HEADER: header},
    )


def _assert_rejected(response: Response, reason: str) -> None:
    assert response.status_code in REJECTION_CODES, (
        f"{reason}: se esperaba 401 o 403 y el receptor respondió {response.status_code} — {response.text}"
    )


@pytest.fixture
def enqueue_forbidden(monkeypatch):
    """Un webhook rechazado no debe tocar SQS: si el receptor intenta encolar, el caso falla."""

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("el receptor intentó encolar un webhook rechazado")

    monkeypatch.setattr("vulcano_telemetry.webhook_receiver.produce_event", _forbidden)


def _available_messages(sqs_client) -> int:
    """Mensajes disponibles en la cola, según el atributo de SQS (eventual, no exacto al instante)."""
    attributes = sqs_client.get_queue_attributes(
        QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attributes.get("ApproximateNumberOfMessages", "0"))


def _wait_for_increase(sqs_client, base: int, delta: int, attempts: int = MAX_WAIT_SECONDS) -> int:
    """Reintenta acotado hasta ver `delta` mensajes nuevos, porque el conteo de SQS es eventual."""
    last = base
    for _ in range(attempts):
        last = _available_messages(sqs_client)
        if last >= base + delta:
            return last
        time.sleep(1)
    return last


def _clear_marker(sqs_client, marker: str, attempts: int = MAX_WAIT_SECONDS) -> bool:
    """Borra sólo el mensaje que encoló esta prueba y devuelve el resto a la cola intacto.

    La recepción usa `VisibilityTimeout=0` a propósito: cualquier mensaje ajeno que se haya leído
    vuelve a estar disponible de inmediato en lugar de quedar invisible durante la ventana de la cola.
    Devuelve `True` si encontró y borró el mensaje con el marcador. No espera entre vueltas: quien
    llama ya confirmó que el mensaje está disponible, así que la búsqueda es acotada y sin pausas.
    """
    for _ in range(attempts):
        response = sqs_client.receive_message(
            QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
        )
        messages = response.get("Messages", [])
        for message in messages:
            if marker in message.get("Body", ""):
                sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
                return True
        if not messages:
            return False
    return False


def _delete_queue_if_exists(sqs_client, name: str) -> None:
    """Idempotencia del fixture: una corrida anterior interrumpida no debe romper esta."""
    try:
        url = sqs_client.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") not in MISSING_QUEUE_CODES:
            raise
        return
    sqs_client.delete_queue(QueueUrl=url)


@pytest.fixture
def chaos_queues():
    """Colas propias de la prueba de veneno: la demo (`vulcano-events-queue`) no se toca.

    `maxReceiveCount=1` y `VisibilityTimeout=0` son deliberados: el primer recibo fallido deja el
    mensaje visible otra vez y el segundo recibo ya supera el umbral, así que la derivación a la DLQ
    se observa dentro del test en vez de esperar la ventana de visibilidad de la demo.
    """
    sqs_client = _sqs_client()
    _delete_queue_if_exists(sqs_client, TEST_QUEUE_NAME)
    _delete_queue_if_exists(sqs_client, TEST_DLQ_NAME)

    dlq_url = sqs_client.create_queue(
        QueueName=TEST_DLQ_NAME, Attributes={"VisibilityTimeout": "0"}
    )["QueueUrl"]
    dlq_arn = sqs_client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"})
    queue_url = sqs_client.create_queue(
        QueueName=TEST_QUEUE_NAME,
        Attributes={"VisibilityTimeout": "0", "RedrivePolicy": redrive},
    )["QueueUrl"]

    yield sqs_client, queue_url, dlq_url

    for url in (queue_url, dlq_url):
        sqs_client.delete_queue(QueueUrl=url)


def _wait_for_redrive(sqs_client, queue_url: str, dlq_url: str, attempts: int = REDRIVE_ATTEMPTS) -> str:
    """Devuelve el cuerpo que llegó a la DLQ, reintentando la recepción de forma acotada.

    Cada recepción con `VisibilityTimeout=0` incrementa el contador de recepciones: al superar
    `maxReceiveCount=1` el `RedrivePolicy` mueve el mensaje a la DLQ. El bucle es corto a propósito;
    si la derivación no ocurre en cinco intentos, el test falla en vez de colgarse.
    """
    for _ in range(attempts):
        sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0
        )
        messages = sqs_client.receive_message(
            QueueUrl=dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
        ).get("Messages", [])
        if messages:
            return messages[0]["Body"]
        time.sleep(1)
    pytest.fail(
        f"el mensaje venenoso no llegó a {TEST_DLQ_NAME} en {attempts} intentos de recepción"
    )


# --- Casos unitarios: el rechazo ocurre antes de tocar SQS (S11) ---------------------------------


def test_tampered_signature_is_rejected(enqueue_forbidden):
    """Un solo carácter distinto en el hex de la firma invalida la autenticación del webhook."""
    client = TestClient(app)
    body = _body("caos.firma_alterada")
    timestamp = int(time.time())
    signature = generate_signature(WEBHOOK_SECRET, timestamp, body)
    tampered = ("0" if signature[0] != "0" else "1") + signature[1:]

    response = _send(client, body, f"t={timestamp},v1={tampered}")

    assert tampered != signature, "la firma alterada quedó igual a la original: el caso no prueba nada"
    _assert_rejected(response, "una firma HMAC alterada debe rechazarse")


def test_wrong_secret_is_rejected(enqueue_forbidden):
    """Cabecera bien formada pero firmada con otra clave: el receptor no puede autenticarla."""
    client = TestClient(app)
    body = _body("caos.secreto_incorrecto")
    timestamp = int(time.time())

    response = _send(client, body, _signature_header(WRONG_SECRET, timestamp, body))

    _assert_rejected(response, "una firma hecha con un secreto ajeno debe rechazarse")


def test_one_hour_old_timestamp_is_rejected(enqueue_forbidden):
    """Replay de S11: firma correcta con `t` de hace una hora, fuera de la ventana anti-replay."""
    client = TestClient(app)
    body = _body("caos.replay")
    timestamp = int(time.time()) - 3600

    response = _send(client, body, _signature_header(WEBHOOK_SECRET, timestamp, body))

    _assert_rejected(response, "un webhook con timestamp de hace una hora debe rechazarse por replay")


# --- Casos de integración: LocalStack real (S9, S11) ---------------------------------------------


@pytest.mark.integration
def test_valid_webhook_is_accepted_and_enqueued():
    """Firma correcta: 202 y un mensaje nuevo disponible en la cola de la demo."""
    sqs_client = _sqs_client()
    body = _body("caos.valido")
    marker = json.loads(body)["id"]
    before = _available_messages(sqs_client)

    response = _send(TestClient(app), body, _signature_header(WEBHOOK_SECRET, int(time.time()), body))

    try:
        assert response.status_code == 202, (
            f"un webhook con firma válida debe aceptarse con 202 y respondió {response.status_code} — {response.text}"
        )
        after = _wait_for_increase(sqs_client, before, 1)

        assert after >= before + 1, (
            f"no apareció el mensaje nuevo en la cola: disponibles antes={before}, después={after}"
        )
        assert _clear_marker(sqs_client, marker), (
            "el mensaje nuevo de la cola no es el que envió esta prueba: el marcador no apareció"
        )
    finally:
        _clear_marker(sqs_client, marker)


@pytest.mark.integration
def test_poison_message_ends_in_the_dlq(chaos_queues):
    """Un cuerpo que no es JSON se descarta, no se borra y el RedrivePolicy lo deriva a la DLQ."""
    sqs_client, queue_url, dlq_url = chaos_queues
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=POISON_BODY)

    logger = logging.getLogger("vulcano_chaos")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

    dispatcher = SQSDispatcher(
        sqs_client,
        queue_url,
        # El destino no importa: el cuerpo corrupto se descarta antes de cualquier POST.
        "http://127.0.0.1:1/webhook",
        "secreto-de-caos",
        logger,
        wait_time=1,
        visibility_timeout=0,
    )

    delivered = dispatcher.process_batch()

    assert delivered == 0, "un cuerpo que no es JSON no puede contarse como entregado"
    assert dispatcher.failed == 1, "el mensaje venenoso no quedó registrado como fallo"
    assert dispatcher.delivered == 0, "no hubo entrega real: el destino no debe tocarse"

    body_in_dlq = _wait_for_redrive(sqs_client, queue_url, dlq_url)

    assert body_in_dlq == POISON_BODY, (
        f"el cuerpo derivado a la DLQ no es el original intacto: {body_in_dlq!r}"
    )

    remaining = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0, VisibilityTimeout=0
    ).get("Messages", [])

    assert remaining == [], f"el mensaje venenoso sigue en {TEST_QUEUE_NAME}: {remaining!r}"


def test_verify_signature_raises_webhook_timestamp_error_for_an_old_timestamp():
    """S11 pide el rechazo *por WebhookTimestampError*, no sólo el código HTTP del receptor.

    La firma se genera correctamente para ese timestamp, así que la única razón para rechazarla es
    la ventana anti-replay: si el orden de las validaciones se invirtiera, este test fallaría.
    """
    body = b'{"transaction_id": "tx_replay"}'
    one_hour_ago = int(time.time()) - 3600
    signature = generate_signature(WEBHOOK_SECRET, one_hour_ago, body)

    with pytest.raises(WebhookTimestampError) as raised:
        verify_signature(WEBHOOK_SECRET, f"t={one_hour_ago},v1={signature}", body)

    assert "tolerancia" in str(raised.value), (
        f"la excepción no describe el problema de ventana temporal: {raised.value}"
    )
