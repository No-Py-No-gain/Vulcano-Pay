
"""
Proyecto Vulcano Pay
Modulo: core_sqs.py
Responsabilidad: Integrante 2

Mensajeria SQS mediante Boto3 y LocalStack.
Publicacion, recepcion y eliminacion de eventos.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .exceptions import (
    CorruptedMessageError,
    SQSConnectionError,
)

logger = logging.getLogger("vulcano_engine")


def get_sqs_client(endpoint_url: str = "http://localhost:4566"):
    """Crea un cliente SQS configurado para LocalStack."""
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _raise_sqs_error(operation: str, queue_url: str, error: Exception):
    """Traduce errores de Boto3 a excepciones de Vulcano."""
    sqs_error = SQSConnectionError(
        f"Fallo en la operacion SQS: {operation}"
    )
    sqs_error.add_note(f"Operacion: {operation}")
    sqs_error.add_note(f"QueueUrl: {queue_url}")
    sqs_error.add_note(f"Error original: {type(error).__name__}")

    raise sqs_error from error


def send_event(
    sqs_client,
    queue_url: str,
    event_data: dict,
    trace_id: str | None = None,
    event_topic: str = "payment.event",
    webhook_id: str | None = None,
) -> str:
    """Publica un evento JSON con atributos de trazabilidad."""

    if not isinstance(event_data, dict):
        raise ValueError("event_data debe ser un diccionario.")

    trace_id = trace_id or str(uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    payload = json.dumps(event_data, ensure_ascii=False)

    message_attributes = {
        "TraceId": {
            "DataType": "String",
            "StringValue": trace_id,
        },
        "EventTopic": {
            "DataType": "String",
            "StringValue": event_topic,
        },
        "Timestamp": {
            "DataType": "String",
            "StringValue": timestamp,
        },
    }

    if webhook_id:
        message_attributes["WebhookId"] = {
            "DataType": "String",
            "StringValue": webhook_id,
        }

    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=payload,
            MessageAttributes=message_attributes,
        )

        message_id = response["MessageId"]

        logger.info(
            "Evento publicado correctamente en SQS",
            extra={
                "message_id": message_id,
                "trace_id": trace_id,
                "event_topic": event_topic,
                "webhook_id": webhook_id,
            },
        )

        return message_id

    except (ClientError, BotoCoreError) as error:
        _raise_sqs_error("send_message", queue_url, error)


def receive_events(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    visibility_timeout: int = 30,
    max_messages: int = 5,
) -> list[dict]:
    """
    Recibe eventos SQS mediante Long Polling.

    No elimina mensajes. El despachador debe confirmar
    su procesamiento antes de llamar a delete_message.
    """

    if not 1 <= wait_time <= 20:
        raise ValueError("wait_time debe estar entre 1 y 20.")

    if not 0 <= visibility_timeout <= 43200:
        raise ValueError(
            "visibility_timeout debe estar entre 0 y 43200."
        )

    if not 1 <= max_messages <= 10:
        raise ValueError("max_messages debe estar entre 1 y 10.")

    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=["All"],
        )

    except (ClientError, BotoCoreError) as error:
        _raise_sqs_error("receive_message", queue_url, error)

    events = []

    for message in response.get("Messages", []):
        try:
            body = json.loads(message["Body"])
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            corrupted = CorruptedMessageError(
                f"Mensaje SQS corrupto: {message['MessageId']}"
            )
            corrupted.add_note(
                f"MessageId: {message['MessageId']}"
            )
            corrupted.add_note(
                "El mensaje no se elimina para permitir "
                "su reintento o derivacion a la DLQ."
            )
            raise corrupted from error

        events.append({
            "message_id": message["MessageId"],
            "receipt_handle": message["ReceiptHandle"],
            "body": body,
            "attributes": message.get("MessageAttributes", {}),
            "receive_count": message.get(
                "Attributes", {}
            ).get("ApproximateReceiveCount"),
        })

    logger.info(
        "Recepcion SQS completada",
        extra={"event_count": len(events)},
    )

    return events


def delete_message(
    sqs_client,
    queue_url: str,
    receipt_handle: str,
) -> None:
    """Elimina un mensaje despues de procesarlo correctamente."""

    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )

        logger.info("Mensaje eliminado correctamente de SQS")

    except (ClientError, BotoCoreError) as error:
        _raise_sqs_error("delete_message", queue_url, error)

def receive_events_safe(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    visibility_timeout: int = 30,
    max_messages: int = 5,
) -> dict:
    """
    Recibe lotes SQS sin interrumpir el procesamiento
    cuando encuentra mensajes con JSON corrupto.

    Retorna:
        {
            "valid": [...],
            "corrupted": [...]
        }

    No elimina mensajes automaticamente.
    """

    if not isinstance(wait_time, int) or not 1 <= wait_time <= 20:
        raise ValueError("wait_time debe estar entre 1 y 20.")

    if (
        not isinstance(visibility_timeout, int)
        or not 0 <= visibility_timeout <= 43200
    ):
        raise ValueError(
            "visibility_timeout debe estar entre 0 y 43200."
        )

    if not isinstance(max_messages, int) or not 1 <= max_messages <= 10:
        raise ValueError("max_messages debe estar entre 1 y 10.")

    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=["All"],
        )

    except (ClientError, BotoCoreError) as error:
        _raise_sqs_error("receive_message", queue_url, error)

    valid_events = []
    corrupted_events = []

    for message in response.get("Messages", []):
        message_id = message["MessageId"]

        try:
            body = json.loads(message["Body"])

        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            corrupted = CorruptedMessageError(
                f"Mensaje SQS corrupto: {message_id}"
            )
            corrupted.add_note(f"MessageId: {message_id}")
            corrupted.add_note(
                "El mensaje no fue eliminado de SQS."
            )
            corrupted.__cause__ = error

            corrupted_events.append({
                "message_id": message_id,
                "error": corrupted,
                "receive_count": message.get(
                    "Attributes", {}
                ).get("ApproximateReceiveCount"),
            })

            logger.warning(
                "Mensaje SQS corrupto detectado: %s",
                message_id,
            )
            continue

        valid_events.append({
            "message_id": message_id,
            "receipt_handle": message["ReceiptHandle"],
            "body": body,
            "attributes": message.get("MessageAttributes", {}),
            "receive_count": message.get(
                "Attributes", {}
            ).get("ApproximateReceiveCount"),
        })

    return {
        "valid": valid_events,
        "corrupted": corrupted_events,
    }

# ============================================================
# Compatibilidad con las interfaces que ya consumía el equipo
# ============================================================

def produce_event(sqs_client, queue_url: str, event_data: dict) -> str:
    """Publica un evento conservando la interfaz original y delega en `send_event`.

    La CLI (`produce-sqs`) y el receptor publican por acá, así que los atributos de trazabilidad
    (C2 del contrato de interfaces) se inyectan en un solo lugar: `TraceId`, `EventTopic`,
    `Timestamp` y, cuando el evento lo trae, `WebhookId`.
    """
    if not isinstance(event_data, dict):
        raise ValueError("event_data debe ser un diccionario.")

    return send_event(
        sqs_client,
        queue_url,
        event_data,
        event_topic=event_data.get("event_topic", "payment.event"),
        webhook_id=event_data.get("webhook_id"),
    )


def consume_events(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    max_messages: int = 5,
) -> list[dict]:
    """Contrato estable de consumo (C3 y C4 del contrato de interfaces).

    Nunca aborta el lote: cada mensaje vuelve con `approx_receive_count` y con la marca `corrupt`.
    Un cuerpo inservible viaja con `body=None`, `corrupt=True` y la excepción en `error`, y **no** se
    borra: el `VisibilityTimeout` lo devuelve a la cola y el `RedrivePolicy` lo deriva a la DLQ.
    """
    batch = receive_events_safe(
        sqs_client,
        queue_url,
        wait_time=wait_time,
        max_messages=max_messages,
    )

    events = [
        {
            "message_id": event["message_id"],
            "receipt_handle": event["receipt_handle"],
            "body": event["body"],
            "approx_receive_count": int(event.get("receive_count") or 1),
            "corrupt": False,
        }
        for event in batch["valid"]
    ]

    events.extend(
        {
            "message_id": event["message_id"],
            "receipt_handle": event.get("receipt_handle"),
            "body": None,
            "approx_receive_count": int(event.get("receive_count") or 1),
            "corrupt": True,
            "error": event["error"],
        }
        for event in batch["corrupted"]
    )

    return events
