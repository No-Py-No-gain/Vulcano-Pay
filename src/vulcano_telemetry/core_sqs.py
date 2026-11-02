"""
Proyecto Vulcano Pay
Modulo: core_sqs.py
Responsabilidad: Integrante 2

Mensajeria SQS mediante Boto3 y LocalStack.

Este modulo implementa el ciclo de vida de los eventos:
- Crear un cliente SQS local.
- Publicar eventos JSON con atributos de trazabilidad.
- Recibir mensajes mediante Long Polling.
- Eliminar explicitamente mensajes procesados.
- Identificar mensajes corruptos sin eliminarlos automaticamente.
- Conservar funciones compatibles con otros modulos del equipo.

Importante: recibir un mensaje NO significa eliminarlo. La eliminacion
se realiza mediante delete_message una vez confirmado su procesamiento.
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

# Logger compartido para registrar las operaciones del motor de eventos.
logger = logging.getLogger("vulcano_engine")


def get_sqs_client(endpoint_url: str = "http://localhost:4566"):
    """Crea y devuelve un cliente Boto3 para el servicio SQS de LocalStack.

    Args:
        endpoint_url (str): Direccion del servicio SQS emulado.

    Returns:
        Cliente Boto3 de SQS, configurado en us-east-1.

    Nota:
        Las credenciales test/test son exclusivamente para el entorno local.
        Crear el cliente no garantiza que LocalStack este disponible.
    """
    # La region y las credenciales son valores de prueba para LocalStack.
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _raise_sqs_error(operation: str, queue_url: str, error: Exception):
    """Transforma un error de Boto3 en SQSConnectionError.

    Args:
        operation (str): Operacion SQS que fallo.
        queue_url (str): URL de la cola involucrada.
        error (Exception): Excepcion original.

    Raises:
        SQSConnectionError: Incluye notas con contexto y conserva
            la causa original mediante ``raise ... from error``.
    """
    sqs_error = SQSConnectionError(
        f"Fallo en la operacion SQS: {operation}"
    )
    # Las notas agregan contexto util para diagnosticar el fallo.
    sqs_error.add_note(f"Operacion: {operation}")
    sqs_error.add_note(f"QueueUrl: {queue_url}")
    sqs_error.add_note(f"Error original: {type(error).__name__}")

    # Conserva la excepcion original como causa del error personalizado.
    raise sqs_error from error


def send_event(
    sqs_client,
    queue_url: str,
    event_data: dict,
    trace_id: str | None = None,
    event_topic: str = "payment.event",
    webhook_id: str | None = None,
) -> str:
    """Publica un diccionario como mensaje JSON en SQS.

    Args:
        sqs_client: Cliente Boto3 previamente creado.
        queue_url (str): URL de la cola de destino.
        event_data (dict): Contenido del evento a serializar.
        trace_id (str | None): Identificador de trazabilidad opcional.
        event_topic (str): Categoria del evento.

    Returns:
        str: MessageId asignado por SQS.

    Raises:
        ValueError: Si event_data no es un diccionario.
        TypeError: Si algun valor no es serializable a JSON.
        SQSConnectionError: Si Boto3 reporta un error capturado de SQS.

    Los atributos TraceId, EventTopic y Timestamp se envian por separado
    del cuerpo JSON, para facilitar el seguimiento de los eventos.
    """

    if not isinstance(event_data, dict):
        raise ValueError("event_data debe ser un diccionario.")

    # Genera un identificador UUID si no se proporciono uno.
    trace_id = trace_id or str(uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    # Convierte el diccionario a JSON conservando caracteres Unicode.
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
        # El cuerpo contiene el JSON; los atributos llevan la trazabilidad.
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
    Recibe y convierte mensajes SQS mediante Long Polling.

    Args:
        sqs_client: Cliente Boto3 de SQS.
        queue_url (str): URL de la cola.
        wait_time (int): Segundos de espera por mensajes (1 a 20).
        visibility_timeout (int): Segundos de invisibilidad temporal
            de los mensajes recibidos (0 a 43200).
        max_messages (int): Cantidad maxima de mensajes por consulta (1 a 10).

    Returns:
        list[dict]: Eventos con message_id, receipt_handle, body,
            attributes y receive_count.

    Raises:
        ValueError: Si algun parametro numerico esta fuera de rango.
        CorruptedMessageError: Si el cuerpo de un mensaje no es JSON valido.
        SQSConnectionError: Si falla una operacion de Boto3 capturada.

    No elimina mensajes. El consumidor debe confirmar su procesamiento
    antes de llamar a delete_message. Un mensaje corrupto tampoco se borra.
    """

    # SQS permite un maximo de 20 segundos de Long Polling.
    if not 1 <= wait_time <= 20:
        raise ValueError("wait_time debe estar entre 1 y 20.")

    if not 0 <= visibility_timeout <= 43200:
        raise ValueError(
            "visibility_timeout debe estar entre 0 y 43200."
        )

    if not 1 <= max_messages <= 10:
        raise ValueError("max_messages debe estar entre 1 y 10.")

    try:
        # Long Polling: espera hasta wait_time si no hay mensajes disponibles.
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

    # Si SQS no devuelve Messages, se procesa una lista vacia.
    for message in response.get("Messages", []):
        try:
            # El cuerpo recibido es texto JSON y se convierte a un objeto Python.
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
            # No se elimina el mensaje: SQS puede reintentarlo o enviarlo a DLQ
            # si la cola tiene configurada una politica de redireccion.
            raise corrupted from error

        # El ReceiptHandle se necesitara para eliminar el mensaje despues.
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
    """Solicita a SQS eliminar un mensaje mediante su ReceiptHandle.

    Args:
        sqs_client: Cliente Boto3 de SQS.
        queue_url (str): URL de la cola.
        receipt_handle (str): Identificador temporal de recepcion del mensaje.

    Returns:
        None.

    Raises:
        SQSConnectionError: Si Boto3 reporta un error capturado.

    Debe llamarse solo cuando el procesamiento del mensaje fue exitoso.
    """

    try:
        # La eliminacion requiere el ReceiptHandle de esta recepcion.
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
    Recibe un lote SQS y separa mensajes validos y corruptos.

    Args:
        sqs_client: Cliente Boto3 de SQS.
        queue_url (str): URL de la cola.
        wait_time (int): Espera de Long Polling entre 1 y 20 segundos.
        visibility_timeout (int): Invisibilidad entre 0 y 43200 segundos.
        max_messages (int): Numero maximo de mensajes entre 1 y 10.

    Returns:
        dict: Dos listas, ``valid`` con los eventos JSON decodificados
        y ``corrupted`` con identificadores, errores y recuentos de recepcion.

    Raises:
        ValueError: Si los parametros numericos son invalidos.
        SQSConnectionError: Si falla una operacion de Boto3 capturada.

    A diferencia de receive_events, no interrumpe el lote cuando un
    mensaje tiene JSON corrupto. Tampoco elimina mensajes automaticamente.
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

    # Separamos los mensajes correctos de los que tienen JSON invalido.
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
            # Conserva la causa original para inspeccion posterior.
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
            # Continuamos con el siguiente mensaje sin detener el lote.
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

    # Se devuelven ambas listas para que el consumidor decida como actuar.
    return {
        "valid": valid_events,
        "corrupted": corrupted_events,
    }

# ============================================================

# Compatibilidad con las interfaces que ya consumía el equipo
# ============================================================


def produce_event(sqs_client, queue_url: str, event_data: dict) -> str:
    """
    Publica un evento SQS mediante la interfaz compatible del equipo.

    Args:
        sqs_client: Cliente Boto3 configurado para SQS.
        queue_url (str): URL de la cola de destino.
        event_data (dict): Datos del evento a publicar.

    Returns:
        str: MessageId asignado por SQS.

    Raises:
        ValueError: Si event_data no es un diccionario.
        TypeError: Si los datos no son serializables a JSON.
        SQSConnectionError: Si falla una operacion SQS capturada.

    Funcionamiento:
        1. Valida que el evento sea un diccionario.
        2. Obtiene el tipo de evento desde event_topic.
        3. Obtiene webhook_id cuando esta presente.
        4. Delega la publicacion en send_event().

    Los atributos TraceId, EventTopic, Timestamp y, cuando
    corresponde, WebhookId se generan en send_event().

    Esta implementacion conserva el contrato de trazabilidad
    actualizado por el equipo.
    """

    # Comprueba que el evento tenga la estructura esperada.
    if not isinstance(event_data, dict):
        raise ValueError("event_data debe ser un diccionario.")

    # Centraliza la publicacion y los atributos de trazabilidad.
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
    """
    Recibe eventos SQS mediante la interfaz compatible del equipo.

    Args:
        sqs_client:
            Cliente Boto3 configurado para SQS.

        queue_url (str):
            URL de la cola desde donde se reciben mensajes.

        wait_time (int):
            Tiempo maximo de espera mediante Long Polling.

        max_messages (int):
            Cantidad maxima de mensajes por consulta.

    Returns:
        list[dict]:
            Lista de eventos con los siguientes campos:

            - message_id: identificador del mensaje.
            - receipt_handle: identificador para su eliminacion.
            - body: contenido JSON decodificado.
            - approx_receive_count: cantidad de recepciones.
            - corrupt: indica si el mensaje es invalido.
            - error: excepcion asociada, si es corrupto.

    Funcionamiento:
        1. Recibe un lote mediante receive_events_safe().
        2. Procesa los mensajes validos.
        3. Identifica los mensajes corruptos.
        4. Devuelve ambos tipos sin interrumpir el lote.

    Importante:
        Los mensajes no se eliminan automaticamente.

        Los mensajes corruptos permanecen en SQS para
        permitir reintentos y su posterior derivacion
        a la Dead Letter Queue (DLQ), cuando la cola
        tiene configurada una politica RedrivePolicy.
    """

    # Recibe los mensajes y los separa en validos y corruptos.
    batch = receive_events_safe(
        sqs_client,
        queue_url,
        wait_time=wait_time,
        max_messages=max_messages,
    )

    # Prepara los mensajes validos para el consumidor.
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

    # Incorpora los mensajes corruptos sin detener el procesamiento.
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

    # Devuelve todos los resultados sin eliminar mensajes de SQS.
    return events
