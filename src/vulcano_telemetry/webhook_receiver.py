
"""
Proyecto Vulcano Pay
Modulo: webhook_receiver.py
Responsabilidad: Integrante 3

Servidor HTTP receptor de webhooks con FastAPI.
Verifica HMAC-SHA256 y publica eventos en SQS.
"""

import json
import logging
import os
from contextlib import asynccontextmanager

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request, status
from starlette.concurrency import run_in_threadpool

from .core_sqs import get_sqs_client, send_event
from .exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .webhook_crypto import verify_signature

logger = logging.getLogger("vulcano_engine")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa SQS y obtiene la URL de la cola una vez."""

    sqs_client = get_sqs_client(
        os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
    )

    try:
        queue_name = os.getenv(
            "VULCANO_QUEUE_NAME",
            "vulcano-events-queue",
        )

        try:
            queue_response = await run_in_threadpool(
                sqs_client.get_queue_url,
                QueueName=queue_name,
            )

        except (ClientError, BotoCoreError) as error:
            # Sin traducción, la CLI no puede mapear esto al código de salida 4.
            sqs_error = SQSConnectionError(
                f"No se pudo resolver la cola '{queue_name}' en SQS."
            )
            sqs_error.add_note("Operacion: get_queue_url")
            sqs_error.add_note(f"QueueName: {queue_name}")
            raise sqs_error from error

        app.state.sqs_client = sqs_client
        app.state.queue_url = queue_response["QueueUrl"]

        logger.info("Conexion SQS inicializada")

        yield

    finally:
        sqs_client.close()



app = FastAPI(
    title="Vulcano Pay - Webhook Receiver",
    lifespan=lifespan,
)


@app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def receive_webhook(request: Request):
    """
    Recibe un webhook firmado.

    1. Lee los bytes originales.
    2. Verifica timestamp y firma HMAC.
    3. Valida el JSON.
    4. Publica el evento en SQS.
    5. Devuelve HTTP 202.
    """

    secret = os.getenv("VULCANO_WEBHOOK_SECRET")

    if not secret or len(secret) < 16:
        logger.error("Clave HMAC no configurada correctamente")
        raise HTTPException(
            status_code=503,
            detail="Servicio no configurado.",
        )

    raw_body = await request.body()

    signature_header = request.headers.get(
        "X-Webhook-Signature", ""
    )

    try:
        verify_signature(
            secret=secret,
            header_value=signature_header,
            payload_bytes=raw_body,
        )

    except WebhookSignatureError as error:
        logger.warning(
            "Webhook rechazado por firma: %s",
            type(error).__name__,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Firma HMAC invalida o ausente.",
        ) from error

    except WebhookTimestampError as error:
        # 403 y no 401: el replay no es un problema de autenticidad sino de ventana temporal.
        logger.warning(
            "Webhook rechazado por timestamp: %s",
            type(error).__name__,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Timestamp fuera de la ventana permitida.",
        ) from error

    try:
        payload = json.loads(raw_body)

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="JSON invalido.",
        ) from error

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="El evento debe ser un objeto JSON.",
        ) from None

    try:
        sqs_client = request.app.state.sqs_client
        queue_url = request.app.state.queue_url

        message_id = await run_in_threadpool(
            send_event,
            sqs_client,
            queue_url,
            payload,
            event_topic=payload.get(
                "event_type", "payment.event"
            ),
            webhook_id=payload.get("webhook_id"),
        )

    except (SQSConnectionError, BotoCoreError, ClientError) as error:
        logger.error(
            "Error de comunicacion con SQS",
            exc_info=True,
        )
        raise HTTPException(
            status_code=503,
            detail="Servicio de mensajeria no disponible.",
        ) from error

    return {
        "status": "accepted",
        "message_id": message_id,
    }
