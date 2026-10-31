
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
        os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    )

    try:
        queue_name = os.getenv(
            "VULCANO_QUEUE_NAME",
            "vulcano-events-queue",
        )

        queue_response = await run_in_threadpool(
            sqs_client.get_queue_url,
            QueueName=queue_name,
        )

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

    except (WebhookSignatureError, WebhookTimestampError) as error:
        logger.warning(
            "Webhook rechazado: %s",
            type(error).__name__,
        )
        raise HTTPException(
            status_code=401,
            detail="Firma o timestamp invalido.",
        ) from error

    try:
        payload = json.loads(raw_body)

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise HTTPException(
            status_code=400,
            detail="JSON invalido.",
        ) from error

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail="El evento debe ser un objeto JSON.",
        )

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
