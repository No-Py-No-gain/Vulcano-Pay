"""Vulcano Pay — motor de eventos asíncronos, webhooks HMAC y observabilidad JSON.

API pública del paquete. Sólo se re-exporta lo que no arrastra dependencias externas al
importar: el resto de los módulos (SQS, crypto, receiver, logging) se importan explícitamente
para no romper el arranque cuando boto3 o fastapi no están instalados.

`QueueTimeoutError` (rol 1, pedido C1 del contrato) ya existe y se re-exporta: es la única forma
semántica de distinguir "la cola está vacía" de "SQS falló".
"""

from .exceptions import (
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)

__all__ = [
    "CorruptedMessageError",
    "QueueTimeoutError",
    "SQSConnectionError",
    "VulcanoError",
    "WebhookDeliveryError",
    "WebhookSignatureError",
    "WebhookTimestampError",
]

__version__ = "0.1.0"
