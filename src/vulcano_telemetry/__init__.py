"""Vulcano Pay — motor de eventos asíncronos, webhooks HMAC y observabilidad JSON.

API pública del paquete. Sólo se re-exporta lo que no arrastra dependencias externas al
importar: el resto de los módulos (SQS, crypto, receiver, logging) se importan explícitamente
para no romper el arranque cuando boto3 o fastapi no están instalados.

Pendiente del rol 1 (ver contrato de interfaces): `QueueTimeoutError` se suma a este `__all__`
cuando el módulo `exceptions.py` lo defina.
"""

from .exceptions import (
    CorruptedMessageError,
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)

__all__ = [
    "CorruptedMessageError",
    "SQSConnectionError",
    "VulcanoError",
    "WebhookDeliveryError",
    "WebhookSignatureError",
    "WebhookTimestampError",
]

__version__ = "0.1.0"
