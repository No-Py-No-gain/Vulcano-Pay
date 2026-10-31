
"""
Proyecto Vulcano Pay
Modulo: webhook_crypto.py
Responsabilidad: Integrante 3

Generacion y verificacion de firmas HMAC-SHA256.
Proteccion contra ataques de replay y timing attacks.
"""

import hashlib
import hmac
import time

from .exceptions import (
    WebhookSignatureError,
    WebhookTimestampError,
)


def generate_signature(
    secret: str,
    timestamp: int,
    payload_bytes: bytes,
) -> str:
    """
    Genera una firma HMAC-SHA256 en hexadecimal.

    El contenido firmado es:
        timestamp + "." + payload_bytes
    """

    message = f"{timestamp}.".encode("utf-8") + payload_bytes

    return hmac.new(
        secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()


def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = 300,
) -> bool:
    """
    Verifica la firma HMAC y el timestamp del webhook.

    Cabecera esperada:
        X-Webhook-Signature: t=timestamp,v1=firma

    Lanza:
        WebhookSignatureError: firma ausente, malformada
            o incorrecta.
        WebhookTimestampError: timestamp fuera
            de la ventana permitida.
    """

    if not header_value:
        raise WebhookSignatureError(
            "Falta la cabecera X-Webhook-Signature."
        )

    try:
        parts = {}

        for item in header_value.split(","):
            key, value = item.strip().split("=", 1)

            if key in parts:
                raise ValueError("Campo duplicado.")

            parts[key] = value

        if set(parts) != {"t", "v1"}:
            raise ValueError("Campos de firma invalidos.")

        timestamp = int(parts["t"])
        received_signature = parts["v1"]

        if timestamp < 0 or not received_signature:
            raise ValueError("Valores de firma invalidos.")

    except (ValueError, TypeError, AttributeError) as error:
        raise WebhookSignatureError(
            "Cabecera X-Webhook-Signature malformada."
        ) from error

    current_time = time.time()

    if abs(current_time - timestamp) > tolerance_sec:
        raise WebhookTimestampError(
            "Timestamp fuera de la ventana permitida."
        )

    expected_signature = generate_signature(
        secret,
        timestamp,
        payload_bytes,
    )

    if not hmac.compare_digest(
        expected_signature,
        received_signature,
    ):
        raise WebhookSignatureError(
            "Firma HMAC-SHA256 invalida."
        )

    return True
