
"""
Proyecto Vulcano Pay
Modulo: sanitizer.py
Responsabilidad: Integrante 1

Validadores personalizados para los argumentos
de la interfaz de linea de comandos (CLI).
"""

import argparse
import re
from urllib.parse import urlparse


def parse_queue_name(value: str) -> str:
    """
    Valida que el nombre de la cola SQS cumpla
    con el patron vulcano-[a-z0-9-]+-queue.
    """
    pattern = r"vulcano-[a-z0-9-]+-queue"

    if not re.fullmatch(pattern, value):
        raise argparse.ArgumentTypeError(
            f"Nombre de cola invalido: '{value}'. "
            "Debe cumplir el patron vulcano-[a-z0-9-]+-queue."
        )

    return value


def parse_wait_time(value: str) -> int:
    """
    Valida el tiempo de Long Polling de SQS.
    Debe ser un numero entero entre 1 y 20 segundos.
    """
    try:
        wait_time = int(value)
    except (ValueError, TypeError) as error:
        raise argparse.ArgumentTypeError(
            "WaitTimeSeconds debe ser un numero entero."
        ) from error

    if not 1 <= wait_time <= 20:
        raise argparse.ArgumentTypeError(
            "WaitTimeSeconds debe estar entre 1 y 20 segundos."
        )

    return wait_time


def parse_webhook_url(value: str) -> str:
    """
    Valida que la URL del Webhook utilice HTTP o HTTPS
    y tenga un host valido.
    """
    try:
        parsed = urlparse(value)

        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or any(char.isspace() for char in value)
        ):
            raise ValueError("Esquema, host o formato invalido.")

        # Comprueba que el puerto, si existe, sea valido.
        _ = parsed.port

    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"URL de Webhook invalida: '{value}'. "
            "Debe ser una URL HTTP/HTTPS valida."
        ) from error

    return value


def parse_secret_key(value: str) -> str:
    """
    Valida que la clave secreta HMAC tenga
    al menos 16 caracteres.
    """
    if len(value) < 16:
        raise argparse.ArgumentTypeError(
            "La clave secreta HMAC debe tener "
            "al menos 16 caracteres."
        )

    return value
