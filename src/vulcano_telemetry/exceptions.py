"""
Proyecto Vulcano Pay
Módulo: exceptions.py
Responsabilidad: Integrante 1

Define las excepciones semánticas utilizadas
por el sistema de mensajería SQS y Webhooks.
"""


class VulcanoError(Exception):
    """Excepción base del sistema Vulcano Pay."""
    pass


# ==========================================
# EXCEPCIONES DEL DOMINIO SQS
# ==========================================

class SQSConnectionError(VulcanoError):
    """Error de conexión con AWS SQS o LocalStack."""
    pass


class QueueTimeoutError(VulcanoError):
    """Tiempo de espera de una operación SQS agotado."""
    pass


class CorruptedMessageError(VulcanoError):
    """Mensaje SQS con contenido inválido o corrupto."""
    pass


# ==========================================
# EXCEPCIONES DEL DOMINIO WEBHOOK
# ==========================================

class WebhookSignatureError(VulcanoError):
    """Firma criptográfica HMAC inválida o ausente."""
    pass


class WebhookTimestampError(VulcanoError):
    """Timestamp fuera de la ventana de tolerancia."""
    pass


class WebhookDeliveryError(VulcanoError):
    """Error al entregar un Webhook HTTP al destino."""
    pass
