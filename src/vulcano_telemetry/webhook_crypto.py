
"""
Proyecto: Vulcano Pay
Modulo: webhook_crypto.py
Responsabilidad: Integrante 3 JUAN PABLO GAITA

Descripcion:
    Implementa la generacion y verificacion de firmas
    criptograficas HMAC-SHA256 para los Webhooks.

    Su objetivo es comprobar la autenticidad e integridad
    de los mensajes recibidos, utilizando una clave secreta
    compartida entre el emisor y el receptor.

    Tambien verifica la antiguedad del timestamp para
    limitar la posibilidad de ataques de repeticion (replay).

    La comparacion de firmas utiliza hmac.compare_digest()
    para reducir riesgos asociados a ataques de tiempo
    (timing attacks).

Funciones:
    - generate_signature(): genera una firma HMAC-SHA256.
    - verify_signature(): valida la firma y el timestamp.
"""

# Biblioteca de funciones hash criptograficas.
import hashlib

# Biblioteca para calcular y verificar firmas HMAC.
import hmac

# Biblioteca para obtener la hora actual del sistema.
import time

# Excepciones personalizadas definidas por el Integrante 1.
from .exceptions import (
    WebhookSignatureError,
    WebhookTimestampError,
)


# ============================================================
# 1. GENERACION DE LA FIRMA CRIPTOGRAFICA
# ============================================================

def generate_signature(
    secret: str,
    timestamp: int,
    payload_bytes: bytes,
) -> str:
    """
    Genera una firma HMAC-SHA256 para un mensaje Webhook.

    Parametros:
        secret (str):
            Clave secreta compartida entre emisor y receptor.

        timestamp (int):
            Marca temporal del mensaje, expresada como
            segundos desde la epoca Unix.

        payload_bytes (bytes):
            Contenido original del Webhook en bytes.

    Retorna:
        str:
            Firma HMAC-SHA256 representada como una cadena
            hexadecimal.

    Funcionamiento:
        1. Une el timestamp y el contenido del mensaje.
        2. Codifica la clave secreta en UTF-8.
        3. Calcula HMAC utilizando SHA-256.
        4. Devuelve el resultado hexadecimal.

    Formato del mensaje firmado:
        timestamp + "." + payload_bytes

    Importante:
        El emisor y el receptor deben utilizar exactamente
        los mismos bytes, timestamp y clave secreta.
    """

    # Construye el mensaje que sera firmado.
    # Se agrega un punto como separador entre el timestamp
    # y el contenido original del Webhook.
    #
    # Ejemplo conceptual:
    # b'1730000000.{"status":"COMPLETED"}'
    message = f"{timestamp}.".encode("utf-8") + payload_bytes

    # hmac.new() calcula el codigo de autenticacion
    # utilizando la clave secreta y el algoritmo SHA-256.
    #
    # hexdigest() convierte el resultado binario
    # en una representacion hexadecimal.
    return hmac.new(
        secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()


# ============================================================
# 2. VERIFICACION DE FIRMA Y TIMESTAMP
# ============================================================

def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = 300,
) -> bool:
    """
    Verifica la autenticidad e integridad de un Webhook.

    Parametros:
        secret (str):
            Clave secreta compartida con el emisor.

        header_value (str):
            Valor recibido en X-Webhook-Signature.

        payload_bytes (bytes):
            Contenido original del Webhook en bytes.

        tolerance_sec (int):
            Diferencia maxima permitida entre el timestamp
            recibido y la hora actual. Por defecto, 300 s.

    Retorna:
        bool:
            True cuando todas las verificaciones se cumplen.

    Excepciones:
        WebhookSignatureError:
            Si falta la cabecera, tiene un formato invalido
            o la firma HMAC no coincide.

        WebhookTimestampError:
            Si el timestamp se encuentra fuera de la
            ventana de tolerancia permitida.

    Formato esperado de la cabecera:
        X-Webhook-Signature: t=timestamp,v1=firma

    Procedimiento:
        1. Comprueba que exista la cabecera.
        2. Extrae los campos t y v1.
        3. Rechaza campos duplicados o inesperados.
        4. Convierte el timestamp a entero.
        5. Comprueba la ventana temporal.
        6. Calcula la firma esperada.
        7. Compara ambas firmas de forma segura.

    Nota:
        La ventana temporal limita la antiguedad de los
        mensajes, pero no impide por completo que un mismo
        mensaje valido se reenvie dentro de esa ventana.
    """

    # Comprueba que se haya recibido la cabecera.
    if not header_value:
        raise WebhookSignatureError(
            "Falta la cabecera X-Webhook-Signature."
        )

    try:
        # Diccionario que almacenara los campos
        # extraidos de la cabecera.
        parts = {}

        # Separa los campos utilizando la coma.
        # Ejemplo: t=1730000000,v1=abcdef...
        for item in header_value.split(","):

            # Separa el nombre y el valor de cada campo.
            # maxsplit=1 evita dividir mas de una vez.
            key, value = item.strip().split("=", 1)

            # Rechaza campos repetidos para evitar
            # interpretaciones ambiguas de la cabecera.
            if key in parts:
                raise ValueError("Campo duplicado.")

            parts[key] = value

        # Exige exactamente dos campos: t y v1.
        if set(parts) != {"t", "v1"}:
            raise ValueError("Campos de firma invalidos.")

        # Convierte el timestamp recibido a entero.
        timestamp = int(parts["t"])

        # Obtiene la firma enviada por el emisor.
        received_signature = parts["v1"]

        # Rechaza timestamps negativos o firmas vacias.
        if timestamp < 0 or not received_signature:
            raise ValueError("Valores de firma invalidos.")

    # Convierte errores de formato en una excepcion
    # personalizada del sistema Vulcano Pay.
    except (ValueError, TypeError, AttributeError) as error:
        raise WebhookSignatureError(
            "Cabecera X-Webhook-Signature malformada."
        ) from error

    # Obtiene la hora actual en segundos Unix.
    current_time = time.time()

    # Calcula la diferencia absoluta entre la hora actual
    # y el timestamp recibido.
    #
    # abs() permite detectar mensajes demasiado antiguos
    # y tambien mensajes con fechas demasiado futuras.
    if abs(current_time - timestamp) > tolerance_sec:
        raise WebhookTimestampError(
            "Timestamp fuera de la ventana permitida."
        )

    # Genera nuevamente la firma usando el contenido
    # original, el timestamp y la clave compartida.
    expected_signature = generate_signature(
        secret,
        timestamp,
        payload_bytes,
    )

    # Compara la firma esperada con la recibida.
    #
    # compare_digest() esta disenada para reducir
    # filtraciones de informacion por diferencias
    # en los tiempos de comparacion.
    if not hmac.compare_digest(
        expected_signature,
        received_signature,
    ):
        raise WebhookSignatureError(
            "Firma HMAC-SHA256 invalida."
        )

    # Si todas las verificaciones fueron correctas,
    # el Webhook se considera valido.
    return True


def build_signature_header(
    secret: str,
    payload_bytes: bytes,
    timestamp: int | None = None,
) -> tuple[int, str]:
    """Devuelve (timestamp, 't=<timestamp>,v1=<hmac>') usando generate_signature.

    Único dueño del formato de la cabecera: el emisor compone con esta función y el receptor
    interpreta con `verify_signature`, así el round-trip queda garantizado por construcción y no
    por convención.
    """

    if timestamp is None:
        timestamp = int(time.time())

    header = f"t={timestamp},v1={generate_signature(secret, timestamp, payload_bytes)}"

    return timestamp, header
