
"""
Proyecto: Vulcano Pay
Modulo: exceptions.py
Responsabilidad: Integrante 1 - JUAN PABLO GAITA

Descripcion:
    Define las excepciones personalizadas utilizadas
    por los diferentes componentes de Vulcano Pay.

    Las excepciones permiten identificar errores
    especificos relacionados con Amazon SQS,
    LocalStack y los Webhooks.

    Todas las excepciones del proyecto heredan de
    VulcanoError, que a su vez hereda de Exception.

    Esta jerarquia permite capturar errores
    individuales o agruparlos mediante una
    excepcion base comun.

Importante:
    Este modulo solamente define los tipos de error.
    Las excepciones son lanzadas y gestionadas
    desde los modulos que realizan las operaciones.
"""


# ============================================================
# 1. EXCEPCION BASE DEL PROYECTO
# ============================================================

class VulcanoError(Exception):
    """
    Clase base de las excepciones de Vulcano Pay.

    Hereda de:
        Exception: Clase base de las excepciones
        convencionales de Python.

    Objetivo:
        Proporcionar un tipo comun para identificar
        los errores personalizados del proyecto.

    Ejemplo:
        except VulcanoError as error:
            print(error)
    """

    # No necesita comportamiento adicional.
    # Su utilidad es establecer una jerarquia comun.
    pass


# ============================================================
# 2. EXCEPCIONES RELACIONADAS CON AMAZON SQS
# ============================================================

class SQSConnectionError(VulcanoError):
    """
    Representa un problema de conexion con SQS.

    Puede utilizarse cuando:
        - LocalStack no esta disponible.
        - El endpoint SQS no responde.
        - Se produce un error de comunicacion.

    Hereda de:
        VulcanoError.

    Nota:
        Esta clase no realiza conexiones.
        Solamente representa el tipo de error.
    """

    pass


class QueueTimeoutError(VulcanoError):
    """
    Representa un tiempo de espera agotado
    durante una operacion relacionada con SQS.

    Puede utilizarse cuando:
        - Una operacion supera un limite de tiempo.
        - El sistema detecta un timeout.

    Hereda de:
        VulcanoError.

    Nota:
        Esta excepcion no implementa por si misma
        ningun mecanismo de control de tiempo.
    """

    pass


class CorruptedMessageError(VulcanoError):
    """
    Representa un mensaje SQS invalido o corrupto.

    Puede utilizarse cuando:
        - El contenido no es un JSON valido.
        - El mensaje no puede interpretarse
          correctamente.

    Hereda de:
        VulcanoError.

    Ejemplo conceptual:
        Un mensaje contiene "{json_invalido"
        y no puede convertirse a un objeto JSON.
    """

    pass


# ============================================================
# 3. EXCEPCIONES RELACIONADAS CON WEBHOOKS
# ============================================================

class WebhookSignatureError(VulcanoError):
    """
    Representa un problema con la firma HMAC
    utilizada para autenticar un Webhook.

    Puede utilizarse cuando:
        - La firma esta ausente.
        - La firma tiene un formato incorrecto.
        - La firma recibida no coincide
          con la firma calculada.

    Hereda de:
        VulcanoError.
    """

    pass


class WebhookTimestampError(VulcanoError):
    """
    Representa un timestamp invalido o fuera
    de la ventana temporal permitida.

    Objetivo:
        Identificar mensajes cuya marca temporal
        no cumple las reglas de verificacion.

    Relacion:
        Se utiliza durante la validacion
        de seguridad de los Webhooks.

    Hereda de:
        VulcanoError.
    """

    pass


class WebhookDeliveryError(VulcanoError):
    """
    Representa un error durante la entrega
    de un Webhook HTTP a su destino.

    Puede utilizarse cuando:
        - El servidor de destino no responde.
        - Se produce un error de comunicacion HTTP.
        - La entrega del Webhook falla.

    Hereda de:
        VulcanoError.

    Nota:
        La clase define el tipo de error,
        pero no implementa el envio HTTP.
    """

    pass
