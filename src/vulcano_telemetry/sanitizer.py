
"""
Proyecto: Vulcano Pay
Modulo: sanitizer.py
Responsabilidad: Integrante 1 - JUAN PABLO GAITA

Descripcion:
    Contiene funciones de validacion utilizadas por la interfaz
    de linea de comandos (CLI) de Vulcano Pay.

    Su objetivo es comprobar que los argumentos ingresados
    cumplan las reglas del proyecto antes de que sean utilizados
    por los modulos de SQS y Webhooks.

    Cuando un argumento es invalido, las funciones generan
    argparse.ArgumentTypeError para que argparse informe
    el problema al usuario.

Funciones:
    - parse_queue_name(): valida el nombre de una cola SQS.
    - parse_wait_time(): valida el tiempo de Long Polling.
    - parse_webhook_url(): valida la estructura de una URL.
    - parse_secret_key(): valida la longitud de la clave HMAC.
"""

# Biblioteca para procesar argumentos de la linea de comandos.
import argparse

# Biblioteca para trabajar con expresiones regulares.
import re

# Funcion para analizar los componentes de una URL.
from urllib.parse import urlparse


# ============================================================
# 1. VALIDACION DEL NOMBRE DE LA COLA SQS
# ============================================================

def parse_queue_name(value: str) -> str:
    """
    Valida el nombre de una cola SQS segun la convencion
    definida para Vulcano Pay.

    Parametros:
        value (str): Nombre de la cola recibido desde la CLI.

    Retorna:
        str: Nombre original de la cola, si es valido.

    Excepciones:
        argparse.ArgumentTypeError:
            Si el nombre no cumple el patron establecido.

    Formato permitido:
        vulcano-[a-z0-9-]+-queue

    Ejemplo valido:
        vulcano-events-queue

    Ejemplo invalido:
        payments-queue
    """

    # Define el patron obligatorio para los nombres de colas.
    # [a-z0-9-]+ permite letras minusculas, numeros y guiones.
    pattern = r"vulcano-[a-z0-9-]+-queue"

    # fullmatch verifica que toda la cadena cumpla el patron.
    # Si no coincide, se informa un error de argumento.
    if not re.fullmatch(pattern, value):
        raise argparse.ArgumentTypeError(
            f"Nombre de cola invalido: '{value}'. "
            "Debe cumplir el patron vulcano-[a-z0-9-]+-queue."
        )

    # Devuelve el nombre sin modificarlo.
    return value


# ============================================================
# 2. VALIDACION DEL TIEMPO DE LONG POLLING
# ============================================================

def parse_wait_time(value: str) -> int:
    """
    Valida el tiempo de espera utilizado por Amazon SQS
    durante una operacion de Long Polling.

    Parametros:
        value (str): Tiempo ingresado desde la CLI.

    Retorna:
        int: Tiempo validado, expresado en segundos.

    Excepciones:
        argparse.ArgumentTypeError:
            Si el valor no es un entero o esta fuera
            del rango permitido.

    Rango permitido:
        De 1 a 20 segundos, inclusive.

    Ejemplo valido:
        "15" -> 15

    Ejemplo invalido:
        "50" -> ArgumentTypeError
    """

    # Intenta convertir el argumento recibido a entero.
    try:
        wait_time = int(value)

    # Captura errores de conversion o de tipo.
    except (ValueError, TypeError) as error:
        raise argparse.ArgumentTypeError(
            "WaitTimeSeconds debe ser un numero entero."
        ) from error

    # Verifica que el tiempo se encuentre dentro del rango
    # establecido para el Long Polling de nuestra CLI.
    if not 1 <= wait_time <= 20:
        raise argparse.ArgumentTypeError(
            "WaitTimeSeconds debe estar entre 1 y 20 segundos."
        )

    # Devuelve el valor convertido a entero.
    return wait_time


# ============================================================
# 3. VALIDACION DE LA URL DEL WEBHOOK
# ============================================================

def parse_webhook_url(value: str) -> str:
    """
    Valida la estructura basica de la URL del receptor Webhook.

    Parametros:
        value (str): Direccion URL ingresada desde la CLI.

    Retorna:
        str: URL original, si cumple las validaciones.

    Excepciones:
        argparse.ArgumentTypeError:
            Si el esquema, host, puerto o formato es invalido.

    Condiciones:
        - Debe utilizar HTTP o HTTPS.
        - Debe contener un hostname.
        - No debe contener espacios en blanco.
        - Si especifica un puerto, debe ser valido.

    Ejemplo valido:
        http://localhost:8000/webhook

    Ejemplo invalido:
        ftp://localhost/webhook

    Nota:
        Esta funcion no comprueba la disponibilidad
        del servidor ni realiza solicitudes HTTP.
    """

    try:
        # Analiza la URL y separa sus componentes.
        parsed = urlparse(value)

        # Verifica que:
        # 1. El protocolo sea HTTP o HTTPS.
        # 2. Exista un hostname.
        # 3. No haya caracteres de espacio en blanco.
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or any(char.isspace() for char in value)
        ):
            raise ValueError("Esquema, host o formato invalido.")

        # Accede a la propiedad port para comprobar que,
        # si se especifico un puerto, su valor sea valido.
        # Python genera ValueError ante ciertos puertos invalidos.
        _ = parsed.port

    # Convierte los errores de URL en errores de argumento CLI.
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"URL de Webhook invalida: '{value}'. "
            "Debe ser una URL HTTP/HTTPS valida."
        ) from error

    # Devuelve la URL sin modificarla.
    return value


# ============================================================
# 4. VALIDACION DE LA CLAVE SECRETA HMAC
# ============================================================

def parse_secret_key(value: str) -> str:
    """
    Valida la longitud minima de la clave secreta HMAC.

    Parametros:
        value (str): Clave secreta recibida desde la CLI.

    Retorna:
        str: Clave original, si cumple la longitud minima.

    Excepciones:
        argparse.ArgumentTypeError:
            Si la clave contiene menos de 16 caracteres.

    Condicion:
        Longitud minima de 16 caracteres.

    Nota:
        Esta funcion no genera ni verifica firmas HMAC.
        La criptografia se implementa en webhook_crypto.py.

        La longitud minima es una regla del proyecto
        y no garantiza por si sola una clave segura.
    """

    # Comprueba la cantidad de caracteres de la clave.
    if len(value) < 16:
        raise argparse.ArgumentTypeError(
            "La clave secreta HMAC debe tener "
            "al menos 16 caracteres."
        )

    # Devuelve la clave original cuando es valida.
    return value
