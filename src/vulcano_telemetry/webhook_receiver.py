
"""
Proyecto: Vulcano Pay
Modulo: webhook_receiver.py
Responsabilidad: Integrante 3

Descripcion:
    Implementa un servidor HTTP con FastAPI para recibir
    eventos Webhook provenientes de sistemas externos.

    Antes de aceptar un evento, el servidor comprueba
    su firma criptografica HMAC-SHA256 y la validez
    temporal del mensaje.

    Una vez validado, convierte el contenido a JSON
    y lo publica en una cola AWS SQS emulada por LocalStack.

Integracion con otros modulos:
    - webhook_crypto.py: verifica firmas HMAC.
    - core_sqs.py: establece la conexion y publica eventos.
    - exceptions.py: proporciona excepciones personalizadas.

Funciones principales:
    - lifespan(): administra el ciclo de vida del servidor.
    - receive_webhook(): recibe y procesa solicitudes HTTP.

Endpoint:
    POST /webhook

Respuesta exitosa:
    HTTP 202 Accepted

    {
        "status": "accepted",
        "message_id": "identificador-sqs"
    }
"""

# Permite interpretar el contenido JSON recibido.
import json

# Permite registrar eventos, advertencias y errores.
import logging

# Permite acceder a variables de entorno.
import os

# Facilita la administracion del ciclo de vida
# de recursos utilizados por FastAPI.
from contextlib import asynccontextmanager
# Excepciones de Boto3 utilizadas para controlar errores de SQS.
from botocore.exceptions import BotoCoreError, ClientError

# Componentes utilizados para crear la API HTTP.
from fastapi import FastAPI, HTTPException, Request, status

# Ejecuta funciones sincronicas en un grupo de hilos
# para evitar bloquear el ciclo de eventos asincrono.
from starlette.concurrency import run_in_threadpool

# Funciones del Integrante 2 para trabajar con SQS.
from .core_sqs import get_sqs_client, send_event

# Excepciones personalizadas del Integrante 1.
from .exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)

# Funcion del Integrante 3 para verificar HMAC-SHA256.
from .webhook_crypto import verify_signature


# Logger compartido con el motor de eventos Vulcano.
logger = logging.getLogger("vulcano_engine")
# ============================================================
# 1. INICIALIZACION Y CIERRE DEL SERVIDOR
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Administra los recursos de SQS durante el ciclo de vida
    de la aplicacion FastAPI.

    Parametros:
        app (FastAPI):
            Instancia de la aplicacion HTTP.

    Funcionamiento:
        1. Crea un cliente de SQS.
        2. Obtiene el nombre de la cola desde el entorno.
        3. Consulta a SQS la URL de esa cola.
        4. Guarda el cliente y la URL en app.state.
        5. Mantiene disponibles los recursos mientras
           la aplicacion esta funcionando.
        6. Cierra el cliente al finalizar.

    Variables de entorno:
        LOCALSTACK_ENDPOINT:
            Direccion del servicio SQS.
            Valor predeterminado: http://localhost:4566

        VULCANO_QUEUE_NAME:
            Nombre de la cola de eventos.
            Valor predeterminado: vulcano-events-queue

    Nota:
        La funcion es asincrona, pero el cliente Boto3
        realiza operaciones sincronicas.

        Por eso se utiliza run_in_threadpool() para
        consultar la URL de la cola sin bloquear
        directamente el ciclo de eventos de FastAPI.
    """

    # Crea el cliente SQS mediante la funcion desarrollada
    # por el Integrante 2.
    #
    # Si no existe LOCALSTACK_ENDPOINT, utiliza
    # la direccion predeterminada de LocalStack.
    sqs_client = get_sqs_client(
        os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
    )

    try:
        # Obtiene el nombre de la cola desde una variable
        # de entorno o utiliza el nombre predeterminado.
        queue_name = os.getenv(
            "VULCANO_QUEUE_NAME",
            "vulcano-events-queue",
        )


        # Consulta la URL real de la cola SQS.
        # Se utiliza un threadpool porque Boto3 es sincronico.
        try:
            queue_response = await run_in_threadpool(
                sqs_client.get_queue_url,
                QueueName=queue_name,
            )

        except (ClientError, BotoCoreError) as error:
            # Convierte los errores de Boto3 en una excepcion
            # personalizada que puede interpretar la CLI.
            sqs_error = SQSConnectionError(
                f"No se pudo resolver la cola '{queue_name}' en SQS."
            )

            # Agrega informacion para facilitar el diagnostico.
            sqs_error.add_note("Operacion: get_queue_url")
            sqs_error.add_note(f"QueueName: {queue_name}")

            # Conserva el error original como causa.
            raise sqs_error from error


        # Guarda los recursos en el estado compartido
        # de FastAPI para que los endpoints puedan usarlos.
        app.state.sqs_client = sqs_client
        app.state.queue_url = queue_response["QueueUrl"]

        # Registra que la inicializacion fue correcta.
        logger.info("Conexion SQS inicializada")

        # Entrega el control a FastAPI.
        #
        # Todo lo anterior a yield se ejecuta al iniciar.
        # El bloque finally se ejecuta al finalizar.
        yield

    finally:
        # Libera los recursos del cliente Boto3 incluso
        # cuando se produce un error durante el ciclo
        # de vida de la aplicacion.
        sqs_client.close()


# ============================================================
# 2. CREACION DE LA APLICACION FASTAPI
# ============================================================

# Crea la aplicacion HTTP y registra lifespan
# como administrador de su ciclo de vida.
app = FastAPI(
    title="Vulcano Pay - Webhook Receiver",
    lifespan=lifespan,
)


# ============================================================
# 3. ENDPOINT RECEPTOR DE WEBHOOKS
# ============================================================

# Registra la ruta POST /webhook.
#
# Si el procesamiento finaliza correctamente, FastAPI
# devuelve HTTP 202 Accepted.
@app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def receive_webhook(request: Request):
    """
    Recibe, autentica y publica un evento Webhook en SQS.

    Parametros:
        request (Request):
            Solicitud HTTP recibida por FastAPI.

    Procedimiento:
        1. Obtiene la clave secreta desde el entorno.
        2. Comprueba que tenga al menos 16 caracteres.
        3. Lee el cuerpo original de la solicitud.
        4. Extrae la cabecera X-Webhook-Signature.
        5. Verifica la firma HMAC y el timestamp.
        6. Interpreta el cuerpo como JSON.
        7. Comprueba que el JSON sea un objeto.
        8. Obtiene el cliente y la URL de SQS.
        9. Publica el evento en la cola.
       10. Devuelve una respuesta HTTP 202.

    Respuestas HTTP:
        202 Accepted:
            El evento fue aceptado y publicado en SQS.

        400 Bad Request:
            El JSON es invalido o no es un objeto.

        401 Unauthorized:
            La firma HMAC o el timestamp son invalidos.

        503 Service Unavailable:
            La clave secreta no esta configurada
            correctamente o falla la comunicacion con SQS.

    Retorna:
        dict:
            Diccionario con el estado de aceptacion
            y el identificador asignado por SQS.

    Importante:
        HTTP 202 confirma la publicacion del mensaje
        en SQS, no que otro componente ya lo haya
        procesado definitivamente.
    """

    # --------------------------------------------------------
    # PASO 1: OBTENER Y VALIDAR LA CLAVE SECRETA
    # --------------------------------------------------------

    # La clave se obtiene de una variable de entorno
    # para no incluirla directamente en el codigo fuente.
    secret = os.getenv("VULCANO_WEBHOOK_SECRET")

    # Rechaza configuraciones sin clave o con una clave
    # de menos de 16 caracteres.
    if not secret or len(secret) < 16:
        logger.error("Clave HMAC no configurada correctamente")

        # HTTP 503 indica que el servicio no esta
        # preparado para procesar solicitudes.
        raise HTTPException(
            status_code=503,
            detail="Servicio no configurado.",
        )

    # --------------------------------------------------------
    # PASO 2: LEER EL CONTENIDO ORIGINAL DEL WEBHOOK
    # --------------------------------------------------------

    # Lee el cuerpo como bytes, sin modificar su formato.
    #
    # Esto es importante porque la firma HMAC se calcula
    # sobre los bytes originales del mensaje.
    raw_body = await request.body()

    # Extrae la cabecera con el timestamp y la firma.
    #
    # Si no existe, utiliza una cadena vacia que sera
    # rechazada por verify_signature().
    signature_header = request.headers.get(
        "X-Webhook-Signature", ""
    )

    # --------------------------------------------------------
    # PASO 3: VERIFICAR FIRMA HMAC-SHA256 Y TIMESTAMP
    # --------------------------------------------------------

    try:
        # Llama a la funcion del modulo webhook_crypto.py.
        #
        # Comprueba:
        # - Formato de la cabecera.
        # - Validez temporal del mensaje.
        # - Coincidencia de la firma criptografica.
        verify_signature(
            secret=secret,
            header_value=signature_header,
            payload_bytes=raw_body,
        )


    except WebhookSignatureError as error:
        # Una firma incorrecta o ausente impide autenticar
        # el origen y la integridad del Webhook.
        logger.warning(
            "Webhook rechazado por firma: %s",
            type(error).__name__,
        )

        # HTTP 401: error de autenticacion.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Firma HMAC invalida o ausente.",
        ) from error

    except WebhookTimestampError as error:
        # Rechaza mensajes que estan fuera de la ventana
        # temporal permitida, evitando ataques de repeticion.
        logger.warning(
            "Webhook rechazado por timestamp: %s",
            type(error).__name__,
        )

        # HTTP 403: mensaje fuera de la ventana temporal.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Timestamp fuera de la ventana permitida.",
        ) from error

    # --------------------------------------------------------
    # PASO 4: INTERPRETAR Y VALIDAR EL JSON
    # --------------------------------------------------------

    try:
        # Convierte los bytes recibidos en una estructura
        # de datos Python.
        #
        # Esta operacion se realiza despues de verificar
        # la firma para autenticar primero el contenido.
        payload = json.loads(raw_body)

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        # Rechaza cuerpos que no puedan interpretarse
        # como documentos JSON validos.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="JSON invalido.",
        ) from error

    # Exige que el JSON principal sea un objeto,
    # representado mediante un diccionario Python.
    #
    # No se aceptan listas, cadenas o numeros como
    # estructura principal del evento.
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="El evento debe ser un objeto JSON.",
        ) from None

    # --------------------------------------------------------
    # PASO 5: PUBLICAR EL EVENTO EN SQS
    # --------------------------------------------------------

    try:
        # Recupera los recursos creados al iniciar
        # la aplicacion mediante lifespan().
        sqs_client = request.app.state.sqs_client
        queue_url = request.app.state.queue_url

        # Publica el evento mediante send_event(),
        # funcion desarrollada por el Integrante 2.
        #
        # run_in_threadpool() evita ejecutar directamente
        # la llamada sincronica de Boto3 en el ciclo
        # de eventos asincrono.
        #
        # El atributo EventTopic toma el valor de
        # event_type si existe; de lo contrario,
        # utiliza "payment.event".
        message_id = await run_in_threadpool(
            send_event,
            sqs_client,
            queue_url,
            payload,
            # Misma fuente que `produce_event`: el topic viaja como `event_topic`; `event_type`
            # queda como alias para eventos de pasarelas que usan ese nombre.
            event_topic=payload.get(
                "event_topic", payload.get("event_type", "payment.event")
            ),
            webhook_id=payload.get("webhook_id"),
        )

    except (SQSConnectionError, BotoCoreError, ClientError) as error:
        # Registra el fallo de comunicacion con SQS.
        #
        # exc_info=True incluye informacion de la
        # excepcion en el sistema de logging.
        logger.error(
            "Error de comunicacion con SQS",
            exc_info=True,
        )

        # Devuelve HTTP 503 porque el sistema de
        # mensajeria no pudo aceptar el evento.
        raise HTTPException(
            status_code=503,
            detail="Servicio de mensajeria no disponible.",
        ) from error

    # --------------------------------------------------------
    # PASO 6: RESPONDER AL EMISOR DEL WEBHOOK
    # --------------------------------------------------------

    # Devuelve una confirmacion con el identificador
    # del mensaje generado por SQS.
    #
    # El decorador del endpoint establece HTTP 202.
    return {
        "status": "accepted",
        "message_id": message_id,
    }
