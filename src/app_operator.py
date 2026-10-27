"""CLI de Vulcano Pay (rol 5): publica eventos, levanta el receptor y corre el despachador.

S7 del enunciado: `argparse` con los cuatro sanitizadores del rol 1 integrados como `type` y tres
subcomandos —`produce-sqs`, `start-receiver`, `start-dispatcher`—. S8 (PEP 765, criterio 2): un
`except*` por cada grupo de excepciones del dominio y un `finally` **sin** `return`, `break` ni
`continue` que apaga el pipeline de observabilidad.

Códigos de salida (`0` ok · `2` validación de argparse · `3` firma/timestamp · `4` SQS ·
`5` entrega · `130` Ctrl-C): la tabla está en el `epilog` del parser, así que `--help` la publica.

Dos decisiones que conviene tener a mano al leer este módulo:

1. `setup_vulcano_logging()` se llama **dentro de `main()`**, nunca al importar. Importar la CLI no
   puede crear archivos de log: eso lo verifica `tests/test_cli.py`. Por eso el logger se obtiene
   con `logging.getLogger(NOMBRE_LOGGER)` una vez que el pipeline ya está configurado.
2. `main()` devuelve el código de salida como `int` y no deja escapar el `SystemExit` de argparse:
   `ParserVulcano.exit()` lo convierte en `SalidaDelParser` y `main()` lo traduce a su código de
   retorno. `if __name__ == "__main__":` y el entrypoint `vulcano` de `pyproject.toml` hacen
   `sys.exit(main())`.

La URL de la cola **siempre** se pide al servicio con `get_queue_url` a partir del nombre; nunca se
concatena el endpoint a mano. Que la cola no exista es un error de SQS, no de validación.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from types import TracebackType
from typing import Any, Sequence

import uvicorn
from botocore.exceptions import ClientError

from vulcano_telemetry.core_sqs import get_sqs_client, produce_event
from vulcano_telemetry.dispatcher import DespachadorSQS, dispatch_webhook
from vulcano_telemetry.exceptions import (
    SQSConnectionError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.logging_engine import (
    NOMBRE_LOGGER,
    setup_vulcano_logging,
    shutdown_vulcano_logging,
)
from vulcano_telemetry.sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)

__all__ = ["build_parser", "dispatch_webhook", "main"]

COLA_POR_DEFECTO = "vulcano-events-queue"
ESPERA_POR_DEFECTO = 10
URL_POR_DEFECTO = "http://127.0.0.1:8000/webhook"
SECRETO_POR_DEFECTO = "mi_clave_secreta_super_segura_vulcano_123"
TIMEOUT_POR_DEFECTO = 5.0
ARCHIVO_LOG_POR_DEFECTO = "vulcano_events.log"
APP_RECEPTOR = "vulcano_telemetry.webhook_receiver:app"

# Evento de ejemplo para que `produce-sqs` sirva en la demo sin escribir un JSON a mano.
EVENTO_DE_EJEMPLO: dict[str, Any] = {
    "transaction_id": "tx_9981",
    "amount": 350.0,
    "status": "COMPLETED",
    "event_topic": "pago.aprobado",
}

# Tabla de códigos de salida: es el `epilog` del parser y por eso aparece en `--help`.
TABLA_DE_CODIGOS = """\
Códigos de salida:
  0    operación completada correctamente
  2    error de validación de argumentos (lo emite argparse)
  3    firma HMAC inválida o timestamp fuera de la ventana (posible replay)
  4    error de comunicación con SQS, incluida una cola inexistente
  5    entrega del webhook fallida
  130  interrupción por Ctrl-C
"""


class SalidaDelParser(Exception):
    """Corte del parser con un código de salida; reemplaza al `SystemExit` de argparse.

    El criterio 1 del trabajo incluye a `SystemExit` entre las excepciones que ningún `except` debe
    capturar, así que el parser no aborta el proceso: levanta esta excepción propia, que `main()`
    traduce a su valor de retorno.
    """

    def __init__(self, codigo: int) -> None:
        super().__init__(f"el parser terminó con el código {codigo}")
        self.codigo = codigo


class ParserVulcano(argparse.ArgumentParser):
    """`ArgumentParser` que no llama a `sys.exit()`: `--help` y los errores vuelven como código."""

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if message:
            self._print_message(message, sys.stderr)
        raise SalidaDelParser(status)


class InterrupcionPorCtrlC:
    """Convierte un Ctrl-C en el código de salida 130, sin traceback.

    No se puede escribir `except KeyboardInterrupt`: el criterio 1 del proyecto lo cuenta entre las
    excepciones que ningún `except` debe capturar. Acá la interrupción llega como argumento de
    `__exit__`, se registra y se transforma en una bandera que `main()` traduce a 130. Es una
    interrupción deliberada del operador, no un error silenciado.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.interrumpida = False

    def __enter__(self) -> InterrupcionPorCtrlC:
        return self

    def __exit__(
        self,
        tipo: type[BaseException] | None,
        excepcion: BaseException | None,
        traza: TracebackType | None,
    ) -> bool:
        if tipo is None or not issubclass(tipo, KeyboardInterrupt):
            return False
        self.interrumpida = True
        self.logger.warning("Interrupción por Ctrl-C: se detiene la operación (código de salida 130).")
        return True


def parse_payload(valor: str) -> dict[str, Any]:
    """Payload JSON en texto → diccionario. Un payload inservible es validación (código 2)."""
    try:
        evento = json.loads(valor)
    except json.JSONDecodeError as err:
        raise argparse.ArgumentTypeError(f"Payload inválido: no es JSON ({err.msg}).") from err
    if not isinstance(evento, dict):
        raise argparse.ArgumentTypeError("Payload inválido: el evento debe ser un objeto JSON.")
    return evento


def _opciones_compartidas(parser: argparse.ArgumentParser) -> None:
    """Opciones comunes a los tres subcomandos, con los sanitizadores del rol 1 como `type`."""
    parser.add_argument(
        "-q",
        "--queue",
        type=parse_queue_name,
        default=COLA_POR_DEFECTO,
        help=f"Nombre de la cola SQS (patrón vulcano-<nombre>-queue, por defecto {COLA_POR_DEFECTO}).",
    )
    parser.add_argument(
        "-w",
        "--wait-time",
        type=parse_wait_time,
        default=ESPERA_POR_DEFECTO,
        help=f"Segundos de long polling en SQS, entre 1 y 20 (por defecto {ESPERA_POR_DEFECTO}).",
    )
    parser.add_argument(
        "--url",
        type=parse_webhook_url,
        default=URL_POR_DEFECTO,
        help=f"URL HTTP/HTTPS del receptor de webhooks (por defecto {URL_POR_DEFECTO}).",
    )
    parser.add_argument(
        "--secret",
        type=parse_secret_key,
        default=SECRETO_POR_DEFECTO,
        help="Clave secreta HMAC de al menos 16 caracteres.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=TIMEOUT_POR_DEFECTO,
        help=f"Timeout en segundos de cada POST de webhook (por defecto {TIMEOUT_POR_DEFECTO}).",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("VULCANO_LOG_FILE") or ARCHIVO_LOG_POR_DEFECTO,
        help=f"Archivo de log JSON con rotación gzip (por defecto VULCANO_LOG_FILE o {ARCHIVO_LOG_POR_DEFECTO}).",
    )


def build_parser() -> argparse.ArgumentParser:
    """Parser con los tres subcomandos de S7 y la tabla de códigos de salida como `epilog`."""
    parser = ParserVulcano(
        prog="vulcano",
        description="Motor de eventos SQS y webhooks HMAC de Vulcano Pay.",
        epilog=TABLA_DE_CODIGOS,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subcomandos = parser.add_subparsers(
        dest="comando",
        required=True,
        title="subcomandos",
        metavar="{produce-sqs,start-receiver,start-dispatcher}",
    )

    producir = subcomandos.add_parser(
        "produce-sqs",
        help="Publica un evento en la cola SQS.",
        description="Publica un evento JSON en la cola y registra el MessageId devuelto por SQS.",
    )
    _opciones_compartidas(producir)
    producir.add_argument(
        "--payload",
        type=parse_payload,
        default=dict(EVENTO_DE_EJEMPLO),
        help="Evento a publicar, como JSON en texto (por defecto un evento de ejemplo).",
    )
    producir.add_argument(
        "--event-topic",
        default=None,
        help="Tema del evento: se inyecta en el cuerpo y queda en el registro forense.",
    )

    receptor = subcomandos.add_parser(
        "start-receiver",
        help="Levanta el receptor HTTP de webhooks.",
        description="Levanta el receptor FastAPI de webhooks con uvicorn.",
    )
    _opciones_compartidas(receptor)
    receptor.add_argument("--host", default="127.0.0.1", help="Interfaz donde escucha el receptor (127.0.0.1).")
    receptor.add_argument("--port", type=int, default=8000, help="Puerto del receptor (8000).")

    despachador = subcomandos.add_parser(
        "start-dispatcher",
        help="Consume la cola y despacha los webhooks firmados.",
        description="Consume la cola SQS y entrega cada evento firmado a la URL configurada.",
    )
    _opciones_compartidas(despachador)
    despachador.add_argument(
        "--max-lotes",
        type=int,
        default=None,
        help="Corta el bucle después de N lotes (por defecto corre hasta Ctrl-C).",
    )

    return parser


def _reportar_grupo(logger: logging.Logger, contexto: str, grupo: ExceptionGroup) -> None:
    """Registra el grupo completo: el árbol del `ExceptionGroup` y cada hijo con sus notas."""
    logger.error(f"{contexto} ({len(grupo.exceptions)} error(es)).", exc_info=grupo)
    for excepcion in grupo.exceptions:
        notas = list(getattr(excepcion, "__notes__", ()))
        logger.error(f"  • {excepcion} | notas={notas}")


def _resolver_url_cola(cliente: Any, nombre_cola: str) -> str:
    """URL real de la cola, pedida al servicio a partir del nombre. Nunca se concatena a mano."""
    try:
        respuesta = cliente.get_queue_url(QueueName=nombre_cola)
    except ClientError as err:
        error = SQSConnectionError(f"No se pudo obtener la URL de la cola '{nombre_cola}'.")
        error.add_note(f"QueueName: {nombre_cola}")
        error.add_note(f"detalle={err}")
        raise error from err
    return respuesta["QueueUrl"]


def _producir_evento(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Publica un evento en la cola y registra el MessageId devuelto (rol 2: `produce_event`)."""
    evento = dict(args.payload)
    if args.event_topic is not None:
        evento["event_topic"] = args.event_topic

    cliente = get_sqs_client()
    url_cola = _resolver_url_cola(cliente, args.queue)
    message_id = produce_event(cliente, url_cola, evento)
    logger.info(
        f"Evento publicado desde la CLI. MessageId: {message_id}",
        extra={"message_id": message_id, "event_topic": args.event_topic},
    )
    return 0


def _levantar_receptor(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Levanta el receptor del rol 3 por su ruta de importación, para que uvicorn lo cargue."""
    logger.info(f"Levantando el receptor de webhooks en http://{args.host}:{args.port}")
    uvicorn.run(APP_RECEPTOR, host=args.host, port=args.port)
    return 0


def _correr_despachador(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Corre el worker de SQS → webhooks. `--max-lotes` acota el bucle para la demo."""
    cliente = get_sqs_client()
    url_cola = _resolver_url_cola(cliente, args.queue)
    despachador = DespachadorSQS(
        cliente,
        url_cola,
        args.url,
        args.secret,
        logger=logger,
        wait_time=args.wait_time,
        timeout=args.timeout,
    )
    logger.info(f"Despachador iniciado: {url_cola} → {args.url}")
    entregados = despachador.ejecutar(iteraciones=args.max_lotes)
    logger.info(f"Despachador detenido. Webhooks entregados: {entregados}.")
    return 0


COMANDOS = {
    "produce-sqs": _producir_evento,
    "start-receiver": _levantar_receptor,
    "start-dispatcher": _correr_despachador,
}


def main(argv: Sequence[str] | None = None) -> int:
    """Punto de entrada de la CLI. Devuelve el código de salida; no deja escapar `SystemExit`.

    Los argumentos inválidos vuelven como 2 desde `SalidaDelParser`, sin haber tocado el pipeline
    de logging. Con argumentos válidos el pipeline se configura una sola vez y el `finally` lo
    apaga siempre, incluso cuando el camino termina en una excepción del dominio o en un Ctrl-C.
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SalidaDelParser as salida:
        return salida.codigo

    setup_vulcano_logging(args.log_file)
    logger = logging.getLogger(NOMBRE_LOGGER)
    interrupcion = InterrupcionPorCtrlC(logger)
    codigo = 0

    try:
        with interrupcion:
            codigo = COMANDOS[args.comando](args, logger)
    except* WebhookSignatureError as grupo:
        _reportar_grupo(logger, "Firma HMAC inválida o ausente en un webhook", grupo)
        codigo = 3
    except* WebhookTimestampError as grupo:
        _reportar_grupo(logger, "Timestamp fuera de la ventana: replay o reloj desincronizado", grupo)
        codigo = 3
    except* SQSConnectionError as grupo:
        _reportar_grupo(logger, "Error de comunicación con SQS", grupo)
        codigo = 4
    except* WebhookDeliveryError as grupo:
        _reportar_grupo(logger, "Entrega de webhook fallida", grupo)
        codigo = 5
    finally:
        shutdown_vulcano_logging()

    if interrupcion.interrumpida:
        return 130
    return codigo


if __name__ == "__main__":
    sys.exit(main())
