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
   con `logging.getLogger(LOGGER_NAME)` una vez que el pipeline ya está configurado.
2. `main()` devuelve el código de salida como `int` y no deja escapar el `SystemExit` de argparse:
   `VulcanoParser.exit()` lo convierte en `ParserExit` y `main()` lo traduce a su código de
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
from vulcano_telemetry.dispatcher import SQSDispatcher, dispatch_webhook
from vulcano_telemetry.exceptions import (
    SQSConnectionError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.logging_engine import (
    LOGGER_NAME,
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

DEFAULT_QUEUE = "vulcano-events-queue"
DEFAULT_WAIT_TIME = 10
DEFAULT_URL = "http://127.0.0.1:8000/webhook"
DEFAULT_SECRET = "mi_clave_secreta_super_segura_vulcano_123"
DEFAULT_TIMEOUT = 5.0
DEFAULT_LOG_FILE = "vulcano_events.log"
RECEIVER_APP = "vulcano_telemetry.webhook_receiver:app"

# Evento de ejemplo para que `produce-sqs` sirva en la demo sin escribir un JSON a mano.
SAMPLE_EVENT: dict[str, Any] = {
    "transaction_id": "tx_9981",
    "amount": 350.0,
    "status": "COMPLETED",
    "event_topic": "pago.aprobado",
}

# Tabla de códigos de salida: es el `epilog` del parser y por eso aparece en `--help`.
EXIT_CODE_TABLE = """\
Códigos de salida:
  0    operación completada correctamente
  2    error de validación de argumentos (lo emite argparse)
  3    firma HMAC inválida o timestamp fuera de la ventana (posible replay)
  4    error de comunicación con SQS, incluida una cola inexistente
  5    entrega del webhook fallida
  130  interrupción por Ctrl-C
"""


class ParserExit(Exception):
    """Corte del parser con un código de salida; reemplaza al `SystemExit` de argparse.

    El criterio 1 del trabajo incluye a `SystemExit` entre las excepciones que ningún `except` debe
    capturar, así que el parser no aborta el proceso: levanta esta excepción propia, que `main()`
    traduce a su valor de retorno.
    """

    def __init__(self, exit_code: int) -> None:
        super().__init__(f"el parser terminó con el código {exit_code}")
        self.exit_code = exit_code


class VulcanoParser(argparse.ArgumentParser):
    """`ArgumentParser` que no llama a `sys.exit()`: `--help` y los errores vuelven como código."""

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if message:
            self._print_message(message, sys.stderr)
        raise ParserExit(status)


class CtrlCInterruption:
    """Convierte un Ctrl-C en el código de salida 130, sin traceback.

    No se puede escribir `except KeyboardInterrupt`: el criterio 1 del proyecto lo cuenta entre las
    excepciones que ningún `except` debe capturar. Acá la interrupción llega como argumento de
    `__exit__`, se registra y se transforma en una bandera que `main()` traduce a 130. Es una
    interrupción deliberada del operador, no un error silenciado.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.interrupted = False

    def __enter__(self) -> CtrlCInterruption:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None or not issubclass(exc_type, KeyboardInterrupt):
            return False
        self.interrupted = True
        self.logger.warning("Interrupción por Ctrl-C: se detiene la operación (código de salida 130).")
        return True


def parse_payload(value: str) -> dict[str, Any]:
    """Payload JSON en texto → diccionario. Un payload inservible es validación (código 2)."""
    try:
        event = json.loads(value)
    except json.JSONDecodeError as err:
        raise argparse.ArgumentTypeError(f"Payload inválido: no es JSON ({err.msg}).") from err
    if not isinstance(event, dict):
        raise argparse.ArgumentTypeError("Payload inválido: el evento debe ser un objeto JSON.")
    return event


def _add_shared_options(parser: argparse.ArgumentParser) -> None:
    """Opciones comunes a los tres subcomandos, con los sanitizadores del rol 1 como `type`."""
    parser.add_argument(
        "-q",
        "--queue",
        type=parse_queue_name,
        default=DEFAULT_QUEUE,
        help=f"Nombre de la cola SQS (patrón vulcano-<nombre>-queue, por defecto {DEFAULT_QUEUE}).",
    )
    parser.add_argument(
        "-w",
        "--wait-time",
        type=parse_wait_time,
        default=DEFAULT_WAIT_TIME,
        help=f"Segundos de long polling en SQS, entre 1 y 20 (por defecto {DEFAULT_WAIT_TIME}).",
    )
    parser.add_argument(
        "--url",
        type=parse_webhook_url,
        default=DEFAULT_URL,
        help=f"URL HTTP/HTTPS del receptor de webhooks (por defecto {DEFAULT_URL}).",
    )
    parser.add_argument(
        "--secret",
        type=parse_secret_key,
        default=DEFAULT_SECRET,
        help="Clave secreta HMAC de al menos 16 caracteres.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout en segundos de cada POST de webhook (por defecto {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("VULCANO_LOG_FILE") or DEFAULT_LOG_FILE,
        help=f"Archivo de log JSON con rotación gzip (por defecto VULCANO_LOG_FILE o {DEFAULT_LOG_FILE}).",
    )


def build_parser() -> argparse.ArgumentParser:
    """Parser con los tres subcomandos de S7 y la tabla de códigos de salida como `epilog`."""
    parser = VulcanoParser(
        prog="vulcano",
        description="Motor de eventos SQS y webhooks HMAC de Vulcano Pay.",
        epilog=EXIT_CODE_TABLE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="subcomandos",
        metavar="{produce-sqs,start-receiver,start-dispatcher}",
    )

    produce_parser = subparsers.add_parser(
        "produce-sqs",
        help="Publica un evento en la cola SQS.",
        description="Publica un evento JSON en la cola y registra el MessageId devuelto por SQS.",
    )
    _add_shared_options(produce_parser)
    produce_parser.add_argument(
        "--payload",
        type=parse_payload,
        default=dict(SAMPLE_EVENT),
        help="Evento a publicar, como JSON en texto (por defecto un evento de ejemplo).",
    )
    produce_parser.add_argument(
        "--event-topic",
        default=None,
        help="Tema del evento: se inyecta en el cuerpo y queda en el registro forense.",
    )

    receiver_parser = subparsers.add_parser(
        "start-receiver",
        help="Levanta el receptor HTTP de webhooks.",
        description="Levanta el receptor FastAPI de webhooks con uvicorn.",
    )
    _add_shared_options(receiver_parser)
    receiver_parser.add_argument("--host", default="127.0.0.1", help="Interfaz donde escucha el receptor (127.0.0.1).")
    receiver_parser.add_argument("--port", type=int, default=8000, help="Puerto del receptor (8000).")

    dispatcher = subparsers.add_parser(
        "start-dispatcher",
        help="Consume la cola y despacha los webhooks firmados.",
        description="Consume la cola SQS y entrega cada evento firmado a la URL configurada.",
    )
    _add_shared_options(dispatcher)
    dispatcher.add_argument(
        "--max-lotes",
        dest="max_batches",
        metavar="MAX_LOTES",
        type=int,
        default=None,
        help="Corta el bucle después de N lotes (por defecto corre hasta Ctrl-C).",
    )

    return parser


def _report_group(logger: logging.Logger, context: str, group: ExceptionGroup) -> None:
    """Registra el grupo completo: el árbol del `ExceptionGroup` y cada hijo con sus notas."""
    logger.error(f"{context} ({len(group.exceptions)} error(es)).", exc_info=group)
    for exc in group.exceptions:
        notes = list(getattr(exc, "__notes__", ()))
        logger.error(f"  • {exc} | notas={notes}")


def _resolve_queue_url(client: Any, queue_name: str) -> str:
    """URL real de la cola, pedida al servicio a partir del nombre. Nunca se concatena a mano."""
    try:
        response = client.get_queue_url(QueueName=queue_name)
    except ClientError as err:
        error = SQSConnectionError(f"No se pudo obtener la URL de la cola '{queue_name}'.")
        error.add_note(f"QueueName: {queue_name}")
        error.add_note(f"detalle={err}")
        raise error from err
    return response["QueueUrl"]


def _produce_event(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Publica un evento en la cola y registra el MessageId devuelto (rol 2: `produce_event`)."""
    event = dict(args.payload)
    if args.event_topic is not None:
        event["event_topic"] = args.event_topic

    client = get_sqs_client()
    queue_url = _resolve_queue_url(client, args.queue)
    message_id = produce_event(client, queue_url, event)
    logger.info(
        f"Evento publicado desde la CLI. MessageId: {message_id}",
        extra={"message_id": message_id, "event_topic": args.event_topic},
    )
    return 0


def _start_receiver(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Levanta el receptor del rol 3 por su ruta de importación, para que uvicorn lo cargue."""
    logger.info(f"Levantando el receptor de webhooks en http://{args.host}:{args.port}")
    uvicorn.run(RECEIVER_APP, host=args.host, port=args.port)
    return 0


def _run_dispatcher(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Corre el worker de SQS → webhooks. `--max-lotes` acota el bucle para la demo."""
    client = get_sqs_client()
    queue_url = _resolve_queue_url(client, args.queue)
    dispatcher = SQSDispatcher(
        client,
        queue_url,
        args.url,
        args.secret,
        logger=logger,
        wait_time=args.wait_time,
        timeout=args.timeout,
    )
    logger.info(f"Despachador iniciado: {queue_url} → {args.url}")
    delivered = dispatcher.run(iterations=args.max_batches)
    logger.info(f"Despachador detenido. Webhooks entregados: {delivered}.")
    return 0


COMMANDS = {
    "produce-sqs": _produce_event,
    "start-receiver": _start_receiver,
    "start-dispatcher": _run_dispatcher,
}


def main(argv: Sequence[str] | None = None) -> int:
    """Punto de entrada de la CLI. Devuelve el código de salida; no deja escapar `SystemExit`.

    Los argumentos inválidos vuelven como 2 desde `ParserExit`, sin haber tocado el pipeline
    de logging. Con argumentos válidos el pipeline se configura una sola vez y el `finally` lo
    apaga siempre, incluso cuando el camino termina en una excepción del dominio o en un Ctrl-C.
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except ParserExit as parser_exit:
        return parser_exit.exit_code

    setup_vulcano_logging(args.log_file)
    logger = logging.getLogger(LOGGER_NAME)
    interruption = CtrlCInterruption(logger)
    exit_code = 0

    try:
        with interruption:
            exit_code = COMMANDS[args.command](args, logger)
    except* WebhookSignatureError as group:
        _report_group(logger, "Firma HMAC inválida o ausente en un webhook", group)
        exit_code = 3
    except* WebhookTimestampError as group:
        _report_group(logger, "Timestamp fuera de la ventana: replay o reloj desincronizado", group)
        exit_code = 3
    except* SQSConnectionError as group:
        _report_group(logger, "Error de comunicación con SQS", group)
        exit_code = 4
    except* WebhookDeliveryError as group:
        _report_group(logger, "Entrega de webhook fallida", group)
        exit_code = 5
    finally:
        shutdown_vulcano_logging()

    if interruption.interrupted:
        return 130
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
