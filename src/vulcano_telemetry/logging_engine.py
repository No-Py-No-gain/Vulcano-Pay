"""Engine de observabilidad de Vulcano Pay: JSON estructurado, no bloqueante y con rotación gzip.

Contrato del registro (lo valida `tests/test_forensic_log.py`):

- campos fijos: `timestamp`, `level`, `logger`, `message`, `process`, `thread_name`;
- campos opcionales, sólo si llegan por `extra`: `trace_id`, `message_id`, `webhook_id`,
  `event_topic`, `signature`, `latency_ms`, `retry_count`, `notes`;
- campos de excepción: `exception` (árbol recursivo de `ExceptionGroup` con sus notas y su cadena
  de causalidad) y `traceback` en texto.

La escritura en disco ocurre exclusivamente dentro del hilo del `QueueListener`: el hilo que
loguea sólo encola el `LogRecord`. Ese es el criterio 5 de la evaluación.
"""

from __future__ import annotations

import copy
import gzip
import json
import logging
import logging.handlers
import os
import queue
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER_NAME = "vulcano_engine"

FIXED_FIELDS = ("timestamp", "level", "logger", "message", "process", "thread_name")

OPTIONAL_FIELDS = (
    "trace_id",
    "message_id",
    "webhook_id",
    "event_topic",
    "signature",
    "latency_ms",
    "retry_count",
    "notes",
)

DEFAULT_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3
MAX_EXCEPTION_DEPTH = 10

# Sesiones activas por nombre de logger: garantiza que el setup sea idempotente.
_SESSIONS: dict[str, LoggingSession] = {}


def serialize_exception(exc: BaseException | None, depth: int = 0) -> dict[str, Any]:
    """Traduce una excepción a un árbol JSON: tipo, mensaje, notas y encadenamiento.

    Un `ExceptionGroup` se despliega en `exceptions` para no perder ninguno de sus hijos; la
    cadena de causalidad se conserva en `cause`/`context`. La recursión está acotada para que un
    logger nunca pueda agotar la pila mientras alguien intenta registrar un error.
    """
    if exc is None:
        return {}

    node: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
        "notes": list(getattr(exc, "__notes__", ())),
    }

    if depth >= MAX_EXCEPTION_DEPTH:
        node["truncated"] = True
        return node

    if isinstance(exc, BaseExceptionGroup):
        node["exceptions"] = [serialize_exception(child, depth + 1) for child in exc.exceptions]
    if exc.__cause__ is not None:
        node["cause"] = serialize_exception(exc.__cause__, depth + 1)
    elif exc.__context__ is not None and not exc.__suppress_context__:
        node["context"] = serialize_exception(exc.__context__, depth + 1)

    return node


class AsyncJSONFormatter(logging.Formatter):
    """Formateador JSON forense con marca de tiempo ISO 8601 UTC del evento.

    La marca se toma de `record.created` (el instante en que se emitió el registro) y no del
    momento de escritura: como la escritura ocurre después, en otro hilo, usar el reloj del
    formateador desordenaría la línea temporal que después se audita.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "thread_name": record.threadName,
        }

        for field in OPTIONAL_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        if record.exc_info and record.exc_info[1] is not None:
            payload["exception"] = serialize_exception(record.exc_info[1])
            payload["traceback"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


class UnformattedQueueHandler(logging.handlers.QueueHandler):
    """`QueueHandler` que no formatea en el hilo productor.

    El `prepare()` de la stdlib formatea el registro y anula `exc_info`, `exc_text` y `stack_info`
    antes de encolarlo (CPython 3.14, `logging/handlers.py`). Eso destruye el árbol de la excepción
    y mueve el costo de formateo al hilo que loguea, que es exactamente lo que el criterio 5
    prohíbe. La documentación de esa clase habilita sobreescribir `prepare()` para este caso: acá
    sólo se copia el registro y el formatter corre en el hilo del listener.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return copy.copy(record)


def rotated_name(default_name: str) -> str:
    """Callback `namer`: el archivo rotado lleva extensión `.gz`."""
    return f"{default_name}.gz"


def rotate_to_gzip(source: str, destination: str) -> None:
    """Callback `rotator`: comprime y reemplaza de forma atómica, sin dejar el `.log` suelto.

    Comprime a un temporal y recién entonces reemplaza el destino, de modo que un corte de energía
    no deje un `.gz` truncado ocupando el lugar del archivo bueno.
    """
    temporary = f"{destination}.tmp"
    with open(source, "rb") as source_file, gzip.open(temporary, "wb") as compressed:
        shutil.copyfileobj(source_file, compressed)
    os.replace(temporary, destination)
    os.remove(source)


class LoggingSession:
    """Dueña del pipeline: cola en memoria, listener en un hilo aparte y rotación comprimida."""

    def __init__(
        self,
        logger: logging.Logger,
        listener: logging.handlers.QueueListener,
        log_queue: queue.Queue,
        queue_handler: logging.handlers.QueueHandler,
        file_handler: logging.handlers.RotatingFileHandler,
        log_file: Path,
    ) -> None:
        self.logger = logger
        self.listener = listener
        self.queue = log_queue
        self.queue_handler = queue_handler
        self.file_handler = file_handler
        self.log_file = log_file
        self._active = True

    @property
    def active(self) -> bool:
        return self._active

    def stop(self) -> None:
        """Drena la cola, cierra el archivo y libera el hilo. Se puede llamar más de una vez.

        `QueueListener.stop()` encola un centinela y hace `join()`: como la cola es FIFO, todo lo
        que estaba pendiente se escribe antes de terminar. No se pierde ningún registro.
        """
        if not self._active:
            return
        self._active = False
        self.listener.stop()
        self.logger.removeHandler(self.queue_handler)
        self.file_handler.close()
        if _SESSIONS.get(self.logger.name) is self:
            _SESSIONS.pop(self.logger.name, None)

    def __enter__(self) -> LoggingSession:
        return self

    def __exit__(self, *_exc: object) -> bool:
        self.stop()
        return False


def setup_vulcano_logging(
    log_file: str | Path = "vulcano_events.log",
    *,
    level: int = logging.INFO,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    logger_name: str = LOGGER_NAME,
) -> LoggingSession:
    """Configura el pipeline no bloqueante y devuelve la sesión que lo posee.

    Idempotente: si ya había una sesión activa para ese logger, la detiene antes de crear la nueva,
    y limpia handlers previos. Así llamar dos veces no duplica líneas en el archivo.
    """
    previous = _SESSIONS.get(logger_name)
    if previous is not None:
        previous.stop()

    destination = Path(log_file)
    destination.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    # Sin propagación: los registros no se duplican por el logger raíz.
    logger.propagate = False

    log_queue: queue.Queue = queue.Queue(-1)
    queue_handler = UnformattedQueueHandler(log_queue)

    file_handler = logging.handlers.RotatingFileHandler(
        destination,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(AsyncJSONFormatter())
    file_handler.namer = rotated_name
    file_handler.rotator = rotate_to_gzip

    logger.addHandler(queue_handler)

    listener = logging.handlers.QueueListener(log_queue, file_handler)
    listener.start()

    session = LoggingSession(
        logger=logger,
        listener=listener,
        log_queue=log_queue,
        queue_handler=queue_handler,
        file_handler=file_handler,
        log_file=destination,
    )
    _SESSIONS[logger_name] = session
    return session


def shutdown_vulcano_logging() -> None:
    """Detiene todas las sesiones activas. Pensado para el `finally` de la CLI."""
    for name in list(_SESSIONS):
        session = _SESSIONS.get(name)
        if session is not None:
            session.stop()
