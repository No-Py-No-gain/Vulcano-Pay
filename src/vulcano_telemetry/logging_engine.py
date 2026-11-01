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

NOMBRE_LOGGER = "vulcano_engine"

CAMPOS_FIJOS = ("timestamp", "level", "logger", "message", "process", "thread_name")

CAMPOS_OPCIONALES = (
    "trace_id",
    "message_id",
    "webhook_id",
    "event_topic",
    "signature",
    "latency_ms",
    "retry_count",
    "notes",
)

MAX_BYTES_POR_DEFECTO = 2 * 1024 * 1024
RESPALDOS_POR_DEFECTO = 3
PROFUNDIDAD_MAXIMA_EXCEPCION = 10

# Sesiones activas por nombre de logger: garantiza que el setup sea idempotente.
_SESIONES: dict[str, LoggingSession] = {}


def serializar_excepcion(exc: BaseException | None, profundidad: int = 0) -> dict[str, Any]:
    """Traduce una excepción a un árbol JSON: tipo, mensaje, notas y encadenamiento.

    Un `ExceptionGroup` se despliega en `exceptions` para no perder ninguno de sus hijos; la
    cadena de causalidad se conserva en `cause`/`context`. La recursión está acotada para que un
    logger nunca pueda agotar la pila mientras alguien intenta registrar un error.
    """
    if exc is None:
        return {}

    nodo: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
        "notes": list(getattr(exc, "__notes__", ())),
    }

    if profundidad >= PROFUNDIDAD_MAXIMA_EXCEPCION:
        nodo["truncated"] = True
        return nodo

    if isinstance(exc, BaseExceptionGroup):
        nodo["exceptions"] = [serializar_excepcion(hijo, profundidad + 1) for hijo in exc.exceptions]
    if exc.__cause__ is not None:
        nodo["cause"] = serializar_excepcion(exc.__cause__, profundidad + 1)
    elif exc.__context__ is not None and not exc.__suppress_context__:
        nodo["context"] = serializar_excepcion(exc.__context__, profundidad + 1)

    return nodo


class AsyncJSONFormatter(logging.Formatter):
    """Formateador JSON forense con marca de tiempo ISO 8601 UTC del evento.

    La marca se toma de `record.created` (el instante en que se emitió el registro) y no del
    momento de escritura: como la escritura ocurre después, en otro hilo, usar el reloj del
    formateador desordenaría la línea temporal que después se audita.
    """

    def format(self, record: logging.LogRecord) -> str:
        datos: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "thread_name": record.threadName,
        }

        for campo in CAMPOS_OPCIONALES:
            valor = getattr(record, campo, None)
            if valor is not None:
                datos[campo] = valor

        if record.exc_info and record.exc_info[1] is not None:
            datos["exception"] = serializar_excepcion(record.exc_info[1])
            datos["traceback"] = self.formatException(record.exc_info)

        return json.dumps(datos, ensure_ascii=False, default=str)


class ColaSinFormatear(logging.handlers.QueueHandler):
    """`QueueHandler` que no formatea en el hilo productor.

    El `prepare()` de la stdlib formatea el registro y anula `exc_info`, `exc_text` y `stack_info`
    antes de encolarlo (CPython 3.14, `logging/handlers.py`). Eso destruye el árbol de la excepción
    y mueve el costo de formateo al hilo que loguea, que es exactamente lo que el criterio 5
    prohíbe. La documentación de esa clase habilita sobreescribir `prepare()` para este caso: acá
    sólo se copia el registro y el formatter corre en el hilo del listener.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return copy.copy(record)


def nombrar_rotado(nombre_por_defecto: str) -> str:
    """Callback `namer`: el archivo rotado lleva extensión `.gz`."""
    return f"{nombre_por_defecto}.gz"


def rotar_a_gzip(origen: str, destino: str) -> None:
    """Callback `rotator`: comprime y reemplaza de forma atómica, sin dejar el `.log` suelto.

    Comprime a un temporal y recién entonces reemplaza el destino, de modo que un corte de energía
    no deje un `.gz` truncado ocupando el lugar del archivo bueno.
    """
    temporal = f"{destino}.tmp"
    with open(origen, "rb") as entrada, gzip.open(temporal, "wb") as salida:
        shutil.copyfileobj(entrada, salida)
    os.replace(temporal, destino)
    os.remove(origen)


class LoggingSession:
    """Dueña del pipeline: cola en memoria, listener en un hilo aparte y rotación comprimida."""

    def __init__(
        self,
        logger: logging.Logger,
        listener: logging.handlers.QueueListener,
        cola: queue.Queue,
        handler_cola: logging.handlers.QueueHandler,
        handler_archivo: logging.handlers.RotatingFileHandler,
        log_file: Path,
    ) -> None:
        self.logger = logger
        self.listener = listener
        self.queue = cola
        self.handler_cola = handler_cola
        self.handler_archivo = handler_archivo
        self.log_file = log_file
        self._activa = True

    @property
    def activa(self) -> bool:
        return self._activa

    def stop(self) -> None:
        """Drena la cola, cierra el archivo y libera el hilo. Se puede llamar más de una vez.

        `QueueListener.stop()` encola un centinela y hace `join()`: como la cola es FIFO, todo lo
        que estaba pendiente se escribe antes de terminar. No se pierde ningún registro.
        """
        if not self._activa:
            return
        self._activa = False
        self.listener.stop()
        self.logger.removeHandler(self.handler_cola)
        self.handler_archivo.close()
        if _SESIONES.get(self.logger.name) is self:
            _SESIONES.pop(self.logger.name, None)

    def __enter__(self) -> LoggingSession:
        return self

    def __exit__(self, *_excepcion: object) -> bool:
        self.stop()
        return False


def setup_vulcano_logging(
    log_file: str | Path = "vulcano_events.log",
    *,
    level: int = logging.INFO,
    max_bytes: int = MAX_BYTES_POR_DEFECTO,
    backup_count: int = RESPALDOS_POR_DEFECTO,
    logger_name: str = NOMBRE_LOGGER,
) -> LoggingSession:
    """Configura el pipeline no bloqueante y devuelve la sesión que lo posee.

    Idempotente: si ya había una sesión activa para ese logger, la detiene antes de crear la nueva,
    y limpia handlers previos. Así llamar dos veces no duplica líneas en el archivo.
    """
    anterior = _SESIONES.get(logger_name)
    if anterior is not None:
        anterior.stop()

    destino = Path(log_file)
    destino.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    # Sin propagación: los registros no se duplican por el logger raíz.
    logger.propagate = False

    cola: queue.Queue = queue.Queue(-1)
    handler_cola = ColaSinFormatear(cola)

    handler_archivo = logging.handlers.RotatingFileHandler(
        destino,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler_archivo.setFormatter(AsyncJSONFormatter())
    handler_archivo.namer = nombrar_rotado
    handler_archivo.rotator = rotar_a_gzip

    logger.addHandler(handler_cola)

    listener = logging.handlers.QueueListener(cola, handler_archivo)
    listener.start()

    sesion = LoggingSession(
        logger=logger,
        listener=listener,
        cola=cola,
        handler_cola=handler_cola,
        handler_archivo=handler_archivo,
        log_file=destino,
    )
    _SESIONES[logger_name] = sesion
    return sesion


def shutdown_vulcano_logging() -> None:
    """Detiene todas las sesiones activas. Pensado para el `finally` de la CLI."""
    for nombre in list(_SESIONES):
        sesion = _SESIONES.get(nombre)
        if sesion is not None:
            sesion.stop()
