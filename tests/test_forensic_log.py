"""Validador forense de la telemetría de Vulcano Pay (rol 6).

Comprueba, sobre los archivos que produce el sistema y no sobre su estado interno:
- el formato JSON y la marca de tiempo ISO 8601 UTC (criterio 6);
- el contenido de correlación: `trace_id`, `message_id`, `webhook_id`, `signature` (S12);
- la rotación comprimida con gzip y la ausencia de pérdida de datos (S4, S12);
- que la escritura a disco ocurre en el hilo del `QueueListener` y no en el que loguea (S2).
"""

import gzip
import json
import logging.handlers
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from vulcano_telemetry.dispatcher import SQSDispatcher
from vulcano_telemetry.logging_engine import (
    FIXED_FIELDS,
    OPTIONAL_FIELDS,
    AsyncJSONFormatter,
    setup_vulcano_logging,
    shutdown_vulcano_logging,
)


@pytest.fixture(autouse=True)
def _close_pipelines():
    """Ninguna prueba deja un QueueListener vivo entre casos."""
    yield
    shutdown_vulcano_logging()


def _read_lines(path) -> list[str]:
    """Líneas crudas de un archivo de log, esté comprimido o no."""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return [line for line in handle.read().splitlines() if line]
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


def _read_records(path) -> list[dict]:
    return [json.loads(line) for line in _read_lines(path)]


def test_each_line_is_json_with_fixed_fields_and_utc_timestamp(tmp_path):
    log = tmp_path / "forense.log"
    session = setup_vulcano_logging(log)

    session.logger.info("primer evento")
    session.logger.warning("segundo evento")
    session.stop()

    records = _read_records(log)

    assert len(records) == 2
    for record in records:
        assert set(FIXED_FIELDS) <= set(record)
        timestamp = datetime.fromisoformat(record["timestamp"])
        assert timestamp.tzinfo is not None, "la marca de tiempo no declara zona horaria"
        assert timestamp.utcoffset() == timedelta(0), "la marca de tiempo no está en UTC"
        assert record["timestamp"].endswith("+00:00")

    assert [r["level"] for r in records] == ["INFO", "WARNING"]
    assert records[0]["timestamp"] <= records[1]["timestamp"]


def test_correlation_fields_come_from_extra_and_are_not_invented(tmp_path):
    log = tmp_path / "forense.log"
    session = setup_vulcano_logging(log)

    session.logger.info(
        "webhook despachado",
        extra={
            "trace_id": "trace-abc",
            "message_id": "msg-123",
            "webhook_id": "wh-9",
            "signature": "t=1700000000,v1=deadbeef",
            "latency_ms": 42.5,
        },
    )
    session.logger.info("evento sin correlación")
    session.stop()

    with_correlation, without_correlation = _read_records(log)

    assert with_correlation["trace_id"] == "trace-abc"
    assert with_correlation["message_id"] == "msg-123"
    assert with_correlation["webhook_id"] == "wh-9"
    assert with_correlation["signature"] == "t=1700000000,v1=deadbeef"
    assert with_correlation["latency_ms"] == 42.5

    for field in OPTIONAL_FIELDS:
        assert field not in without_correlation, f"el formatter inventó el campo '{field}' que no estaba en extra"


def test_exception_group_is_serialized_as_a_tree_with_notes(tmp_path):
    log = tmp_path / "forense.log"
    session = setup_vulcano_logging(log)

    try:
        raise ExceptionGroup("lote fallido", [ValueError("firma inválida"), RuntimeError("SQS caído")])
    except ExceptionGroup as group:
        group.exceptions[0].add_note("verificar X-Webhook-Signature")
        group.exceptions[1].add_note("revisar LocalStack:4566")
        session.logger.error("fallo al procesar el lote", exc_info=group)
    session.stop()

    record, = _read_records(log)
    tree = record["exception"]

    assert tree["type"] == "ExceptionGroup"
    # str() de la stdlib agrega el conteo: "lote fallido (2 sub-exceptions)".
    assert tree["message"].startswith("lote fallido")
    assert "2 sub-exceptions" in tree["message"]
    assert len(tree["exceptions"]) == 2
    assert tree["exceptions"][0]["type"] == "ValueError"
    assert tree["exceptions"][0]["notes"] == ["verificar X-Webhook-Signature"]
    assert tree["exceptions"][1]["notes"] == ["revisar LocalStack:4566"]
    assert "Traceback" in record["traceback"]


def test_rotation_compresses_to_gzip_without_data_loss(tmp_path):
    log = tmp_path / "rotado.log"
    session = setup_vulcano_logging(log, max_bytes=2048, backup_count=5)

    emitted_count = 30
    for index in range(emitted_count):
        session.logger.info("evento %d %s", index, "x" * 40)
    session.stop()

    compressed = sorted(tmp_path.glob("rotado.log.*.gz"))
    assert compressed, "no se generó ningún respaldo comprimido"
    assert not list(tmp_path.glob("*.tmp")), "quedó un temporal de compresión sin reemplazar"

    lines = []
    for path in [*compressed, log]:
        lines.extend(_read_lines(path))

    assert len(lines) == emitted_count, f"se perdieron registros: {len(lines)} de {emitted_count}"
    assert all(json.loads(line) for line in lines)


def test_writes_happen_in_the_listener_thread_not_the_logging_one(tmp_path):
    log = tmp_path / "hilos.log"
    session = setup_vulcano_logging(log)

    writing_threads: list[str] = []
    original_emit = session.file_handler.emit

    def spy_emit(record):
        writing_threads.append(threading.current_thread().name)
        original_emit(record)

    session.file_handler.emit = spy_emit
    session.logger.info("evento desde el hilo principal")
    session.stop()

    assert writing_threads, "el registro nunca llegó al handler de archivo"
    assert writing_threads[0] != threading.current_thread().name
    assert writing_threads[0] != "MainThread"


def test_logger_has_no_direct_file_handler(tmp_path):
    session = setup_vulcano_logging(tmp_path / "directo.log")

    assert session.logger.handlers, "el logger quedó sin handlers"
    for handler in session.logger.handlers:
        assert isinstance(handler, logging.handlers.QueueHandler), (
            f"handler de escritura directa detectado: {type(handler).__name__}"
        )
        assert not isinstance(handler, logging.FileHandler)
    assert session.logger.propagate is False


def test_calling_setup_twice_does_not_duplicate_lines(tmp_path):
    log = tmp_path / "idempotente.log"

    first = setup_vulcano_logging(log)
    second = setup_vulcano_logging(log)

    assert second is not first
    assert first.active is False
    second.logger.info("una sola línea")
    second.stop()

    assert len(_read_lines(log)) == 1


def test_the_formatter_does_not_need_the_pipeline_to_format():
    """El formatter es una función pura del LogRecord: se puede usar suelto en cualquier handler."""
    record = logging.LogRecord(
        name="vulcano_engine",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="suelto",
        args=(),
        exc_info=None,
    )

    data = json.loads(AsyncJSONFormatter().format(record))

    assert data["message"] == "suelto"
    assert set(FIXED_FIELDS) <= set(data)


class _FailingTarget:
    """Destino HTTP local que responde 500: fuerza el camino de fallo del despacho."""

    def __init__(self) -> None:
        target = self
        target.requests_seen = 0

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                target.requests_seen += 1
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_port}/webhook"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def failing_target():
    target = _FailingTarget()
    yield target
    target.close()


class _SingleMessageSQS:
    """Doble mínimo del cliente SQS: entrega un mensaje y registra los borrados."""

    def __init__(self, message: dict) -> None:
        self._pending = message
        self.deleted_handles: list[str] = []

    def receive_message(self, **_kwargs: object) -> dict:
        message, self._pending = self._pending, None
        return {"Messages": [message]} if message else {}

    def delete_message(self, QueueUrl: str, ReceiptHandle: str) -> dict:
        self.deleted_handles.append(ReceiptHandle)
        return {}


def test_real_dispatch_record_keeps_trace_signature_and_latency(tmp_path, failing_target):
    """S12 sobre el sistema: el registro lo produce el worker de verdad, no una llamada al formatter.

    Se lee el archivo que quedó en disco —incluidos los respaldos comprimidos— y se comprueba que la
    línea del despacho lleve TraceId, MessageId, el contador de reintentos y la cabecera
    X-Webhook-Signature que se envió.
    """
    log = tmp_path / "despacho.log"
    session = setup_vulcano_logging(log, max_bytes=700, backup_count=5)
    message = {
        "MessageId": "msg-e2e",
        "ReceiptHandle": "rh-e2e",
        "Body": json.dumps({"transaction_id": "tx_e2e"}),
        "Attributes": {"ApproximateReceiveCount": "2"},
        "MessageAttributes": {
            "TraceId": {"StringValue": "trace-e2e"},
            "WebhookId": {"StringValue": "wh-e2e"},
        },
    }
    dispatcher = SQSDispatcher(
        client=_SingleMessageSQS(message),
        queue_url="http://localhost:4566/000000000000/cola-e2e",
        target_url=failing_target.url,
        secret="secreto_de_prueba_de_16_mas",
        logger=session.logger,
        timeout=5.0,
    )

    assert dispatcher.process_batch() == 0, "un destino que responde 500 no puede contar como entrega"
    for index in range(6):
        session.logger.info("relleno para forzar la rotación %s %s", index, "y" * 60)
    session.stop()

    files = [*sorted(tmp_path.glob("despacho.log.*.gz")), log]
    assert any(path.suffix == ".gz" for path in files), "no se generó ningún .log.gz del flujo real"

    records = [json.loads(line) for path in files for line in _read_lines(path)]
    of_dispatch = [record for record in records if record.get("message_id") == "msg-e2e"]

    assert len(of_dispatch) == 1, f"el despacho real no dejó exactamente un registro: {of_dispatch}"
    record = of_dispatch[0]
    assert record["trace_id"] == "trace-e2e", "el TraceId de SQS no llegó al registro"
    assert record["webhook_id"] == "wh-e2e", "el WebhookId del atributo no llegó al registro"
    assert record["retry_count"] == 2, "el contador de reintentos no llegó al registro"
    assert isinstance(record["latency_ms"], (int, float)), "la latencia de la entrega no quedó registrada"
    notes = " ".join(record.get("notes", []))
    assert "X-Webhook-Signature=" in notes, f"el registro no conserva la firma enviada: {record}"
    assert record["exception"]["type"] == "WebhookDeliveryError"
