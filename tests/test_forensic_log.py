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

import pytest

from vulcano_telemetry.logging_engine import (
    CAMPOS_FIJOS,
    CAMPOS_OPCIONALES,
    AsyncJSONFormatter,
    setup_vulcano_logging,
    shutdown_vulcano_logging,
)


@pytest.fixture(autouse=True)
def _cerrar_pipelines():
    """Ninguna prueba deja un QueueListener vivo entre casos."""
    yield
    shutdown_vulcano_logging()


def _leer(ruta) -> list[str]:
    """Líneas crudas de un archivo de log, esté comprimido o no."""
    if ruta.suffix == ".gz":
        with gzip.open(ruta, "rt", encoding="utf-8") as archivo:
            return [linea for linea in archivo.read().splitlines() if linea]
    return [linea for linea in ruta.read_text(encoding="utf-8").splitlines() if linea]


def _registros(ruta) -> list[dict]:
    return [json.loads(linea) for linea in _leer(ruta)]


def test_cada_linea_es_json_con_campos_fijos_y_timestamp_utc(tmp_path):
    log = tmp_path / "forense.log"
    sesion = setup_vulcano_logging(log)

    sesion.logger.info("primer evento")
    sesion.logger.warning("segundo evento")
    sesion.stop()

    registros = _registros(log)

    assert len(registros) == 2
    for registro in registros:
        assert set(CAMPOS_FIJOS) <= set(registro)
        marca = datetime.fromisoformat(registro["timestamp"])
        assert marca.tzinfo is not None, "la marca de tiempo no declara zona horaria"
        assert marca.utcoffset() == timedelta(0), "la marca de tiempo no está en UTC"
        assert registro["timestamp"].endswith("+00:00")

    assert [r["level"] for r in registros] == ["INFO", "WARNING"]
    assert registros[0]["timestamp"] <= registros[1]["timestamp"]


def test_los_campos_de_correlacion_salen_de_extra_y_no_se_inventan(tmp_path):
    log = tmp_path / "forense.log"
    sesion = setup_vulcano_logging(log)

    sesion.logger.info(
        "webhook despachado",
        extra={
            "trace_id": "trace-abc",
            "message_id": "msg-123",
            "webhook_id": "wh-9",
            "signature": "t=1700000000,v1=deadbeef",
            "latency_ms": 42.5,
        },
    )
    sesion.logger.info("evento sin correlación")
    sesion.stop()

    con_correlacion, sin_correlacion = _registros(log)

    assert con_correlacion["trace_id"] == "trace-abc"
    assert con_correlacion["message_id"] == "msg-123"
    assert con_correlacion["webhook_id"] == "wh-9"
    assert con_correlacion["signature"] == "t=1700000000,v1=deadbeef"
    assert con_correlacion["latency_ms"] == 42.5

    for campo in CAMPOS_OPCIONALES:
        assert campo not in sin_correlacion, f"el formatter inventó el campo '{campo}' que no estaba en extra"


def test_un_exception_group_se_serializa_como_arbol_con_sus_notas(tmp_path):
    log = tmp_path / "forense.log"
    sesion = setup_vulcano_logging(log)

    try:
        raise ExceptionGroup("lote fallido", [ValueError("firma inválida"), RuntimeError("SQS caído")])
    except ExceptionGroup as grupo:
        grupo.exceptions[0].add_note("verificar X-Webhook-Signature")
        grupo.exceptions[1].add_note("revisar LocalStack:4566")
        sesion.logger.error("fallo al procesar el lote", exc_info=grupo)
    sesion.stop()

    registro, = _registros(log)
    arbol = registro["exception"]

    assert arbol["type"] == "ExceptionGroup"
    # str() de la stdlib agrega el conteo: "lote fallido (2 sub-exceptions)".
    assert arbol["message"].startswith("lote fallido")
    assert "2 sub-exceptions" in arbol["message"]
    assert len(arbol["exceptions"]) == 2
    assert arbol["exceptions"][0]["type"] == "ValueError"
    assert arbol["exceptions"][0]["notes"] == ["verificar X-Webhook-Signature"]
    assert arbol["exceptions"][1]["notes"] == ["revisar LocalStack:4566"]
    assert "Traceback" in registro["traceback"]


def test_la_rotacion_comprime_a_gzip_sin_perdida_de_datos(tmp_path):
    log = tmp_path / "rotado.log"
    sesion = setup_vulcano_logging(log, max_bytes=2048, backup_count=5)

    emitidos = 30
    for indice in range(emitidos):
        sesion.logger.info("evento %d %s", indice, "x" * 40)
    sesion.stop()

    comprimidos = sorted(tmp_path.glob("rotado.log.*.gz"))
    assert comprimidos, "no se generó ningún respaldo comprimido"
    assert not list(tmp_path.glob("*.tmp")), "quedó un temporal de compresión sin reemplazar"

    lineas = []
    for ruta in [*comprimidos, log]:
        lineas.extend(_leer(ruta))

    assert len(lineas) == emitidos, f"se perdieron registros: {len(lineas)} de {emitidos}"
    assert all(json.loads(linea) for linea in lineas)


def test_la_escritura_ocurre_en_el_hilo_del_listener_y_no_en_el_que_loguea(tmp_path):
    log = tmp_path / "hilos.log"
    sesion = setup_vulcano_logging(log)

    hilos_de_escritura: list[str] = []
    emit_original = sesion.handler_archivo.emit

    def emit_espia(record):
        hilos_de_escritura.append(threading.current_thread().name)
        emit_original(record)

    sesion.handler_archivo.emit = emit_espia
    sesion.logger.info("evento desde el hilo principal")
    sesion.stop()

    assert hilos_de_escritura, "el registro nunca llegó al handler de archivo"
    assert hilos_de_escritura[0] != threading.current_thread().name
    assert hilos_de_escritura[0] != "MainThread"


def test_el_logger_no_tiene_habilitado_ningun_handler_de_archivo_directo(tmp_path):
    sesion = setup_vulcano_logging(tmp_path / "directo.log")

    assert sesion.logger.handlers, "el logger quedó sin handlers"
    for handler in sesion.logger.handlers:
        assert isinstance(handler, logging.handlers.QueueHandler), (
            f"handler de escritura directa detectado: {type(handler).__name__}"
        )
        assert not isinstance(handler, logging.FileHandler)
    assert sesion.logger.propagate is False


def test_llamar_dos_veces_al_setup_no_duplica_lineas(tmp_path):
    log = tmp_path / "idempotente.log"

    primera = setup_vulcano_logging(log)
    segunda = setup_vulcano_logging(log)

    assert segunda is not primera
    assert primera.activa is False
    segunda.logger.info("una sola línea")
    segunda.stop()

    assert len(_leer(log)) == 1


def test_el_formatter_no_necesita_el_pipeline_para_formatear():
    """El formatter es una función pura del LogRecord: se puede usar suelto en cualquier handler."""
    registro = logging.LogRecord(
        name="vulcano_engine",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="suelto",
        args=(),
        exc_info=None,
    )

    datos = json.loads(AsyncJSONFormatter().format(registro))

    assert datos["message"] == "suelto"
    assert set(CAMPOS_FIJOS) <= set(datos)
