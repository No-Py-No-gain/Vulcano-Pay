"""CLI de Vulcano Pay (`src/app_operator.py`, rol 5): subcomandos, exit codes y apagado ordenado.

Todo es unitario: sin Docker, sin red y sin LocalStack. Los dobles son clases locales que levantan
las excepciones del dominio donde las levantaría el sistema real, y el pipeline de logging se
reemplaza por un handler en memoria para no escribir archivos.

Convención de `main()` (la misma que documenta el módulo): devuelve el código de salida como `int`
y nunca deja escapar `SystemExit`; por eso `--help` devuelve 0 y cualquier error de validación de
argparse devuelve 2. El bloque `if __name__ == "__main__":` es el que hace `sys.exit(main())`.

Lo que se verifica, por regla del enunciado:
- el parser expone los tres subcomandos de S7 y los cuatro sanitizadores del rol 1 integrados;
- cada grupo de excepciones del dominio tiene su código de salida (`except*`, S7/S8);
- el `finally` apaga el pipeline incluso cuando el camino termina en excepción (S8, PEP 765);
- importar la CLI no escribe ningún archivo de log (la observabilidad arranca en `main()`).
"""

from __future__ import annotations

import importlib.util
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

import app_operator
from vulcano_telemetry.exceptions import (
    SQSConnectionError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.logging_engine import LOGGER_NAME

ROOT = Path(__file__).resolve().parents[1]
CLI_PATH = ROOT / "src" / "app_operator.py"
QUEUE_URL = "http://localhost:4566/000000000000/vulcano-events-queue"
VALID_SECRET = "clave-secreta-de-prueba-suficientemente-larga"
SUBCOMMANDS = ("produce-sqs", "start-receiver", "start-dispatcher")


class RecordCollector(logging.Handler):
    """Handler en memoria: deja ver los registros emitidos sin tocar el disco."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@dataclass
class ObservedPipeline:
    """Lo que el test puede observar del pipeline de logging reemplazado."""

    records: list[logging.LogRecord]
    setups: list[dict[str, Any]]
    shutdowns: list[int]


class SQSClientWithQueue:
    """Doble de SQS: resuelve el nombre de la cola a una URL sin hablar con AWS."""

    def __init__(self, url: str = QUEUE_URL) -> None:
        self.url = url
        self.queue_url_calls: list[dict[str, Any]] = []

    def get_queue_url(self, **arguments: Any) -> dict[str, Any]:
        self.queue_url_calls.append(arguments)
        return {"QueueUrl": self.url}


class SQSClientMissingQueue:
    """Doble de SQS para la cola que no existe: Boto3 levanta `ClientError`."""

    def get_queue_url(self, **_kwargs: Any) -> dict[str, Any]:
        raise ClientError(
            {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "does not exist"}},
            "GetQueueUrl",
        )


def _dispatcher_double(monkeypatch, exception: type[BaseException] | None = None, calls: list[Any] | None = None):
    """Reemplaza `DespachadorSQS` por un doble que registra su invocación y/o falla al ejecutar."""

    class DispatcherDouble:
        def __init__(self, client, queue_url, target_url, secret, logger=None, **options: Any) -> None:
            self.invocation = {
                "client": client,
                "queue_url": queue_url,
                "target_url": target_url,
                "secret": secret,
                "logger": logger,
                **options,
            }
            if calls is not None:
                calls.append(self.invocation)

        def run(self, iterations: int | None = None) -> int:
            if calls is not None:
                calls.append({"iterations": iterations})
            if exception is not None:
                raise exception("fallo simulado dentro del despachador")
            return 0

    monkeypatch.setattr(app_operator, "SQSDispatcher", DispatcherDouble)


@pytest.fixture
def pipeline(monkeypatch):
    """Pipeline de logging reemplazado: nada escribe archivos y todo registro queda observable."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()
    collector = RecordCollector()
    logger.addHandler(collector)

    setups: list[dict[str, Any]] = []
    shutdowns: list[int] = []

    def fake_setup(log_file="vulcano_events.log", **_options: Any):
        setups.append({"log_file": str(log_file)})
        return object()

    def fake_shutdown() -> None:
        shutdowns.append(1)

    monkeypatch.setattr(app_operator, "setup_vulcano_logging", fake_setup)
    monkeypatch.setattr(app_operator, "shutdown_vulcano_logging", fake_shutdown)

    yield ObservedPipeline(records=collector.records, setups=setups, shutdowns=shutdowns)

    logger.removeHandler(collector)
    logger.handlers.clear()


def test_help_lists_the_three_subcommands():
    """S7: la ayuda del parser publica los tres subcomandos acordados."""
    help_text = app_operator.build_parser().format_help()

    for subcommand in SUBCOMMANDS:
        assert subcommand in help_text, f"la ayuda no documenta el subcomando {subcommand}"
    assert "130" in help_text, "la ayuda debe publicar la tabla de códigos de salida"


def test_missing_subcommand_exits_with_code_2(capsys):
    """Convención de `main()`: devuelve 2 en vez de dejar escapar el `SystemExit` de argparse."""
    assert app_operator.main(argv=[]) == 2

    error = capsys.readouterr().err

    assert "obligatorio" in error.lower() or "required" in error.lower(), (
        f"el error no explica que falta el subcomando: {error!r}"
    )


def test_invalid_queue_name_exits_with_code_2(capsys, pipeline):
    """El sanitizador del rol 1 se integra como `type` de argparse y su mensaje llega al usuario."""
    assert app_operator.main(["produce-sqs", "-q", "cola-invalida"]) == 2

    error = capsys.readouterr().err

    assert "Nombre de cola inválido 'cola-invalida'" in error, f"falta el mensaje del sanitizador: {error!r}"
    assert pipeline.shutdowns == [], "una validación de argparse no debe ni arrancar el pipeline"


def test_short_secret_exits_with_code_2(capsys, pipeline):
    """El sanitizador de la clave HMAC rechaza menos de 16 caracteres antes de tocar la red."""
    assert app_operator.main(["produce-sqs", "--secret", "corto"]) == 2

    error = capsys.readouterr().err

    assert "al menos 16 caracteres" in error, f"falta el mensaje del sanitizador: {error!r}"
    assert pipeline.shutdowns == [], "una validación de argparse no debe ni arrancar el pipeline"


def test_importing_the_cli_writes_no_log_file(tmp_path, monkeypatch):
    """La observabilidad arranca en `main()`, nunca en el import del módulo."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("VULCANO_LOG_FILE", raising=False)

    spec = importlib.util.spec_from_file_location("app_operator_import_aislado", CLI_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    created = sorted(path.name for path in tmp_path.iterdir())

    assert created == [], f"importar la CLI dejó archivos en el directorio de trabajo: {created}"
    assert callable(module.main)


def test_produce_sqs_publishes_and_returns_zero(pipeline, monkeypatch):
    """`produce-sqs` resuelve la URL con `get_queue_url`, publica y registra el MessageId devuelto."""
    client = SQSClientWithQueue()
    published: list[tuple[Any, str, dict[str, Any]]] = []

    def fake_produce(received_client, queue_url, event):
        published.append((received_client, queue_url, event))
        return "msg-publicado-1"

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: client)
    monkeypatch.setattr(app_operator, "produce_event", fake_produce)

    assert app_operator.main(["produce-sqs"]) == 0

    assert client.queue_url_calls == [{"QueueName": "vulcano-events-queue"}], (
        "la URL de la cola se pide al servicio a partir del nombre, no se concatena a mano"
    )
    assert len(published) == 1
    _, queue_url, event = published[0]
    assert queue_url == QUEUE_URL
    assert event, "el payload por defecto debe traer un evento de ejemplo"

    records = [
        record for record in pipeline.records if getattr(record, "message_id", None) == "msg-publicado-1"
    ]
    assert records, "el MessageId devuelto por SQS no quedó registrado"
    assert "msg-publicado-1" in records[0].getMessage()
    assert pipeline.shutdowns == [1], "el finally debe apagar el pipeline también en el camino feliz"


def test_produce_sqs_injects_the_event_topic_into_the_event(pipeline, monkeypatch):
    """`--event-topic` viaja en el cuerpo del evento y en el registro forense."""
    events: list[dict[str, Any]] = []

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    monkeypatch.setattr(app_operator, "produce_event", lambda _c, _u, event: events.append(event) or "msg-tema")

    assert app_operator.main(["produce-sqs", "--event-topic", "pago.aprobado"]) == 0

    assert events == [{**app_operator.SAMPLE_EVENT, "event_topic": "pago.aprobado"}]
    records = [record for record in pipeline.records if getattr(record, "message_id", None) == "msg-tema"]
    assert records and records[0].event_topic == "pago.aprobado"


def test_non_json_payload_exits_with_code_2(capsys, pipeline):
    """El payload inválido es validación de argumentos (código 2), no un error de SQS."""
    assert app_operator.main(["produce-sqs", "--payload", "{no es json}"]) == 2

    assert "Payload inválido" in capsys.readouterr().err
    assert pipeline.shutdowns == []


def test_missing_queue_exits_with_code_4(pipeline, monkeypatch):
    """Que la cola no exista es un error de SQS: exit 4 con su log, no un traceback."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientMissingQueue())

    assert app_operator.main(["produce-sqs"]) == 4

    assert any(record.levelno == logging.ERROR for record in pipeline.records)
    assert pipeline.shutdowns == [1]


def test_start_receiver_runs_uvicorn_with_the_receiver_app(pipeline, monkeypatch):
    """El receptor se levanta por la ruta de importación, para que uvicorn lo construya a demanda."""
    invocations: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(app_operator.uvicorn, "run", lambda *args, **kwargs: invocations.append((args, kwargs)))

    assert app_operator.main(["start-receiver", "--host", "0.0.0.0", "--port", "9999"]) == 0

    (args, kwargs) = invocations[0]
    assert args == ("vulcano_telemetry.webhook_receiver:app",)
    assert kwargs == {"host": "0.0.0.0", "port": 9999}


def test_start_dispatcher_propagates_options_and_honors_max_batches(pipeline, monkeypatch):
    """`--max-lotes` acota el bucle para que la demo sea determinista; el resto viaja al despachador."""
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    _dispatcher_double(monkeypatch, calls=calls)

    exit_code = app_operator.main(
        [
            "start-dispatcher",
            "-q",
            "vulcano-events-queue",
            "-w",
            "5",
            "--url",
            "https://ejemplo.test/webhook",
            "--secret",
            VALID_SECRET,
            "--timeout",
            "2.5",
            "--max-lotes",
            "2",
        ]
    )

    assert exit_code == 0

    invocation = calls[0]
    assert invocation["queue_url"] == QUEUE_URL
    assert invocation["target_url"] == "https://ejemplo.test/webhook"
    assert invocation["secret"] == VALID_SECRET
    assert invocation["wait_time"] == 5
    assert invocation["timeout"] == 2.5
    assert calls[1] == {"iterations": 2}, "el dosificador '--max-lotes' no llegó al bucle del despachador"


@pytest.mark.parametrize(
    ("exception", "exit_code"),
    [
        (SQSConnectionError, 4),
        (WebhookDeliveryError, 5),
        (WebhookSignatureError, 3),
        (WebhookTimestampError, 3),
    ],
    ids=["sqs", "delivery", "signature", "timestamp"],
)
def test_each_exception_group_maps_to_its_exit_code(exception, exit_code, pipeline, monkeypatch):
    """S7/S8: cada grupo de `except*` tiene su log y su código de salida."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    _dispatcher_double(monkeypatch, exception=exception)

    assert app_operator.main(["start-dispatcher"]) == exit_code

    errors = [record for record in pipeline.records if record.levelno == logging.ERROR]
    assert errors, f"el grupo de {exception.__name__} no dejó registro de error"
    assert any(record.exc_info is not None for record in errors), "el registro debe conservar el árbol del grupo"


def test_ctrl_c_interruption_exits_with_130(pipeline, monkeypatch):
    """Un Ctrl-C durante el despachador devuelve 130 sin propagar el traceback."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    _dispatcher_double(monkeypatch, exception=KeyboardInterrupt)

    assert app_operator.main(["start-dispatcher"]) == 130

    assert pipeline.shutdowns == [1], "el finally debe apagar el pipeline incluso con Ctrl-C"


def test_finally_shuts_down_the_logging_pipeline(pipeline, monkeypatch):
    """S8: el `finally` libera el listener aunque el camino termine en una excepción del dominio."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    _dispatcher_double(monkeypatch, exception=WebhookDeliveryError)

    assert app_operator.main(["start-dispatcher"]) == 5

    assert pipeline.shutdowns == [1], "shutdown_vulcano_logging debe llamarse exactamente una vez"


def test_default_log_file_comes_from_the_environment_variable(pipeline, monkeypatch, tmp_path):
    """`--log-file` toma su valor de `VULCANO_LOG_FILE` cuando el operador no lo pasa."""
    monkeypatch.setenv("VULCANO_LOG_FILE", str(tmp_path / "registro.jsonl"))
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: SQSClientWithQueue())
    _dispatcher_double(monkeypatch)

    assert app_operator.main(["start-dispatcher"]) == 0

    assert pipeline.setups[0]["log_file"] == str(tmp_path / "registro.jsonl")
