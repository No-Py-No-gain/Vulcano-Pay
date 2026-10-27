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
from vulcano_telemetry.logging_engine import NOMBRE_LOGGER

RAIZ = Path(__file__).resolve().parents[1]
RUTA_CLI = RAIZ / "src" / "app_operator.py"
URL_COLA = "http://localhost:4566/000000000000/vulcano-events-queue"
SECRETO_VALIDO = "clave-secreta-de-prueba-suficientemente-larga"
SUBCOMANDOS = ("produce-sqs", "start-receiver", "start-dispatcher")


class ColectorDeRegistros(logging.Handler):
    """Handler en memoria: deja ver los registros emitidos sin tocar el disco."""

    def __init__(self) -> None:
        super().__init__()
        self.registros: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.registros.append(record)


@dataclass
class PipelineObservado:
    """Lo que el test puede observar del pipeline de logging reemplazado."""

    registros: list[logging.LogRecord]
    configuraciones: list[dict[str, Any]]
    apagados: list[int]


class ClienteSQSConCola:
    """Doble de SQS: resuelve el nombre de la cola a una URL sin hablar con AWS."""

    def __init__(self, url: str = URL_COLA) -> None:
        self.url = url
        self.pedidos: list[dict[str, Any]] = []

    def get_queue_url(self, **argumentos: Any) -> dict[str, Any]:
        self.pedidos.append(argumentos)
        return {"QueueUrl": self.url}


class ClienteSQSColaPerdida:
    """Doble de SQS para la cola que no existe: Boto3 levanta `ClientError`."""

    def get_queue_url(self, **_argumentos: Any) -> dict[str, Any]:
        raise ClientError(
            {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "does not exist"}},
            "GetQueueUrl",
        )


def _despachador_doble(monkeypatch, excepcion: type[BaseException] | None = None, llamadas: list[Any] | None = None):
    """Reemplaza `DespachadorSQS` por un doble que registra su invocación y/o falla al ejecutar."""

    class DespachadorDoble:
        def __init__(self, cliente, queue_url, target_url, secret, logger=None, **opciones: Any) -> None:
            self.invocacion = {
                "cliente": cliente,
                "queue_url": queue_url,
                "target_url": target_url,
                "secret": secret,
                "logger": logger,
                **opciones,
            }
            if llamadas is not None:
                llamadas.append(self.invocacion)

        def ejecutar(self, iteraciones: int | None = None) -> int:
            if llamadas is not None:
                llamadas.append({"iteraciones": iteraciones})
            if excepcion is not None:
                raise excepcion("fallo simulado dentro del despachador")
            return 0

    monkeypatch.setattr(app_operator, "DespachadorSQS", DespachadorDoble)


@pytest.fixture
def pipeline(monkeypatch):
    """Pipeline de logging reemplazado: nada escribe archivos y todo registro queda observable."""
    logger = logging.getLogger(NOMBRE_LOGGER)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()
    colector = ColectorDeRegistros()
    logger.addHandler(colector)

    configuraciones: list[dict[str, Any]] = []
    apagados: list[int] = []

    def setup_falso(log_file="vulcano_events.log", **_opciones: Any):
        configuraciones.append({"log_file": str(log_file)})
        return object()

    def shutdown_falso() -> None:
        apagados.append(1)

    monkeypatch.setattr(app_operator, "setup_vulcano_logging", setup_falso)
    monkeypatch.setattr(app_operator, "shutdown_vulcano_logging", shutdown_falso)

    yield PipelineObservado(registros=colector.registros, configuraciones=configuraciones, apagados=apagados)

    logger.removeHandler(colector)
    logger.handlers.clear()


def test_el_help_lista_los_tres_subcomandos():
    """S7: la ayuda del parser publica los tres subcomandos acordados."""
    ayuda = app_operator.build_parser().format_help()

    for subcomando in SUBCOMANDOS:
        assert subcomando in ayuda, f"la ayuda no documenta el subcomando {subcomando}"
    assert "130" in ayuda, "la ayuda debe publicar la tabla de códigos de salida"


def test_sin_subcomando_termina_con_codigo_2(capsys):
    """Convención de `main()`: devuelve 2 en vez de dejar escapar el `SystemExit` de argparse."""
    assert app_operator.main(argv=[]) == 2

    error = capsys.readouterr().err

    assert "obligatorio" in error.lower() or "required" in error.lower(), (
        f"el error no explica que falta el subcomando: {error!r}"
    )


def test_un_nombre_de_cola_invalido_termina_con_codigo_2(capsys, pipeline):
    """El sanitizador del rol 1 se integra como `type` de argparse y su mensaje llega al usuario."""
    assert app_operator.main(["produce-sqs", "-q", "cola-invalida"]) == 2

    error = capsys.readouterr().err

    assert "Nombre de cola inválido 'cola-invalida'" in error, f"falta el mensaje del sanitizador: {error!r}"
    assert pipeline.apagados == [], "una validación de argparse no debe ni arrancar el pipeline"


def test_un_secreto_corto_termina_con_codigo_2(capsys, pipeline):
    """El sanitizador de la clave HMAC rechaza menos de 16 caracteres antes de tocar la red."""
    assert app_operator.main(["produce-sqs", "--secret", "corto"]) == 2

    error = capsys.readouterr().err

    assert "al menos 16 caracteres" in error, f"falta el mensaje del sanitizador: {error!r}"
    assert pipeline.apagados == [], "una validación de argparse no debe ni arrancar el pipeline"


def test_importar_la_cli_no_escribe_ningun_archivo_de_log(tmp_path, monkeypatch):
    """La observabilidad arranca en `main()`, nunca en el import del módulo."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("VULCANO_LOG_FILE", raising=False)

    especificacion = importlib.util.spec_from_file_location("app_operator_import_aislado", RUTA_CLI)
    modulo = importlib.util.module_from_spec(especificacion)
    especificacion.loader.exec_module(modulo)

    creados = sorted(ruta.name for ruta in tmp_path.iterdir())

    assert creados == [], f"importar la CLI dejó archivos en el directorio de trabajo: {creados}"
    assert callable(modulo.main)


def test_produce_sqs_publica_y_devuelve_cero(pipeline, monkeypatch):
    """`produce-sqs` resuelve la URL con `get_queue_url`, publica y registra el MessageId devuelto."""
    cliente = ClienteSQSConCola()
    publicaciones: list[tuple[Any, str, dict[str, Any]]] = []

    def produce_falso(cliente_recibido, url_cola, evento):
        publicaciones.append((cliente_recibido, url_cola, evento))
        return "msg-publicado-1"

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: cliente)
    monkeypatch.setattr(app_operator, "produce_event", produce_falso)

    assert app_operator.main(["produce-sqs"]) == 0

    assert cliente.pedidos == [{"QueueName": "vulcano-events-queue"}], (
        "la URL de la cola se pide al servicio a partir del nombre, no se concatena a mano"
    )
    assert len(publicaciones) == 1
    _, url_cola, evento = publicaciones[0]
    assert url_cola == URL_COLA
    assert evento, "el payload por defecto debe traer un evento de ejemplo"

    registros = [
        registro for registro in pipeline.registros if getattr(registro, "message_id", None) == "msg-publicado-1"
    ]
    assert registros, "el MessageId devuelto por SQS no quedó registrado"
    assert "msg-publicado-1" in registros[0].getMessage()
    assert pipeline.apagados == [1], "el finally debe apagar el pipeline también en el camino feliz"


def test_produce_sqs_inyecta_el_event_topic_en_el_evento(pipeline, monkeypatch):
    """`--event-topic` viaja en el cuerpo del evento y en el registro forense."""
    eventos: list[dict[str, Any]] = []

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    monkeypatch.setattr(app_operator, "produce_event", lambda _c, _u, evento: eventos.append(evento) or "msg-tema")

    assert app_operator.main(["produce-sqs", "--event-topic", "pago.aprobado"]) == 0

    assert eventos == [{**app_operator.EVENTO_DE_EJEMPLO, "event_topic": "pago.aprobado"}]
    registros = [registro for registro in pipeline.registros if getattr(registro, "message_id", None) == "msg-tema"]
    assert registros and registros[0].event_topic == "pago.aprobado"


def test_un_payload_que_no_es_json_termina_con_codigo_2(capsys, pipeline):
    """El payload inválido es validación de argumentos (código 2), no un error de SQS."""
    assert app_operator.main(["produce-sqs", "--payload", "{no es json}"]) == 2

    assert "Payload inválido" in capsys.readouterr().err
    assert pipeline.apagados == []


def test_una_cola_inexistente_termina_con_codigo_4(pipeline, monkeypatch):
    """Que la cola no exista es un error de SQS: exit 4 con su log, no un traceback."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSColaPerdida())

    assert app_operator.main(["produce-sqs"]) == 4

    assert any(registro.levelno == logging.ERROR for registro in pipeline.registros)
    assert pipeline.apagados == [1]


def test_start_receiver_levanta_uvicorn_con_el_app_del_receptor(pipeline, monkeypatch):
    """El receptor se levanta por la ruta de importación, para que uvicorn lo construya a demanda."""
    invocaciones: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(app_operator.uvicorn, "run", lambda *args, **kwargs: invocaciones.append((args, kwargs)))

    assert app_operator.main(["start-receiver", "--host", "0.0.0.0", "--port", "9999"]) == 0

    (args, kwargs) = invocaciones[0]
    assert args == ("vulcano_telemetry.webhook_receiver:app",)
    assert kwargs == {"host": "0.0.0.0", "port": 9999}


def test_start_dispatcher_propaga_las_opciones_y_respeta_max_lotes(pipeline, monkeypatch):
    """`--max-lotes` acota el bucle para que la demo sea determinista; el resto viaja al despachador."""
    llamadas: list[dict[str, Any]] = []

    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    _despachador_doble(monkeypatch, llamadas=llamadas)

    codigo = app_operator.main(
        [
            "start-dispatcher",
            "-q",
            "vulcano-events-queue",
            "-w",
            "5",
            "--url",
            "https://ejemplo.test/webhook",
            "--secret",
            SECRETO_VALIDO,
            "--timeout",
            "2.5",
            "--max-lotes",
            "2",
        ]
    )

    assert codigo == 0

    invocacion = llamadas[0]
    assert invocacion["queue_url"] == URL_COLA
    assert invocacion["target_url"] == "https://ejemplo.test/webhook"
    assert invocacion["secret"] == SECRETO_VALIDO
    assert invocacion["wait_time"] == 5
    assert invocacion["timeout"] == 2.5
    assert llamadas[1] == {"iteraciones": 2}, "el dosificador '--max-lotes' no llegó al bucle del despachador"


@pytest.mark.parametrize(
    ("excepcion", "codigo"),
    [
        (SQSConnectionError, 4),
        (WebhookDeliveryError, 5),
        (WebhookSignatureError, 3),
        (WebhookTimestampError, 3),
    ],
    ids=["sqs", "entrega", "firma", "timestamp"],
)
def test_cada_grupo_de_excepciones_mapea_su_codigo_de_salida(excepcion, codigo, pipeline, monkeypatch):
    """S7/S8: cada grupo de `except*` tiene su log y su código de salida."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    _despachador_doble(monkeypatch, excepcion=excepcion)

    assert app_operator.main(["start-dispatcher"]) == codigo

    errores = [registro for registro in pipeline.registros if registro.levelno == logging.ERROR]
    assert errores, f"el grupo de {excepcion.__name__} no dejó registro de error"
    assert any(registro.exc_info is not None for registro in errores), "el registro debe conservar el árbol del grupo"


def test_la_interrupcion_por_ctrl_c_termina_en_130(pipeline, monkeypatch):
    """Un Ctrl-C durante el despachador devuelve 130 sin propagar el traceback."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    _despachador_doble(monkeypatch, excepcion=KeyboardInterrupt)

    assert app_operator.main(["start-dispatcher"]) == 130

    assert pipeline.apagados == [1], "el finally debe apagar el pipeline incluso con Ctrl-C"


def test_el_finally_apaga_el_pipeline_de_logging(pipeline, monkeypatch):
    """S8: el `finally` libera el listener aunque el camino termine en una excepción del dominio."""
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    _despachador_doble(monkeypatch, excepcion=WebhookDeliveryError)

    assert app_operator.main(["start-dispatcher"]) == 5

    assert pipeline.apagados == [1], "shutdown_vulcano_logging debe llamarse exactamente una vez"


def test_el_log_file_por_defecto_sale_de_la_variable_de_entorno(pipeline, monkeypatch, tmp_path):
    """`--log-file` toma su valor de `VULCANO_LOG_FILE` cuando el operador no lo pasa."""
    monkeypatch.setenv("VULCANO_LOG_FILE", str(tmp_path / "registro.jsonl"))
    monkeypatch.setattr(app_operator, "get_sqs_client", lambda: ClienteSQSConCola())
    _despachador_doble(monkeypatch)

    assert app_operator.main(["start-dispatcher"]) == 0

    assert pipeline.configuraciones[0]["log_file"] == str(tmp_path / "registro.jsonl")
