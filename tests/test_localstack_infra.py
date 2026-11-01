"""Prueba de la infraestructura SQS de LocalStack: cola principal con RedrivePolicy hacia la DLQ.

Es una prueba de integración: necesita LocalStack corriendo en http://localhost:4566 y se
activa con `VULCANO_IT=1`. Sin esa variable se saltea, para que la suite unitaria no dependa
de Docker.

Lo que se verifica, por regla del enunciado:
- la DLQ existe y es el destino del `RedrivePolicy` (S9);
- el `maxReceiveCount` configurado es el que decide cuándo un mensaje venenoso cae en la DLQ (S9);
- el bootstrap es idempotente: correrlo dos veces deja el mismo estado y el mismo ARN (S13).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

RAIZ = Path(__file__).resolve().parents[1]
BOOTSTRAP = RAIZ / "scripts" / "bootstrap_localstack.py"
ENDPOINT_URL = os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
QUEUE_NAME = os.getenv("VULCANO_QUEUE_NAME", "vulcano-events-queue")
DLQ_NAME = os.getenv("VULCANO_DLQ_NAME", "vulcano-events-dlq")
MAX_RECEIVE_COUNT = os.getenv("VULCANO_MAX_RECEIVE_COUNT", "3")

def _cliente():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name=os.getenv("VULCANO_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _correr_bootstrap() -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(BOOTSTRAP)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "VULCANO_READY_TIMEOUT": "30"},
    )


def _atributos(cliente, nombre: str) -> tuple[str, dict[str, str]]:
    url = cliente.get_queue_url(QueueName=nombre)["QueueUrl"]
    attrs = cliente.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    return url, attrs


@pytest.fixture(scope="module")
def bootstrapeado():
    primera = _correr_bootstrap()
    assert primera.returncode == 0, f"bootstrap falló: {primera.stderr}"
    yield primera
    # Segunda corrida: debe ser idempotente y no ensuciar el estado.
    segunda = _correr_bootstrap()
    assert segunda.returncode == 0, f"bootstrap no es idempotente: {segunda.stderr}"


@pytest.mark.integration
def test_la_dlq_y_la_cola_principal_existen(bootstrapeado):
    cliente = _cliente()

    _, attrs_dlq = _atributos(cliente, DLQ_NAME)
    _, attrs_cola = _atributos(cliente, QUEUE_NAME)

    assert attrs_dlq["QueueArn"] != attrs_cola["QueueArn"]


@pytest.mark.integration
def test_la_cola_principal_deriva_a_la_dlq_con_el_max_receive_count_configurado(bootstrapeado):
    cliente = _cliente()

    _, attrs_dlq = _atributos(cliente, DLQ_NAME)
    _, attrs_cola = _atributos(cliente, QUEUE_NAME)

    assert "RedrivePolicy" in attrs_cola, (
        "la cola principal no tiene RedrivePolicy: los mensajes venenosos nunca llegan a la DLQ"
    )
    redrive = json.loads(attrs_cola["RedrivePolicy"])
    assert redrive["deadLetterTargetArn"] == attrs_dlq["QueueArn"]
    assert str(redrive["maxReceiveCount"]) == str(MAX_RECEIVE_COUNT)


@pytest.mark.integration
def test_el_bootstrap_es_idempotente(bootstrapeado):
    cliente = _cliente()

    _, antes_cola = _atributos(cliente, QUEUE_NAME)
    _, antes_dlq = _atributos(cliente, DLQ_NAME)
    segunda = _correr_bootstrap()

    assert segunda.returncode == 0, segunda.stderr
    _, despues_cola = _atributos(cliente, QUEUE_NAME)
    _, despues_dlq = _atributos(cliente, DLQ_NAME)

    assert (antes_cola["QueueArn"], antes_dlq["QueueArn"]) == (despues_cola["QueueArn"], despues_dlq["QueueArn"])
    assert json.loads(despues_cola["RedrivePolicy"])["maxReceiveCount"] == str(MAX_RECEIVE_COUNT)


@pytest.mark.integration
def test_la_dlq_no_tiene_redrive_propio(bootstrapeado):
    """Una DLQ que deriva a otra cola indica una configuración circular: no debe existir."""
    cliente = _cliente()

    _, attrs_dlq = _atributos(cliente, DLQ_NAME)

    assert "RedrivePolicy" not in attrs_dlq


def test_sin_localstack_el_bootstrap_falla_con_codigo_4(tmp_path):
    """Control negativo: con el endpoint caído el script informa el fallo, no se cuelga ni miente."""
    resultado = subprocess.run(
        [sys.executable, str(BOOTSTRAP)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            **os.environ,
            "VULCANO_ENDPOINT_URL": "http://127.0.0.1:4567",
            "VULCANO_READY_TIMEOUT": "3",
        },
    )

    assert resultado.returncode == 4
    assert "ERROR" in resultado.stderr


def test_el_cliente_sqs_reporta_error_de_conexion_sin_endpoint_levantado():
    """Boto3 contra un puerto cerrado levanta ClientError/BotoCoreError, no una excepción genérica."""
    from botocore.exceptions import BotoCoreError

    cliente = boto3.client(
        "sqs",
        endpoint_url="http://127.0.0.1:4567",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 1}, connect_timeout=1, read_timeout=1),
    )

    with pytest.raises((ClientError, BotoCoreError)):
        cliente.list_queues()
