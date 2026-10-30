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

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "scripts" / "bootstrap_localstack.py"
ENDPOINT_URL = os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
QUEUE_NAME = os.getenv("VULCANO_QUEUE_NAME", "vulcano-events-queue")
DLQ_NAME = os.getenv("VULCANO_DLQ_NAME", "vulcano-events-dlq")
MAX_RECEIVE_COUNT = os.getenv("VULCANO_MAX_RECEIVE_COUNT", "3")

def _client():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name=os.getenv("VULCANO_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _run_bootstrap() -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(BOOTSTRAP)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "VULCANO_READY_TIMEOUT": "30"},
    )


def _queue_attributes(client, name: str) -> tuple[str, dict[str, str]]:
    url = client.get_queue_url(QueueName=name)["QueueUrl"]
    attributes = client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    return url, attributes


@pytest.fixture(scope="module")
def bootstrapped():
    first = _run_bootstrap()
    assert first.returncode == 0, f"bootstrap falló: {first.stderr}"
    yield first
    # Segunda corrida: debe ser idempotente y no ensuciar el estado.
    second = _run_bootstrap()
    assert second.returncode == 0, f"bootstrap no es idempotente: {second.stderr}"


@pytest.mark.integration
def test_the_dlq_and_the_main_queue_exist(bootstrapped):
    client = _client()

    _, dlq_attributes = _queue_attributes(client, DLQ_NAME)
    _, queue_attributes = _queue_attributes(client, QUEUE_NAME)

    assert dlq_attributes["QueueArn"] != queue_attributes["QueueArn"]


@pytest.mark.integration
def test_main_queue_redrives_to_the_dlq_with_the_configured_max_receive_count(bootstrapped):
    client = _client()

    _, dlq_attributes = _queue_attributes(client, DLQ_NAME)
    _, queue_attributes = _queue_attributes(client, QUEUE_NAME)

    assert "RedrivePolicy" in queue_attributes, (
        "la cola principal no tiene RedrivePolicy: los mensajes venenosos nunca llegan a la DLQ"
    )
    redrive = json.loads(queue_attributes["RedrivePolicy"])
    assert redrive["deadLetterTargetArn"] == dlq_attributes["QueueArn"]
    assert str(redrive["maxReceiveCount"]) == str(MAX_RECEIVE_COUNT)


@pytest.mark.integration
def test_bootstrap_is_idempotent(bootstrapped):
    client = _client()

    _, queue_before = _queue_attributes(client, QUEUE_NAME)
    _, dlq_before = _queue_attributes(client, DLQ_NAME)
    second = _run_bootstrap()

    assert second.returncode == 0, second.stderr
    _, queue_after = _queue_attributes(client, QUEUE_NAME)
    _, dlq_after = _queue_attributes(client, DLQ_NAME)

    assert (queue_before["QueueArn"], dlq_before["QueueArn"]) == (queue_after["QueueArn"], dlq_after["QueueArn"])
    assert json.loads(queue_after["RedrivePolicy"])["maxReceiveCount"] == str(MAX_RECEIVE_COUNT)


@pytest.mark.integration
def test_the_dlq_has_no_redrive_of_its_own(bootstrapped):
    """Una DLQ que deriva a otra cola indica una configuración circular: no debe existir."""
    client = _client()

    _, dlq_attributes = _queue_attributes(client, DLQ_NAME)

    assert "RedrivePolicy" not in dlq_attributes


def test_bootstrap_fails_with_code_4_without_localstack(tmp_path):
    """Control negativo: con el endpoint caído el script informa el fallo, no se cuelga ni miente."""
    result = subprocess.run(
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

    assert result.returncode == 4
    assert "ERROR" in result.stderr


def test_sqs_client_reports_connection_error_without_endpoint():
    """Boto3 contra un puerto cerrado levanta ClientError/BotoCoreError, no una excepción genérica."""
    from botocore.exceptions import BotoCoreError

    client = boto3.client(
        "sqs",
        endpoint_url="http://127.0.0.1:4567",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 1}, connect_timeout=1, read_timeout=1),
    )

    with pytest.raises((ClientError, BotoCoreError)):
        client.list_queues()
