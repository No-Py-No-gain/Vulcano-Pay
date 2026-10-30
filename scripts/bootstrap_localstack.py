"""Bootstrap idempotente de las colas SQS de Vulcano Pay sobre LocalStack.

Crea la Dead Letter Queue y la cola principal con su `RedrivePolicy` apuntando a la DLQ, o
reconcilia los atributos si ya existen. Se puede ejecutar las veces que haga falta: nunca
duplica colas ni pierde el `maxReceiveCount` configurado.

Se mantiene deliberadamente autocontenido (sin importar `vulcano_telemetry`) para que la
infraestructura se pueda levantar aunque el paquete todavía no compile.

Salidas: 0 si todo quedó configurado, 4 si LocalStack no responde o SQS falla.
"""

from __future__ import annotations

import json
import os
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

ENDPOINT_URL = os.getenv("VULCANO_ENDPOINT_URL", "http://localhost:4566")
REGION = os.getenv("VULCANO_REGION", "us-east-1")
QUEUE_NAME = os.getenv("VULCANO_QUEUE_NAME", "vulcano-events-queue")
DLQ_NAME = os.getenv("VULCANO_DLQ_NAME", "vulcano-events-dlq")
MAX_RECEIVE_COUNT = int(os.getenv("VULCANO_MAX_RECEIVE_COUNT", "3"))
VISIBILITY_TIMEOUT = int(os.getenv("VULCANO_VISIBILITY_TIMEOUT", "30"))
READY_TIMEOUT = float(os.getenv("VULCANO_READY_TIMEOUT", "60"))
RETRY_INTERVAL = 2.0

EXIT_OK = 0
EXIT_SQS = 4


def build_client():
    """Cliente SQS apuntando a LocalStack con credenciales simuladas."""
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def wait_until_ready(client, timeout: float = READY_TIMEOUT) -> None:
    """Espera a que LocalStack acepte llamadas SQS. Falla con TimeoutError si no responde."""
    limite = time.monotonic() + timeout
    ultimo_error: Exception | None = None
    while time.monotonic() < limite:
        try:
            client.list_queues()
            return
        except (BotoCoreError, ClientError) as err:
            ultimo_error = err
            time.sleep(RETRY_INTERVAL)
    raise TimeoutError(f"LocalStack no respondió en {timeout}s: {ultimo_error}")


def ensure_queue(client, name: str, attributes: dict[str, str]) -> str:
    """Devuelve la QueueUrl, creándola si no existe. No falla si ya estaba creada."""
    try:
        return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") != "QueueAlreadyExists":
            raise
        return client.get_queue_url(QueueName=name)["QueueUrl"]


def queue_arn(client, queue_url: str) -> str:
    """ARN de la cola, necesario para la RedrivePolicy."""
    return client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def main() -> int:
    client = build_client()

    try:
        wait_until_ready(client)

        attrs_comunes = {"VisibilityTimeout": str(VISIBILITY_TIMEOUT)}
        dlq_url = ensure_queue(client, DLQ_NAME, attrs_comunes)
        dlq_arn = queue_arn(client, dlq_url)

        redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(MAX_RECEIVE_COUNT)})
        attrs_principales = {**attrs_comunes, "RedrivePolicy": redrive}
        queue_url = ensure_queue(client, QUEUE_NAME, attrs_principales)

        # Reconciliación explícita: si las colas ya existían con otra configuración, se corrige.
        client.set_queue_attributes(QueueUrl=dlq_url, Attributes=attrs_comunes)
        client.set_queue_attributes(QueueUrl=queue_url, Attributes=attrs_principales)

        print(
            json.dumps(
                {
                    "endpoint": ENDPOINT_URL,
                    "cola": {"nombre": QUEUE_NAME, "url": queue_url},
                    "dlq": {"nombre": DLQ_NAME, "url": dlq_url, "arn": dlq_arn},
                    "max_receive_count": MAX_RECEIVE_COUNT,
                    "visibility_timeout": VISIBILITY_TIMEOUT,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    except TimeoutError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return EXIT_SQS
    except (BotoCoreError, ClientError) as err:
        print(f"ERROR: fallo de SQS contra LocalStack: {err}", file=sys.stderr)
        return EXIT_SQS

    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
