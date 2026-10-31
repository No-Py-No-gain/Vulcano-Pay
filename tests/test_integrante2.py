
"""
Proyecto Vulcano Pay
Pruebas automatizadas - Integrante 2.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src"),
)

from vulcano_telemetry.core_sqs import (
    send_event,
    receive_events,
    delete_message,
)

from vulcano_telemetry.exceptions import (
    CorruptedMessageError,
    SQSConnectionError,
)


QUEUE_URL = "http://localhost:4566/000000000000/vulcano-events-queue"


def test_send_event_success():
    client = MagicMock()
    client.send_message.return_value = {
        "MessageId": "msg-001"
    }

    result = send_event(
        sqs_client=client,
        queue_url=QUEUE_URL,
        event_data={"payment_id": "PAY-001"},
        trace_id="TRACE-001",
        event_topic="payment.completed",
    )

    assert result == "msg-001"

    arguments = client.send_message.call_args.kwargs

    assert arguments["MessageAttributes"]["TraceId"][
        "StringValue"
    ] == "TRACE-001"

    assert arguments["MessageAttributes"]["EventTopic"][
        "StringValue"
    ] == "payment.completed"

    assert arguments["MessageAttributes"]["Timestamp"][
        "StringValue"
    ]


def test_send_event_invalid_payload():
    client = MagicMock()

    with pytest.raises(ValueError):
        send_event(
            sqs_client=client,
            queue_url=QUEUE_URL,
            event_data="no-es-diccionario",
        )

    client.send_message.assert_not_called()


def test_receive_events_success():
    client = MagicMock()

    client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg-001",
                "ReceiptHandle": "receipt-001",
                "Body": '{"payment_id": "PAY-001"}',
                "MessageAttributes": {
                    "TraceId": {
                        "StringValue": "TRACE-001",
                        "DataType": "String",
                    }
                },
                "Attributes": {
                    "ApproximateReceiveCount": "1"
                },
            }
        ]
    }

    events = receive_events(
        sqs_client=client,
        queue_url=QUEUE_URL,
        wait_time=5,
    )

    assert len(events) == 1
    assert events[0]["message_id"] == "msg-001"
    assert events[0]["body"]["payment_id"] == "PAY-001"
    assert events[0]["receipt_handle"] == "receipt-001"

    client.delete_message.assert_not_called()


def test_receive_events_empty_queue():
    client = MagicMock()
    client.receive_message.return_value = {}

    events = receive_events(
        sqs_client=client,
        queue_url=QUEUE_URL,
        wait_time=1,
    )

    assert events == []


def test_receive_events_corrupted_json():
    client = MagicMock()

    client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg-corrupt",
                "ReceiptHandle": "receipt-corrupt",
                "Body": "{json_invalido",
            }
        ]
    }

    with pytest.raises(CorruptedMessageError) as exc_info:
        receive_events(
            sqs_client=client,
            queue_url=QUEUE_URL,
        )

    assert exc_info.value.__cause__ is not None
    client.delete_message.assert_not_called()


@pytest.mark.parametrize("wait_time", [0, 21])
def test_receive_events_invalid_wait_time(wait_time):
    client = MagicMock()

    with pytest.raises(ValueError):
        receive_events(
            sqs_client=client,
            queue_url=QUEUE_URL,
            wait_time=wait_time,
        )


def test_delete_message_success():
    client = MagicMock()

    delete_message(
        sqs_client=client,
        queue_url=QUEUE_URL,
        receipt_handle="receipt-001",
    )

    client.delete_message.assert_called_once_with(
        QueueUrl=QUEUE_URL,
        ReceiptHandle="receipt-001",
    )


def test_send_event_connection_error():
    client = MagicMock()

    original_error = ClientError(
        {
            "Error": {
                "Code": "ServiceUnavailable",
                "Message": "SQS no disponible",
            }
        },
        "SendMessage",
    )

    client.send_message.side_effect = original_error

    with pytest.raises(SQSConnectionError) as exc_info:
        send_event(
            sqs_client=client,
            queue_url=QUEUE_URL,
            event_data={"payment_id": "PAY-001"},
        )

    assert exc_info.value.__cause__ is original_error
    assert len(exc_info.value.__notes__) >= 1


def test_delete_message_connection_error():
    client = MagicMock()

    original_error = ClientError(
        {
            "Error": {
                "Code": "ServiceUnavailable",
                "Message": "SQS no disponible",
            }
        },
        "DeleteMessage",
    )

    client.delete_message.side_effect = original_error

    with pytest.raises(SQSConnectionError) as exc_info:
        delete_message(
            sqs_client=client,
            queue_url=QUEUE_URL,
            receipt_handle="receipt-001",
        )

    assert exc_info.value.__cause__ is original_error
def test_receive_events_mixed_batch():
    """
    Comprueba el comportamiento cuando SQS entrega
    un mensaje valido seguido de uno corrupto.
    """
    client = MagicMock()

    client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg-valid",
                "ReceiptHandle": "receipt-valid",
                "Body": '{"payment_id": "PAY-001"}',
            },
            {
                "MessageId": "msg-corrupt",
                "ReceiptHandle": "receipt-corrupt",
                "Body": "{json_invalido",
            },
        ]
    }

    with pytest.raises(CorruptedMessageError) as exc_info:
        receive_events(
            sqs_client=client,
            queue_url=QUEUE_URL,
            max_messages=2,
        )

    assert "msg-corrupt" in str(exc_info.value)
    client.delete_message.assert_not_called()
def test_receive_events_safe_mixed_batch():
    """
    Verifica que los mensajes validos se conserven
    aunque el lote contenga un mensaje corrupto.
    """
    from vulcano_telemetry.core_sqs import receive_events_safe

    client = MagicMock()

    client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg-valid-001",
                "ReceiptHandle": "receipt-valid-001",
                "Body": '{"payment_id": "PAY-001"}',
            },
            {
                "MessageId": "msg-corrupt-001",
                "ReceiptHandle": "receipt-corrupt-001",
                "Body": "{json_invalido",
            },
            {
                "MessageId": "msg-valid-002",
                "ReceiptHandle": "receipt-valid-002",
                "Body": '{"payment_id": "PAY-002"}',
            },
        ]
    }

    result = receive_events_safe(
        sqs_client=client,
        queue_url=QUEUE_URL,
        wait_time=5,
        max_messages=3,
    )

    # Debe conservar los dos mensajes validos.
    assert len(result["valid"]) == 2

    assert result["valid"][0]["body"]["payment_id"] == "PAY-001"
    assert result["valid"][1]["body"]["payment_id"] == "PAY-002"

    # Debe identificar el mensaje corrupto.
    assert len(result["corrupted"]) == 1
    assert result["corrupted"][0]["message_id"] == "msg-corrupt-001"

    # La excepcion debe conservar su causa original.
    error = result["corrupted"][0]["error"]

    assert isinstance(error, CorruptedMessageError)
    assert error.__cause__ is not None

    # Ningun mensaje debe eliminarse automaticamente.
    client.delete_message.assert_not_called()
def test_send_event_non_serializable_payload():
    """
    Verifica que un evento no serializable
    no se publique en SQS.
    """
    client = MagicMock()

    event_data = {
        "payment_id": "PAY-003",
        "tags": {"urgente", "revision"},
    }

    with pytest.raises(TypeError):
        send_event(
            sqs_client=client,
            queue_url=QUEUE_URL,
            event_data=event_data,
        )

    client.send_message.assert_not_called()

def test_receive_events_safe_json_list():
    """
    Verifica el comportamiento actual cuando SQS
    recibe JSON valido que contiene una lista.
    """
    from vulcano_telemetry.core_sqs import receive_events_safe

    client = MagicMock()

    client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg-list-001",
                "ReceiptHandle": "receipt-list-001",
                "Body": '["PAY-001", "PAY-002"]',
            }
        ]
    }

    result = receive_events_safe(
        sqs_client=client,
        queue_url=QUEUE_URL,
        max_messages=1,
    )

    assert len(result["valid"]) == 1
    assert result["valid"][0]["body"] == [
        "PAY-001",
        "PAY-002",
    ]

    assert len(result["corrupted"]) == 0
    client.delete_message.assert_not_called()
