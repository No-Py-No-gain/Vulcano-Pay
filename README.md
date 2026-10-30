# Vulcano Telemetry — Proyecto Vulcano Pay

Motor de eventos asíncronos para Vulcano Pay: recepción de webhooks HTTP con validación
criptográfica HMAC-SHA256 sobre los bytes crudos, desacople mediante AWS SQS (emulado con
LocalStack en `http://localhost:4566`) vía Boto3, despacho firmado hacia destinos de terceros y
observabilidad con logs JSON no bloqueantes.

Trabajo Práctico N.º 2 — Programación para Automatización II (UPATecO).

## Estructura

```
vulcano_engine/
├── src/
│   ├── vulcano_telemetry/
│   │   ├── __init__.py           # API pública del paquete (__all__)
│   │   ├── exceptions.py         # Excepciones semánticas del dominio
│   │   ├── sanitizer.py          # Validadores declarativos para argparse
│   │   ├── core_sqs.py           # Cliente Boto3 y mensajería SQS
│   │   ├── webhook_crypto.py     # Firma y verificación HMAC-SHA256
│   │   ├── webhook_receiver.py   # Servidor receptor de webhooks (FastAPI)
│   │   ├── logging_engine.py     # Formatter JSON, QueueHandler/Listener y rotación gzip
│   │   └── dispatcher.py         # Despachador de webhooks y worker SQS
│   └── app_operator.py           # Punto de entrada CLI (argparse + except*)
├── tests/                        # pytest: caos, forense y guardián de criterios
├── scripts/                      # Bootstrap de LocalStack
├── docker-compose.yml            # LocalStack
├── requirements.txt              # Dependencias de ejecución (5 paquetes obligatorios)
├── requirements-dev.txt          # Dependencias de desarrollo (pytest, ruff)
└── pyproject.toml                # Empaquetado, pytest y ruff
```

## Puesta en marcha

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

docker compose up -d                      # LocalStack en http://localhost:4566
python scripts/bootstrap_localstack.py    # crea vulcano-events-queue y vulcano-events-dlq
```

## Variables de entorno

Todas opcionales; el código usa estos mismos valores por defecto. Copiá esta tabla a un archivo
`.env` local (ignorado por git) si necesitás sobreescribirlas.

| Variable | Default | Uso |
|---|---|---|
| `VULCANO_ENDPOINT_URL` | `http://localhost:4566` | endpoint de LocalStack |
| `VULCANO_REGION` | `us-east-1` | región simulada de Boto3 |
| `VULCANO_QUEUE_NAME` | `vulcano-events-queue` | cola de eventos |
| `VULCANO_DLQ_NAME` | `vulcano-events-dlq` | Dead Letter Queue |
| `VULCANO_MAX_RECEIVE_COUNT` | `3` | reintentos antes de derivar a la DLQ |
| `VULCANO_VISIBILITY_TIMEOUT` | `30` | segundos de invisibilidad por mensaje |
| `VULCANO_WEBHOOK_SECRET` | — | clave HMAC (mínimo 16 caracteres) |
| `VULCANO_TARGET_URL` | `http://localhost:9000/webhook-destino` | destino del despacho |
| `VULCANO_TOLERANCE_SEC` | `300` | ventana anti-replay del timestamp |
| `VULCANO_LOG_FILE` | `vulcano_events.log` | archivo de log activo |
| `VULCANO_LOG_MAX_BYTES` | `2097152` | tamaño de rotación (2 MB) |
| `VULCANO_LOG_BACKUPS` | `3` | respaldos `.gz` conservados |

## Estado

En construcción. Este README se completa al cierre de la unidad de trabajo del CLI con: diagrama
de arquitectura Mermaid, diagrama de secuencia, matriz de cumplimiento de los criterios de
evaluación, tabla de códigos HTTP y de exit codes, y decisiones de diseño justificadas.
