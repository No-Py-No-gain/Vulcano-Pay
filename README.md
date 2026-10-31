Vulcano Pay =)

**Motor de eventos con Amazon SQS, LocalStack y webhooks seguros mediante HMAC-SHA256.**

1. Descripción del proyecto

Vulcano Pay es un proyecto desarrollado en Python que implementa un sistema de recepción, validación y publicación de eventos asociados a transacciones.

Utiliza FastAPI para recibir solicitudes HTTP, HMAC-SHA256 para verificar la autenticidad e integridad de los mensajes y Amazon SQS, emulado mediante LocalStack, para almacenar los eventos.

El proyecto incorpora validación de parámetros, excepciones personalizadas, trazabilidad de mensajes y pruebas automatizadas.

2. Arquitectura

Flujo de publicación implementado:

```text
CLI (app_operator.py)
        |
        | HTTP POST /webhook
        | X-Webhook-Signature
        v
FastAPI (webhook_receiver.py)
        |
        | Verificación HMAC y timestamp
        | Validación JSON
        v
Motor SQS (core_sqs.py)
        |
        | Publicación con atributos
        v
LocalStack / Amazon SQS
        |
        +-- vulcano-events-queue
                  |
                  | Redrive Policy
                  v
            vulcano-events-dlq
```

El receptor devuelve HTTP `202 Accepted` cuando el evento se publica correctamente en SQS.

3. Estructura del proyecto

```text
Vulcano-Pay/
├── README.md
├── requirements.txt
├── src/
│   ├── app_operator.py
│   └── vulcano_telemetry/
│       ├── __init__.py
│       ├── core_sqs.py
│       ├── exceptions.py
│       ├── logging_engine.py
│       ├── sanitizer.py
│       ├── webhook_crypto.py
│       └── webhook_receiver.py
├── test/
│   ├── test_chaos.py
│   └── test_forensic_log.py
└── tests/
    ├── test_integrante1.py
    ├── test_integrante2.py
    └── test_integrante3.py
```

Esta estructura corresponde a la rama `feat/integrantes-1-2-3`.

4. Integrante 1 — Excepciones y sanitización

Archivos principales:

- `exceptions.py`
- `sanitizer.py`

Responsabilidades:

- Definir excepciones personalizadas.
- Validar nombres de colas SQS.
- Validar tiempos de Long Polling.
- Validar direcciones webhook.
- Validar claves HMAC.
- Proporcionar errores identificables para facilitar el diagnóstico.

5. Integrante 2 — Motor SQS

Archivo principal: `core_sqs.py`.

Implementa la comunicación con Amazon SQS utilizando Boto3 y LocalStack.

Funciones principales:

- Obtener el cliente SQS.
- Publicar eventos.
- Recibir mensajes mediante Long Polling.
- Eliminar mensajes explícitamente.
- Gestionar errores de conexión y mensajes corruptos.
- Mantener compatibilidad con las interfaces del proyecto.

Los eventos publicados mediante `send_event()` incluyen:

| Atributo | Descripción |
|---|---|
| `TraceId` | Identificador de trazabilidad |
| `EventTopic` | Tipo de evento |
| `Timestamp` | Fecha y hora del evento |

La infraestructura utiliza una cola principal y una Dead Letter Queue (DLQ), con un máximo configurado de tres recepciones antes de la derivación de mensajes que no se eliminan correctamente.

6. Integrante 3 — Seguridad HMAC y receptor webhook

Archivos principales:

- `webhook_crypto.py`
- `webhook_receiver.py`

Firma HMAC-SHA256

El sistema genera una firma criptográfica sobre el timestamp y los bytes originales del mensaje.

Formato del encabezado:

`X-Webhook-Signature: t=<timestamp>,v1=<firma>`

La verificación utiliza comparación segura y rechaza mensajes con firmas incorrectas o timestamps fuera de la tolerancia de 300 segundos.

Receptor FastAPI

Endpoint:

`POST /webhook`

Secuencia de procesamiento:

1. Recibir los bytes originales del cuerpo HTTP.
2. Validar el timestamp y la firma HMAC.
3. Interpretar y validar el contenido JSON.
4. Publicar el evento en SQS.
5. Responder HTTP `202 Accepted` tras la publicación exitosa.

La clave HMAC del receptor se configura mediante la variable de entorno `VULCANO_WEBHOOK_SECRET`.

7. Requisitos

- Python (versión utilizada en las pruebas: 3.14.0).
- Docker con LocalStack.
- Dependencias de `requirements.txt`.

8. Instalación en Windows PowerShell

Clonar el repositorio:

```powershell
git clone https://github.com/No-Py-No-gain/Vulcano-Pay.git
cd Vulcano-Pay
```

Para trabajar con los cambios de los Integrantes 1, 2 y 3 mientras el Pull Request #9 esté pendiente:

```powershell
git fetch origin
git switch feat/integrantes-1-2-3
```

Crear y activar el entorno virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Instalar dependencias:

```powershell
python -m pip install -r requirements.txt
```

9. Infraestructura LocalStack

Configuración utilizada en las pruebas:

| Recurso | Valor |
|---|---|
| Endpoint | `http://localhost:4566` |
| Región | `us-east-1` |
| Cola principal | `vulcano-events-queue` |
| DLQ | `vulcano-events-dlq` |
| `maxReceiveCount` | `3` |
| Visibility Timeout | `30` segundos |

La rama `chore/localstack-infra` contiene:

- `docker-compose.yml`: definición del contenedor LocalStack.
- `scripts/bootstrap_localstack.py`: creación y reconciliación de colas y atributos SQS.
- Pruebas específicas de infraestructura.

Estos archivos todavía no forman parte de `feat/integrantes-1-2-3`.

Una vez integrados, la infraestructura podrá iniciarse con:

```powershell
docker compose up -d
python .\scripts\bootstrap_localstack.py
```

Estos comandos requieren disponer de los archivos de infraestructura en la rama de trabajo. No deben ejecutarse desde una rama donde aún no existan.

El script de inicialización crea o reutiliza las colas y configura la Redrive Policy.

10. Configuración HMAC

Antes de iniciar el receptor, configurar una clave de desarrollo de al menos 16 caracteres:

```powershell
$env:VULCANO_WEBHOOK_SECRET = "<clave_de_desarrollo>"
```

La misma clave debe utilizarse al enviar los webhooks.

**Seguridad:** no utilizar secretos de producción en el código ni incorporarlos a Git.

11. Ejecución del receptor

En una terminal PowerShell, con el entorno virtual activo y LocalStack disponible:

```powershell
python -m uvicorn src.vulcano_telemetry.webhook_receiver:app --host 127.0.0.1 --port 8000
```

El receptor quedará escuchando solicitudes HTTP.

12. Envío de webhooks desde la CLI

En otra terminal:

```powershell
python .\src\app_operator.py --mode send-webhook --url http://127.0.0.1:8000/webhook --secret "<clave_de_desarrollo>"
```

Consultar ayuda:

```powershell
python .\src\app_operator.py --help
```

Consultar eventos SQS:

```powershell
python .\src\app_operator.py --mode start-dispatcher
```

**Alcance actual:** el modo `start-dispatcher` de esta CLI recibe eventos de SQS y registra su cantidad. No implementa por sí mismo un despacho HTTP completo hacia destinos externos.

13. Pruebas automatizadas

Las pruebas de los Integrantes 1, 2 y 3 se ejecutan mediante:

```powershell
python -m pytest .\tests\test_integrante1.py .\tests\test_integrante2.py .\tests\test_integrante3.py -v
```

Resultados obtenidos durante las verificaciones locales:

| Integrante | Pruebas aprobadas |
|---|---:|
| Integrante 1 | 40 |
| Integrante 2 | 14 |
| Integrante 3 | 19 |
| **Total** | **73** |

También se verificó la integración real entre la CLI, el receptor FastAPI y LocalStack SQS.

El receptor respondió HTTP `202 Accepted`, y se confirmó la presencia del mensaje en la cola principal con sus atributos de trazabilidad.

Las 73 pruebas corresponden a los tres módulos indicados y no representan una certificación de todas las funcionalidades del repositorio.

14. Estado de integración

Los módulos de los Integrantes 1, 2 y 3 se desarrollaron y verificaron localmente.

Los cambios se publicaron en:

- Rama: `feat/integrantes-1-2-3`.
- Commit inicial: `5a1839b`.
- Pull Request: `#9`.

La integración con la infraestructura LocalStack, el dispatcher HTTP y otros componentes desarrollados por el equipo requiere revisión y pruebas conjuntas antes de considerarse finalizada.