"""Configuración de la suite de Vulcano Telemetry.

Las pruebas marcadas `integration` necesitan LocalStack real en http://localhost:4566 y sólo
corren con `VULCANO_IT=1`. El resto de la suite debe poder ejecutarse sin Docker.
"""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.getenv("VULCANO_IT") == "1":
        return
    skip_marker = pytest.mark.skip(
        reason="integración: requiere LocalStack en http://localhost:4566 (activar con VULCANO_IT=1)"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
