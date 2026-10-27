"""Guardián estático de los criterios 1 y 2 de la evaluación (§3 del enunciado).

No valida comportamiento: valida que el árbol de código no pueda violar las dos reglas
sintácticas que la cátedra evalúa. Se ejecuta sobre `src/` y `tests/` completos, incluidos
los módulos de los roles 1-3, porque el criterio es del trabajo, no de un módulo.

Criterio 1 (§3): excepciones propias heredan de `Exception`; prohibido `except:` desnudo,
`except: pass` y capturar `BaseException`.
Criterio 2 (§3, PEP 765): prohibido `return`, `break` o `continue` que salgan de un `finally`.
"""

import ast
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]

# BaseException y sus alias: capturarlos atrapa KeyboardInterrupt y SystemExit.
PROHIBIDOS = {"BaseException", "BaseExceptionGroup", "GeneratorExit", "KeyboardInterrupt", "SystemExit"}

# Un return/break/continue dentro de estas estructuras no sale del finally que lo contiene.
NO_ATRAVIESA = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def _archivos(carpeta: Path) -> list[Path]:
    if not carpeta.is_dir():
        return []
    return sorted(carpeta.rglob("*.py"))


def _arboles() -> list[tuple[Path, ast.Module]]:
    rutas = _archivos(RAIZ / "src") + _archivos(RAIZ / "tests")
    arboles = []
    for ruta in rutas:
        arboles.append((ruta, ast.parse(ruta.read_text(encoding="utf-8"), filename=str(ruta))))
    return arboles


def _nombres(nodo: ast.expr | None) -> set[str]:
    """Nombres de excepción capturados por un `except`, sea simple, tupla o atributo."""
    if nodo is None:
        return set()
    if isinstance(nodo, ast.Name):
        return {nodo.id}
    if isinstance(nodo, ast.Attribute):
        return {nodo.attr}
    if isinstance(nodo, ast.Tuple):
        return set().union(*(_nombres(elemento) for elemento in nodo.elts))
    return set()


def _salidas_abruptas(cuerpo: list[ast.stmt]) -> list[ast.stmt]:
    """Sentencias `return`/`break`/`continue` alcanzables sin cruzar un def/class/lambda."""
    encontradas = []
    pendientes: list[ast.AST] = list(cuerpo)
    while pendientes:
        nodo = pendientes.pop()
        if isinstance(nodo, (ast.Return, ast.Break, ast.Continue)):
            encontradas.append(nodo)
            continue
        if isinstance(nodo, NO_ATRAVIESA):
            continue
        pendientes.extend(ast.iter_child_nodes(nodo))
    return encontradas


@pytest.mark.parametrize(
    ("descripcion", "detectar"),
    [
        (
            "except desnudo o que captura BaseException o sus alias",
            lambda nodo: nodo.type is None or bool(_nombres(nodo.type) & PROHIBIDOS),
        ),
        (
            "except cuyo cuerpo es sólo pass (excepción silenciada)",
            lambda nodo: len(nodo.body) == 1 and isinstance(nodo.body[0], ast.Pass),
        ),
    ],
)
def test_criterio_1_prohibicion_de_baseexception(descripcion, detectar):
    infracciones = [
        f"{ruta.relative_to(RAIZ)}:{nodo.lineno}"
        for ruta, arbol in _arboles()
        for nodo in ast.walk(arbol)
        if isinstance(nodo, ast.ExceptHandler) and detectar(nodo)
    ]
    assert not infracciones, f"Criterio 1 — {descripcion}: {infracciones}"


def test_criterio_2_pep765_sin_salidas_abruptas_en_finally():
    infracciones = []
    for ruta, arbol in _arboles():
        for nodo in ast.walk(arbol):
            if not isinstance(nodo, (ast.Try, ast.TryStar)):
                continue
            for salida in _salidas_abruptas(nodo.finalbody):
                infracciones.append(f"{ruta.relative_to(RAIZ)}:{salida.lineno} {type(salida).__name__}")

    assert not infracciones, f"Criterio 2 — PEP 765, salidas abruptas dentro de finally: {infracciones}"


def test_criterio_1_las_excepciones_del_dominio_heredan_de_exception():
    from vulcano_telemetry import exceptions as dominio

    violaciones = [
        nombre
        for nombre, objeto in vars(dominio).items()
        if isinstance(objeto, type) and issubclass(objeto, BaseException) and not issubclass(objeto, Exception)
    ]
    assert not violaciones, f"Criterio 1 — heredan de BaseException: {violaciones}"
