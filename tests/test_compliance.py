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

ROOT = Path(__file__).resolve().parents[1]

# BaseException y sus alias: capturarlos atrapa KeyboardInterrupt y SystemExit.
FORBIDDEN_NAMES = {"BaseException", "BaseExceptionGroup", "GeneratorExit", "KeyboardInterrupt", "SystemExit"}

# Un return/break/continue dentro de estas estructuras no sale del finally que lo contiene.
BOUNDARY_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def _python_files(folder: Path) -> list[Path]:
    if not folder.is_dir():
        return []
    return sorted(folder.rglob("*.py"))


def _parsed_trees() -> list[tuple[Path, ast.Module]]:
    paths = _python_files(ROOT / "src") + _python_files(ROOT / "tests")
    trees = []
    for path in paths:
        trees.append((path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))))
    return trees


def _exception_names(node: ast.expr | None) -> set[str]:
    """Nombres de excepción capturados por un `except`, sea simple, tupla o atributo."""
    if node is None:
        return set()
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, ast.Tuple):
        return set().union(*(_exception_names(element) for element in node.elts))
    return set()


def _abrupt_exits(body: list[ast.stmt]) -> list[ast.stmt]:
    """Sentencias `return`/`break`/`continue` alcanzables sin cruzar un def/class/lambda."""
    found = []
    pending: list[ast.AST] = list(body)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.Return, ast.Break, ast.Continue)):
            found.append(node)
            continue
        if isinstance(node, BOUNDARY_NODES):
            continue
        pending.extend(ast.iter_child_nodes(node))
    return found


@pytest.mark.parametrize(
    ("description", "detects"),
    [
        (
            "except desnudo o que captura BaseException o sus alias",
            lambda node: node.type is None or bool(_exception_names(node.type) & FORBIDDEN_NAMES),
        ),
        (
            "except cuyo cuerpo es sólo pass (excepción silenciada)",
            lambda node: len(node.body) == 1 and isinstance(node.body[0], ast.Pass),
        ),
    ],
)
def test_criterion_1_baseexception_prohibition(description, detects):
    violations = [
        f"{path.relative_to(ROOT)}:{node.lineno}"
        for path, tree in _parsed_trees()
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and detects(node)
    ]
    assert not violations, f"Criterio 1 — {description}: {violations}"


def test_criterion_2_pep765_no_abrupt_exits_in_finally():
    violations = []
    for path, tree in _parsed_trees():
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Try, ast.TryStar)):
                continue
            for exit_node in _abrupt_exits(node.finalbody):
                violations.append(f"{path.relative_to(ROOT)}:{exit_node.lineno} {type(exit_node).__name__}")

    assert not violations, f"Criterio 2 — PEP 765, salidas abruptas dentro de finally: {violations}"


def test_criterion_1_domain_exceptions_inherit_from_exception():
    from vulcano_telemetry import exceptions as domain_module

    violations = [
        name
        for name, obj in vars(domain_module).items()
        if isinstance(obj, type) and issubclass(obj, BaseException) and not issubclass(obj, Exception)
    ]
    assert not violations, f"Criterio 1 — heredan de BaseException: {violations}"
