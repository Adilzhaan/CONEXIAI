"""Sandboxed arithmetic evaluator for client-defined risk formulas.

NEVER uses eval()/exec(). Expressions are parsed to an AST and walked against a
strict whitelist: numbers, the declared factor variables, the operators
+ - * / // % **, unary +/-, and the functions min/max/clamp/abs/log/sqrt/pow.
Anything else (attribute access, subscripts, calls to other names, comprehensions,
lambdas, names not in `variables`) is rejected at validation time.
"""
from __future__ import annotations

import ast
import math

# Allowed AST node types (everything else → reject).
_ALLOWED_NODES = (
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.Call, ast.Name, ast.Load,
    ast.Constant,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.USub, ast.UAdd,
)


def _clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


_ALLOWED_FUNCS = {
    "min": min,
    "max": max,
    "abs": abs,
    "clamp": _clamp,
    "log": lambda x, base=math.e: math.log(x, base) if x > 0 else 0.0,
    "sqrt": lambda x: math.sqrt(x) if x >= 0 else 0.0,
    "pow": pow,
}


class FormulaError(ValueError):
    """Raised when an expression is unsafe, malformed, or fails to evaluate."""


def validate_expression(expression: str, variables: list[str]) -> None:
    """Parse + whitelist check. Raises FormulaError on anything unsafe.

    Does NOT evaluate — see range_check / safe_eval for that."""
    expr = (expression or "").strip()
    if not expr:
        raise FormulaError("Пустое выражение")
    if len(expr) > 500:
        raise FormulaError("Выражение слишком длинное")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise FormulaError(f"Синтаксическая ошибка: {e.msg}") from e

    allowed_names = set(variables or [])
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise FormulaError(f"Запрещённая конструкция: {type(node).__name__}")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
            raise FormulaError("Разрешены только числа")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_FUNCS:
                raise FormulaError("Разрешены только функции: " + ", ".join(_ALLOWED_FUNCS))
            if node.keywords:
                raise FormulaError("Именованные аргументы запрещены")
        if isinstance(node, ast.Name) and node.id not in allowed_names and node.id not in _ALLOWED_FUNCS:
            raise FormulaError(f"Неизвестная переменная: {node.id}")


def _eval_node(node: ast.AST, env: dict):
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, env)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in env:
            return env[node.id]
        raise FormulaError(f"Неизвестная переменная: {node.id}")
    if isinstance(node, ast.UnaryOp):
        v = _eval_node(node.operand, env)
        return +v if isinstance(node.op, ast.UAdd) else -v
    if isinstance(node, ast.BinOp):
        a, b = _eval_node(node.left, env), _eval_node(node.right, env)
        op = node.op
        if isinstance(op, ast.Add):
            return a + b
        if isinstance(op, ast.Sub):
            return a - b
        if isinstance(op, ast.Mult):
            return a * b
        if isinstance(op, ast.Div):
            return a / b if b != 0 else 0.0
        if isinstance(op, ast.FloorDiv):
            return a // b if b != 0 else 0.0
        if isinstance(op, ast.Mod):
            return a % b if b != 0 else 0.0
        if isinstance(op, ast.Pow):
            return a ** b
    if isinstance(node, ast.Call):
        fn = _ALLOWED_FUNCS[node.func.id]
        args = [_eval_node(a, env) for a in node.args]
        return fn(*args)
    raise FormulaError(f"Запрещённая конструкция: {type(node).__name__}")


def safe_eval(expression: str, values: dict, clamp_to=(0.0, 100.0)) -> float:
    """Evaluate a (pre-validated) expression with factor `values`.
    Result is forced into clamp_to (default 0–100). NaN/inf → FormulaError."""
    tree = ast.parse((expression or "").strip(), mode="eval")
    out = _eval_node(tree, {k: float(v) for k, v in (values or {}).items()})
    out = float(out)
    if out != out or out in (float("inf"), float("-inf")):  # NaN / inf
        raise FormulaError("Результат не число")
    lo, hi = clamp_to
    return _clamp(out, lo, hi)


def range_check(expression: str, variables: list[str]) -> list[dict]:
    """Run the expression on extreme + mid inputs; every result must land in
    0–100. Returns the sample table for display. Raises on failure."""
    vars_ = list(variables or [])
    cases = {
        "all 0":  {v: 0 for v in vars_},
        "all 10": {v: 10 for v in vars_},
        "all 5":  {v: 5 for v in vars_},
    }
    results = []
    for label, vals in cases.items():
        score = safe_eval(expression, vals)  # already clamped 0–100
        results.append({"case": label, "inputs": vals, "score": round(score)})
    return results
