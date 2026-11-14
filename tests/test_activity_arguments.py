"""A workflow sends an activity exactly the arguments it declares.

Temporal converts an activity's arguments to their annotated types only when as many arrive as the function has
parameters; with fewer (a trailing default left out) every argument arrives as plain JSON, a dataclass as a dict.
The workflow tests run mock activities, so only this check sees a workflow call that is short of the real one."""

import ast
import dataclasses
import inspect
import typing

import pytest

import autodev.activities
import autodev.workflows
import mia_buffers.activities
import mia_buffers.workflows
import mia_data.activities
import mia_data.workflows

PAIRS = [
    (autodev.workflows, autodev.activities.Activities),
    (mia_data.workflows, mia_data.activities.Activities),
    (mia_buffers.workflows, mia_buffers.activities.Activities),
]


def _activities(cls: type) -> dict[str, typing.Callable]:
    """The class's activities by the name a workflow calls them by."""
    found = {}
    for attribute in vars(cls).values():
        definition = getattr(attribute, "__temporal_activity_definition", None)
        if definition is not None:
            found[definition.name] = attribute
    return found


def _holds_dataclass(hint: object) -> bool:
    """Whether a value of this type needs converting from JSON: a dataclass, or a container or union of one."""
    return dataclasses.is_dataclass(hint) or any(_holds_dataclass(inner) for inner in typing.get_args(hint))


def _calls(module: object) -> list[tuple[str, int, int]]:
    """(activity name, arguments sent, line) of each call by name whose arguments can be counted: `_act("name",
    a, b)` and `execute_activity("name", args=[a, b])`; a call that unpacks a list is left out."""
    calls = []
    for node in ast.walk(ast.parse(inspect.getsource(module))):
        if not (isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant)):
            continue
        name, function = node.args[0].value, getattr(node.func, "attr", "")
        if not isinstance(name, str):
            continue
        if function == "_act":
            sent: list[ast.expr] | None = node.args[1:]
        elif function in ("execute_activity", "start_activity"):
            listed = next((keyword.value for keyword in node.keywords if keyword.arg == "args"), None)
            sent = listed.elts if isinstance(listed, ast.List) else None
        else:
            continue
        if sent is not None and not any(isinstance(argument, ast.Starred) for argument in sent):
            calls.append((name, len(sent), node.lineno))
    return calls


@pytest.mark.parametrize(("module", "cls"), PAIRS, ids=lambda value: getattr(value, "__name__", ""))
def test_a_workflow_sends_a_typed_activity_every_argument_it_declares(module, cls):
    activities, short = _activities(cls), []
    checked = 0
    for name, sent, line in _calls(module):
        function = activities.get(name)
        if function is None:
            continue
        hints = typing.get_type_hints(function)
        parameters = [parameter for parameter in inspect.signature(function).parameters if parameter != "self"]
        if any(_holds_dataclass(hints.get(parameter)) for parameter in parameters):
            checked += 1
            if sent != len(parameters):
                short.append(f"{module.__name__}:{line} sends {name} {sent} arguments; it declares {len(parameters)}")
    assert not short, "\n".join(short)
    assert checked or module is autodev.workflows  # the check found the calls it is there for
