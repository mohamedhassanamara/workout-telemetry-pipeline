import json

import pytest

from telemetry.schemas.events import WorkoutSetLogged
from telemetry.schemas.registry import inline_refs

EXERCISES = {"back_squat", "bench_press", "deadlift", "overhead_press", "barbell_row", "pull_up"}


def test_inline_refs_removes_all_references():
    flat = inline_refs(WorkoutSetLogged.model_json_schema())
    text = json.dumps(flat)
    assert "$ref" not in text
    assert "$defs" not in text


def test_exercise_property_carries_the_enum_values():
    flat = inline_refs(WorkoutSetLogged.model_json_schema())
    assert set(flat["properties"]["exercise"]["enum"]) == EXERCISES


def test_inline_refs_does_not_mutate_its_input():
    schema = WorkoutSetLogged.model_json_schema()
    before = json.dumps(schema, sort_keys=True)
    inline_refs(schema)
    assert json.dumps(schema, sort_keys=True) == before


def test_sibling_keys_override_the_definition():
    schema = {
        "$defs": {"X": {"type": "string", "title": "X"}},
        "properties": {"a": {"$ref": "#/$defs/X", "title": "A"}},
    }
    assert inline_refs(schema) == {"properties": {"a": {"type": "string", "title": "A"}}}


def test_unresolvable_reference_raises():
    with pytest.raises(ValueError, match="unresolvable"):
        inline_refs({"properties": {"a": {"$ref": "#/$defs/Missing"}}})


def test_cyclic_reference_raises():
    schema = {
        "$defs": {"A": {"items": {"$ref": "#/$defs/A"}}},
        "properties": {"a": {"$ref": "#/$defs/A"}},
    }
    with pytest.raises(ValueError, match="cyclic"):
        inline_refs(schema)
