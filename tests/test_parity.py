"""Unit tests for the pure diff logic in scripts/parity.py."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from parity import DiffResult, diff_sides, row_hash


def test_diff_sides_all_match():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1", "k2": "h2"}
    result = diff_sides(left, right)
    assert result == DiffResult(
        matched=2, mismatched=[], only_left=[], only_right=[]
    )


def test_diff_sides_hash_mismatch():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1", "k2": "DIFFERENT"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.mismatched == ["k2"]
    assert result.only_left == []
    assert result.only_right == []


def test_diff_sides_only_in_left():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.only_left == ["k2"]
    assert result.only_right == []


def test_diff_sides_only_in_right():
    left = {"k1": "h1"}
    right = {"k1": "h1", "k2": "h2"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.only_left == []
    assert result.only_right == ["k2"]


def test_diff_sides_empty_both_sides():
    result = diff_sides({}, {})
    assert result == DiffResult(matched=0, mismatched=[], only_left=[], only_right=[])


def test_row_hash_deterministic_and_null_handling():
    h1 = row_hash(["a", None, 1, 2.5])
    h2 = row_hash(["a", None, 1, 2.5])
    assert h1 == h2
    # None is distinguishable from the string 'None'
    assert row_hash([None]) != row_hash(["None"])
    # Order matters
    assert row_hash(["a", "b"]) != row_hash(["b", "a"])
