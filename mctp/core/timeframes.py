from __future__ import annotations

from mctp.core.constants import (
    CANONICAL_DIRECTIONAL_TIMEFRAME_VALUES,
    CANONICAL_MACRO_TIMEFRAME_VALUES,
    CANONICAL_OPTIONAL_PRECISION_TIMEFRAME_VALUES,
    CANONICAL_ROADMAP_TIMEFRAME_VALUES,
    CANONICAL_TRIGGER_TIMEFRAME_VALUES,
    SUPPORTED_TIMEFRAME_VALUES,
)
from mctp.core.enums import Timeframe


SUPPORTED_TIMEFRAMES: tuple[Timeframe, ...] = tuple(Timeframe(value) for value in SUPPORTED_TIMEFRAME_VALUES)
CANONICAL_ROADMAP_TIMEFRAMES: tuple[Timeframe, ...] = tuple(
    Timeframe(value) for value in CANONICAL_ROADMAP_TIMEFRAME_VALUES
)
CANONICAL_MACRO_TIMEFRAMES: tuple[Timeframe, ...] = tuple(
    Timeframe(value) for value in CANONICAL_MACRO_TIMEFRAME_VALUES
)
CANONICAL_DIRECTIONAL_TIMEFRAMES: tuple[Timeframe, ...] = tuple(
    Timeframe(value) for value in CANONICAL_DIRECTIONAL_TIMEFRAME_VALUES
)
CANONICAL_TRIGGER_TIMEFRAMES: tuple[Timeframe, ...] = tuple(
    Timeframe(value) for value in CANONICAL_TRIGGER_TIMEFRAME_VALUES
)
CANONICAL_OPTIONAL_PRECISION_TIMEFRAMES: tuple[Timeframe, ...] = tuple(
    Timeframe(value) for value in CANONICAL_OPTIONAL_PRECISION_TIMEFRAME_VALUES
)

_CANONICAL_ROADMAP_TIMEFRAME_ROLES: dict[Timeframe, str] = {
    Timeframe.MONTHLY: "macro_context",
    Timeframe.W1: "macro_context",
    Timeframe.D1: "directional_structural",
    Timeframe.H4: "directional_structural",
    Timeframe.H1: "directional_structural",
    Timeframe.M15: "trigger",
    Timeframe.M5: "optional_precision",
}


def is_supported_timeframe(timeframe: Timeframe) -> bool:
    return timeframe in SUPPORTED_TIMEFRAMES


def is_canonical_roadmap_timeframe(timeframe: Timeframe) -> bool:
    return timeframe in CANONICAL_ROADMAP_TIMEFRAMES


def canonical_roadmap_timeframe_role(timeframe: Timeframe) -> str | None:
    return _CANONICAL_ROADMAP_TIMEFRAME_ROLES.get(timeframe)
