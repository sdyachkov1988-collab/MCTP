from mctp.core.constants import (
    CANONICAL_DIRECTIONAL_TIMEFRAME_VALUES,
    CANONICAL_MACRO_TIMEFRAME_VALUES,
    CANONICAL_OPTIONAL_PRECISION_TIMEFRAME_VALUES,
    CANONICAL_ROADMAP_TIMEFRAME_VALUES,
    CANONICAL_TRIGGER_TIMEFRAME_VALUES,
    STRATEGY_TIMEFRAMES,
    SUPPORTED_TIMEFRAME_VALUES,
)
from mctp.core.enums import Timeframe
from mctp.core.timeframes import (
    CANONICAL_DIRECTIONAL_TIMEFRAMES,
    CANONICAL_MACRO_TIMEFRAMES,
    CANONICAL_OPTIONAL_PRECISION_TIMEFRAMES,
    CANONICAL_ROADMAP_TIMEFRAMES,
    CANONICAL_TRIGGER_TIMEFRAMES,
    SUPPORTED_TIMEFRAMES,
    canonical_roadmap_timeframe_role,
    is_canonical_roadmap_timeframe,
    is_supported_timeframe,
)
from mctp.runtime.mtf_kline_manager import MTF_TIMEFRAMES


def test_supported_timeframes_include_generic_and_canonical_sets():
    assert SUPPORTED_TIMEFRAME_VALUES == STRATEGY_TIMEFRAMES
    assert Timeframe.M30 in SUPPORTED_TIMEFRAMES
    assert Timeframe.MONTHLY in SUPPORTED_TIMEFRAMES
    assert Timeframe.W1 in SUPPORTED_TIMEFRAMES


def test_canonical_roadmap_timeframes_match_roadmap_set_only():
    assert CANONICAL_ROADMAP_TIMEFRAME_VALUES == ("1M", "1w", "1d", "4h", "1h", "15m", "5m")
    assert CANONICAL_ROADMAP_TIMEFRAMES == (
        Timeframe.MONTHLY,
        Timeframe.W1,
        Timeframe.D1,
        Timeframe.H4,
        Timeframe.H1,
        Timeframe.M15,
        Timeframe.M5,
    )
    assert Timeframe.M30 not in CANONICAL_ROADMAP_TIMEFRAMES


def test_canonical_roadmap_roles_are_explicit():
    assert CANONICAL_MACRO_TIMEFRAME_VALUES == ("1M", "1w")
    assert CANONICAL_DIRECTIONAL_TIMEFRAME_VALUES == ("1d", "4h", "1h")
    assert CANONICAL_TRIGGER_TIMEFRAME_VALUES == ("15m",)
    assert CANONICAL_OPTIONAL_PRECISION_TIMEFRAME_VALUES == ("5m",)

    assert CANONICAL_MACRO_TIMEFRAMES == (Timeframe.MONTHLY, Timeframe.W1)
    assert CANONICAL_DIRECTIONAL_TIMEFRAMES == (Timeframe.D1, Timeframe.H4, Timeframe.H1)
    assert CANONICAL_TRIGGER_TIMEFRAMES == (Timeframe.M15,)
    assert CANONICAL_OPTIONAL_PRECISION_TIMEFRAMES == (Timeframe.M5,)

    assert canonical_roadmap_timeframe_role(Timeframe.MONTHLY) == "macro_context"
    assert canonical_roadmap_timeframe_role(Timeframe.W1) == "macro_context"
    assert canonical_roadmap_timeframe_role(Timeframe.D1) == "directional_structural"
    assert canonical_roadmap_timeframe_role(Timeframe.H4) == "directional_structural"
    assert canonical_roadmap_timeframe_role(Timeframe.H1) == "directional_structural"
    assert canonical_roadmap_timeframe_role(Timeframe.M15) == "trigger"
    assert canonical_roadmap_timeframe_role(Timeframe.M5) == "optional_precision"
    assert canonical_roadmap_timeframe_role(Timeframe.M30) is None


def test_supported_and_canonical_helpers_do_not_mix_m30():
    assert is_supported_timeframe(Timeframe.M30) is True
    assert is_canonical_roadmap_timeframe(Timeframe.M30) is False


def test_current_operational_runtime_mtf_set_remains_4tf():
    assert MTF_TIMEFRAMES == (
        Timeframe.M15,
        Timeframe.H1,
        Timeframe.H4,
        Timeframe.D1,
    )
