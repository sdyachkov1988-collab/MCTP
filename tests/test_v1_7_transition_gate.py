from pathlib import Path


def test_v17_to_v20_readiness_gate_artifacts_exist():
    root = Path(__file__).resolve().parent.parent
    expected = [
        root / "docs" / "README.md",
        root / "docs" / "v1_7_operator_runbook.md",
        root / "docs" / "v1_7_pre_live_checklist.md",
        root / "docs" / "v1_7_incident_journal_template.md",
        root / "docs" / "v1_7_operator_intervention_rules.md",
        root / "docs" / "v1_7_to_v2_0_readiness_gate.md",
    ]
    for path in expected:
        assert path.exists(), f"missing readiness artifact: {path.name}"


def test_v17_to_v20_repo_phase_boundary_docs_are_consistent():
    root = Path(__file__).resolve().parent.parent
    readme = (root / "README.md").read_text(encoding="utf-8")
    context = (root / "MCTP_context.md").read_text(encoding="utf-8")
    gate = (root / "docs" / "v1_7_to_v2_0_readiness_gate.md").read_text(encoding="utf-8")

    assert "`v1.7`" in readme
    assert "`v1.7`" in context
    assert "`v1.7`" in gate
    assert "`v2.0`" in gate

    assert "`v1.7+` live-readiness / chaos / operator framework" not in readme
    assert "`v1.7+` live-readiness / chaos / operator automation" not in context

    assert "scenario matrix" in gate
    assert "chaos / integration verification completed" in gate
    assert "all 4 websocket streams independently verified" in gate
