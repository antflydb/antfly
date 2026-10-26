from pathlib import Path
import hashlib

ROOT = Path(__file__).resolve().parent
PATCH = ROOT / "attention-stored-installed-baseline-preflight-v1.patch"
DESIGN = ROOT / "attention-stored-installed-baseline-preflight-v1.md"

def test_patch_is_unapplied_and_scoped():
    text = PATCH.read_text()
    assert text.count("diff --git a/") == 5
    assert "serving_authority: bool = false" in text
    assert "release_authority: bool = false" in text
    assert "if (!builtin.is_test" in text

def test_owner_semantics_are_explicit():
    text = PATCH.read_text()
    assert "global_owner_count" in text
    assert "total_owner_count" in text
    assert "globals.len + self.sliding.len" in text
    added = "\n".join(line[1:] for line in text.splitlines() if line.startswith("+") and not line.startswith("+++"))
    assert "expected_owner_count" not in added

def test_preflight_precedes_reservation():
    text = PATCH.read_text()
    check = text.index("const measured_baseline = try live.committedContextStorageDiagnostic")
    binding = text.index("ProviderBinding and reserve_fn")
    assert binding < check
    design = DESIGN.read_text()
    assert "before loading the stored manifest driver or calling `reserve_fn`" in design

def test_failure_evidence_is_versioned_and_nonauthorizing():
    text = PATCH.read_text()
    assert "stored-installed-external-failure.v1" in text
    assert "writeFailureReceipt" in text
    assert "committed_but_receipt_failed" in text
    assert "never replace an existing receipt" in text

def test_static_prediction_is_not_acceptance_authority():
    assert "Static prediction remains diagnostic evidence only" in DESIGN.read_text()
