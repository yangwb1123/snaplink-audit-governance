"""Regression tests for checks/replay_round_reset.py (per-round accepted
reader reset gate for internal/kafka/replay.go).

Covers: missing reset (pre-fix shape) fails; reset before scanAccepted
passes; reset after scanAccepted fails; unparseable RunOnce fails closed;
and the current tree passes (the REQ-1 fix is in place)."""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from checks import replay_round_reset

ROOT = Path(__file__).resolve().parents[1]

PRE_FIX_RUNONCE = """\
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	collected, err := r.collectFailures(ctx)
	if err != nil {
		return 0, err
	}
	wanted := wantedEvents(collected)
	replayed, resolved, scanErr := r.scanAccepted(ctx, wanted)
	if scanErr != nil {
		return replayed, scanErr
	}
	return replayed, nil
}
"""

FIXED_RUNONCE = """\
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	collected, err := r.collectFailures(ctx)
	if err != nil {
		return 0, err
	}
	wanted := wantedEvents(collected)
	if err := r.resetAcceptedToFirstOffset(); err != nil {
		return 0, err
	}
	replayed, resolved, scanErr := r.scanAccepted(ctx, wanted)
	if scanErr != nil {
		return replayed, scanErr
	}
	return replayed, nil
}
"""

RESET_AFTER_SCAN_RUNONCE = """\
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	collected, err := r.collectFailures(ctx)
	if err != nil {
		return 0, err
	}
	wanted := wantedEvents(collected)
	replayed, resolved, scanErr := r.scanAccepted(ctx, wanted)
	if err := r.resetAcceptedToFirstOffset(); err != nil {
		return 0, err
	}
	if scanErr != nil {
		return replayed, scanErr
	}
	return replayed, nil
}
"""

UNBALANCED_RUNONCE = """\
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	collected, err := r.collectFailures(ctx)
	return replayed, nil
"""


def _write_fixture(root: Path, body: str) -> None:
    target = root / "internal" / "kafka"
    target.mkdir(parents=True, exist_ok=True)
    (target / "replay.go").write_text("package kafka\n\n" + body, encoding="utf-8")


class ReplayRoundResetTest(unittest.TestCase):
    def _run(self, body: str) -> int:
        with tempfile.TemporaryDirectory() as td:
            _write_fixture(Path(td), body)
            with redirect_stdout(io.StringIO()):
                return replay_round_reset.run(root=Path(td))

    def test_missing_reset_fails(self):
        """Pre-fix shape (no per-round reset in RunOnce) must fail the gate."""
        self.assertEqual(self._run(PRE_FIX_RUNONCE), 1)

    def test_reset_before_scan_passes(self):
        """REQ-1 shape (reset before scanAccepted) must pass the gate."""
        self.assertEqual(self._run(FIXED_RUNONCE), 0)

    def test_reset_after_scan_fails(self):
        """A reset placed AFTER scanAccepted still reuses the previous round's
        end position for the scan and must fail."""
        self.assertEqual(self._run(RESET_AFTER_SCAN_RUNONCE), 1)

    def test_unparseable_fails_closed(self):
        """A syntactically broken RunOnce body must fail, never pass."""
        self.assertEqual(self._run(UNBALANCED_RUNONCE), 1)

    def test_current_tree_passes(self):
        """The fixed tree satisfies the gate (guards against wiring drift)."""
        with redirect_stdout(io.StringIO()):
            self.assertEqual(replay_round_reset.run(root=ROOT), 0)


if __name__ == "__main__":
    unittest.main()
