#!/usr/bin/env bash
# Gate-level proto drift simulation (R8 / AC-1).
#
# Injects a field into api/proto/audit.proto inside a `git archive HEAD`
# copy, runs the full quality gate, requires it to FAIL naming the drifted
# field, restores the copy, and requires the gate to PASS again.  The
# committed tree is never touched (F9: scripts/tests operate only on copies).
#
# Overridable knobs (used by the unit test to keep this fast and
# deterministic without a full ~2x95s gate run):
#   GATE_CMD   command run twice inside the copy (default: the real gate)
#   ANCHOR     .proto line the injection anchors on
set -euo pipefail

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git archive HEAD | tar -x -C "$tmp"
cd "$tmp"

ANCHOR=${ANCHOR:-"  string execution_run_id = 27;"}
GATE_CMD=${GATE_CMD:-python3 cli.py quality}

# Derive the next free EventEnvelope field number by scanning, never
# hard-coding (the test survives future field additions).
NEXT=$(python3 - <<'EOF'
import re
proto = open("api/proto/audit.proto").read()
env = re.search(r"message EventEnvelope \{(.*?)\n\}", proto, re.S)
if not env:
    print("FAIL: drift-simulate: EventEnvelope block not found", file=sys.stderr)
    raise SystemExit(1)
nums = [int(n) for n in re.findall(r"= (\d+);", env.group(1))]
print(max(nums) + 1)
EOF
)

# Inject drift_probe into the copy.
python3 - "$NEXT" "$ANCHOR" <<'EOF'
import sys
path = "api/proto/audit.proto"
source = open(path).read()
anchor = sys.argv[2]
if anchor not in source:
    print(f"FAIL: drift-simulate: anchor {anchor!r} not found in audit.proto "
          f"(EventEnvelope layout changed?)", file=sys.stderr)
    raise SystemExit(1)
number = sys.argv[1]
source = source.replace(anchor, anchor + f"\n  string drift_probe = {number};")
open(path, "w").write(source)
EOF

# The drifted tree must FAIL the gate, naming the drifted field.
if $GATE_CMD > gate.log 2>&1; then
    echo "FAIL: drift simulation: gate passed despite proto drift" >&2
    exit 1
fi
if ! grep -q "drift_probe" gate.log || ! grep -q "EventEnvelope" gate.log; then
    echo "FAIL: drift simulation: gate failed but did not name drifted field 'drift_probe' in EventEnvelope" >&2
    exit 1
fi

# Restore the copy to the pristine state.
python3 - "$NEXT" <<'EOF'
import sys
path = "api/proto/audit.proto"
source = open(path).read()
needle = f"  string drift_probe = {sys.argv[1]};\n"
if needle not in source:
    print("FAIL: drift-simulate: injected field drift_probe not found (restore aborted)", file=sys.stderr)
    raise SystemExit(1)
open(path, "w").write(source.replace(needle, ""))
EOF
if grep -q "drift_probe" api/proto/audit.proto; then
    echo "FAIL: drift-simulate: drift_probe still present after restore" >&2
    exit 1
fi

# The restored tree must PASS the full gate.
if ! $GATE_CMD > gate2.log 2>&1; then
    echo "FAIL: drift simulation: restored tree gate failed (see gate2.log)" >&2
    exit 1
fi
echo "drift simulation: PASS"
