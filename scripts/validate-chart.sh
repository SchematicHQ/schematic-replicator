#!/bin/bash
# Validate the Helm chart: lint, render every ci/ value file through strict
# Kubernetes schema validation, assert the single-writer invariants hold, and
# confirm the misconfiguration guards still reject bad input.
#
# Requires: helm, kubeconform
# Usage: ./scripts/validate-chart.sh

set -uo pipefail

CHART="deployments/charts/schematic-replicator"
FAILURES=0

green() { printf '\033[0;32m%s\033[0m\n' "$1"; }
red() { printf '\033[0;31m%s\033[0m\n' "$1"; }

pass() { green "  PASS  $1"; }
fail() {
    red "  FAIL  $1"
    FAILURES=$((FAILURES + 1))
}

for cmd in helm kubeconform; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        red "$cmd is required but not installed"
        exit 1
    fi
done

echo "==> helm lint"
if helm lint "$CHART" --set schematic.apiKey=lint-key >/dev/null 2>&1; then
    pass "lint"
else
    fail "lint"
    helm lint "$CHART" --set schematic.apiKey=lint-key
fi

echo "==> render + schema validation"
for values in "$CHART"/ci/*.yaml; do
    name=$(basename "$values")
    rendered=$(helm template ci-release "$CHART" -f "$values" 2>&1)
    if [ $? -ne 0 ]; then
        fail "$name (template error)"
        echo "$rendered" | head -5
        continue
    fi
    if echo "$rendered" | kubeconform -strict -summary 2>&1 | grep -q "Invalid: 0, Errors: 0"; then
        pass "$name"
    else
        fail "$name (schema)"
        echo "$rendered" | kubeconform -strict 2>&1 | grep -i invalid | head -3
    fi
done

# The chart exists to enforce these. A refactor that lets either become
# configurable reintroduces a crash-loop (replicas) or a deadlocked rollout
# (strategy), so assert them against every value file rather than trusting
# the template to stay correct.
echo "==> single-writer invariants"
for values in "$CHART"/ci/*.yaml; do
    name=$(basename "$values")
    rendered=$(helm template ci-release "$CHART" -f "$values" 2>/dev/null)
    replicas=$(echo "$rendered" | awk '/^kind: Deployment/,0' | awk '/^  replicas:/ {print $2; exit}')
    strategy=$(echo "$rendered" | awk '/^kind: Deployment/,0' | awk '/^    type:/ {print $2; exit}')
    if [ "$replicas" = "1" ] && [ "$strategy" = "Recreate" ]; then
        pass "$name (replicas=1, strategy=Recreate)"
    else
        fail "$name (replicas=$replicas, strategy=$strategy)"
    fi
done

# Each guard must reject its bad input. Without these, a broken guard fails
# open and is invisible until a customer hits it at runtime.
echo "==> misconfiguration guards reject bad input"
assert_rejects() {
    local desc="$1"
    shift
    if helm template ci-release "$CHART" "$@" >/dev/null 2>&1; then
        fail "$desc (accepted, should have been rejected)"
    else
        pass "$desc"
    fi
}

assert_rejects "missing API key"
assert_rejects "redis.addr as URL" \
    --set schematic.apiKey=k --set redis.addr=redis://host:6379
assert_rejects "redis.addr as rediss URL" \
    --set schematic.apiKey=k --set redis.addr=rediss://host:6379
assert_rejects "cluster enabled without addrs" \
    --set schematic.apiKey=k --set redis.cluster.enabled=true
assert_rejects "cluster addr as URL" \
    --set schematic.apiKey=k --set redis.cluster.enabled=true \
    --set redis.cluster.addrs=redis://a:7000
assert_rejects "empty redis.addr" \
    --set schematic.apiKey=k --set redis.addr=""
assert_rejects "existingSecret without key" \
    --set schematic.existingSecret=s --set schematic.existingSecretKey=""

# The chart mirrors the application's environment surface. Nothing else couples
# them, so a new os.Getenv in the code silently becomes unreachable via the
# chart unless this check fails the build.
echo "==> chart env surface matches the application"
ENV_DRIFT=$(python3 - "$CHART" <<'PYEOF'
import glob, os, re, sys

chart = sys.argv[1]

# Env vars the application reads. Handles both os.Getenv("LITERAL") and the
# const-indirect form (apiKeyEnvVar = "SCHEMATIC_API_KEY"; os.Getenv(apiKeyEnvVar)).
src = ""
for path in glob.glob("*.go"):
    if path.endswith("_test.go"):
        continue
    with open(path) as handle:
        src += handle.read()

code = set(re.findall(r'os\.Getenv\("([A-Z][A-Z0-9_]*)"\)', src))
consts = dict(re.findall(r'(\w+)\s*=\s*"([A-Z][A-Z0-9_]*)"', src))
for ident in re.findall(r'os\.Getenv\((\w+)\)', src):
    if ident in consts:
        code.add(consts[ident])

# Env vars the chart can emit.
tpl = ""
for path in (
    os.path.join(chart, "templates", "_helpers.tpl"),
    os.path.join(chart, "templates", "deployment.yaml"),
):
    with open(path) as handle:
        tpl += handle.read()

emitted = set(re.findall(r'"name"\s+"([A-Z][A-Z0-9_]*)"', tpl))
emitted |= set(re.findall(r'-\s+name:\s+([A-Z][A-Z0-9_]*)\b', tpl))

# Vars deliberately left to extraEnv rather than modelled as a first-class value.
allowed_missing: set[str] = set()

missing = code - emitted - allowed_missing
extra = emitted - code

for name in sorted(missing):
    print(f"read by app, not settable via chart: {name}")
for name in sorted(extra):
    print(f"emitted by chart, not read by app: {name}")
PYEOF
)
if [ -z "$ENV_DRIFT" ]; then
    pass "env surface in sync"
else
    while IFS= read -r line; do fail "$line"; done <<<"$ENV_DRIFT"
fi

echo
if [ "$FAILURES" -eq 0 ]; then
    green "chart validation passed"
    exit 0
fi
red "chart validation failed: $FAILURES check(s)"
exit 1
