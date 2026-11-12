#!/usr/bin/env bash
# Build portable test executables or run a previously built bundle.
set -euo pipefail

usage() {
    cat <<'HELP'
Usage:
  research/kvo/bench.sh build OUT [GOOS GOARCH]
  research/kvo/bench.sh run BUNDLE RESULTS

Build defaults to the local target. Use `build /tmp/kvo-linux linux amd64`
for a self-contained Linux x64 bundle; the server does not need Go installed.

Run settings:
  KVO_BENCH       Go benchmark regexp (default .)
  KVO_COUNT       repetitions per case (default 5)
  KVO_BENCHTIME   time per repetition (default 150ms)
  KVO_CPU        optional Linux CPU affinity, e.g. 24
  KVO_TEST_ONLY  1 skips timing after correctness checks
  KVO_PACKAGES   space-separated binary names (default edb kvo mmap draft experiments)
  KVO_TMPDIR     isolated directory for temporary databases (default RESULTS/tmp)
  GOMAXPROCS     defaults to 1; recorded in environment.txt

No application databases are opened. Storage benchmarks create their own
files under KVO_TMPDIR. Nothing drops caches or changes system settings.
HELP
}

capture_runtime() {
    date -u '+UTC %Y-%m-%dT%H:%M:%SZ'
    uname -srmo 2>/dev/null || uname -srm
    printf 'GOMAXPROCS=%s\nGOGC=%s\nGOMEMLIMIT=%s\n' "${GOMAXPROCS:-1}" "${GOGC:-default}" "${GOMEMLIMIT:-default}"
    printf 'KVO_CPU=%s\nKVO_COUNT=%s\nKVO_BENCHTIME=%s\nKVO_BENCH=%s\n' "${KVO_CPU:-unbound}" "${KVO_COUNT:-5}" "${KVO_BENCHTIME:-150ms}" "${KVO_BENCH:-.}"
    if command -v lscpu >/dev/null; then
        lscpu
        free -m
        cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true
        cat /sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size 2>/dev/null || true
    elif command -v sysctl >/dev/null; then
        sysctl hw.model hw.memsize hw.cachelinesize hw.ncpu hw.physicalcpu machdep.cpu.brand_string
    fi
    uptime
}

case "${1:-}" in
build)
    [[ $# == 2 || $# == 4 ]] || { usage; exit 2; }
    repo=$(cd "$(dirname "$0")/../.." && pwd)
    mkdir -p "$2"
    bundle=$(cd "$2" && pwd)
    cd "$repo"
    target_os=${3:-$(go env GOOS)}
    target_arch=${4:-$(go env GOARCH)}
    replay=research/kvo/experiments/testdata/workload-shapes.json.gz
    {
        date -u '+UTC %Y-%m-%dT%H:%M:%SZ'
        go version
        git rev-parse HEAD
        printf 'target=%s/%s CGO_ENABLED=0 GOAMD64=v1\n' "$target_os" "$target_arch"
        if [[ -f "$replay" ]]; then
            printf 'workload-fixture=embedded\n'
        else
            printf 'workload-fixture=absent (sample-dependent cases skip)\n'
        fi
        git status --short
    } > "$bundle/build.txt"
    for package in edb kvo mmap draft experiments; do
        case "$package" in
            edb) path=.;;
            kvo|mmap) path=./$package;;
            *) path=./research/kvo/$package;;
        esac
        env GOOS="$target_os" GOARCH="$target_arch" GOAMD64=v1 CGO_ENABLED=0 \
            go test -c -o "$bundle/$package.test" "$path"
        go version -m "$bundle/$package.test" >> "$bundle/build.txt"
    done
    # Freeze sources, not just HEAD: this research deliberately has dirty drafts.
    # Include only Go/build/research sources and the explicitly named local replay.
    git ls-files --cached --others --exclude-standard | LC_ALL=C sort -u | \
        awk '/\.go$|^go\.(mod|sum)$|^research\/kvo\/[^\/]+\.(md|py|sh)$|^research\/kvo\/experiments\/testdata\/README\.md$/' > "$bundle/source-files.txt"
    # The replay stays out of Git, but a local bundle must record the exact input
    # embedded in its test binary. These generated bundles are local artifacts.
    if [[ -f "$replay" ]]; then
        printf '%s\n' "$replay" >> "$bundle/source-files.txt"
    fi
    tar -czf "$bundle/source.tar.gz" -T "$bundle/source-files.txt"
    cp research/kvo/bench.sh research/kvo/summarize.py "$bundle/"
    if command -v sha256sum >/dev/null; then
        (cd "$bundle" && sha256sum ./*.test source.tar.gz) > "$bundle/SHA256SUMS"
    else
        (cd "$bundle" && shasum -a 256 ./*.test source.tar.gz) > "$bundle/SHA256SUMS"
    fi
    printf 'Built %s/%s bundle: %s\n' "$target_os" "$target_arch" "$bundle"
    ;;
run)
    [[ $# == 3 ]] || { usage; exit 2; }
    bundle=$(cd "$2" && pwd)
    mkdir -p "$3"
    results=$(cd "$3" && pwd)
    [[ ! -e "$results/environment.txt" ]] || { printf 'Refusing to overwrite an existing run: %s\n' "$results" >&2; exit 1; }
    export GOMAXPROCS=${GOMAXPROCS:-1}
    export TMPDIR=${KVO_TMPDIR:-$results/tmp}
    mkdir -p "$TMPDIR"
    capture_runtime > "$results/environment.txt"
    cp "$bundle/build.txt" "$bundle/SHA256SUMS" "$results/"
    runner=()
    if [[ -n ${KVO_CPU:-} ]]; then
        command -v taskset >/dev/null || { printf 'KVO_CPU needs Linux taskset\n' >&2; exit 1; }
        runner=(taskset -c "$KVO_CPU")
    fi
    packages=${KVO_PACKAGES:-edb kvo mmap draft experiments}
    for package in $packages; do
        printf 'Checking %s\n' "$package"
        "${runner[@]}" "$bundle/$package.test" -test.v -test.count=1 -test.timeout=10m > "$results/test-$package.txt" 2>&1
        "$bundle/$package.test" -test.list '^Benchmark' > "$results/list-$package.txt"
        if [[ ${KVO_TEST_ONLY:-0} != 1 ]]; then
            printf 'Measuring %s\n' "$package"
            "${runner[@]}" "$bundle/$package.test" -test.run '^$' -test.bench "${KVO_BENCH:-.}" \
                -test.benchmem -test.benchtime "${KVO_BENCHTIME:-150ms}" \
                -test.count "${KVO_COUNT:-5}" -test.timeout=90m > "$results/bench-$package.txt" 2>&1
            if command -v python3 >/dev/null; then
                python3 "$bundle/summarize.py" "$results/bench-$package.txt"
            fi
        fi
    done
    capture_runtime > "$results/environment-end.txt"
    printf 'PASS\n' > "$results/completed.txt"
    printf 'Complete: %s\n' "$results"
    ;;
*) usage; exit 2;;
esac
