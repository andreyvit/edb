#!/bin/sh
# LUA_PREFIX points to a Lua or LuaJIT installation.
set -eu
cd "$(dirname "$0")"
: "${LUA_PREFIX:?set LUA_PREFIX to a Lua or LuaJIT installation}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if [ -d "$LUA_PREFIX/include/luajit-2.1" ]; then
    inc="$LUA_PREFIX/include/luajit-2.1"
    lib=luajit-5.1
    flags=-DCHECK_LUAJIT
else
    inc=${LUA_INCLUDE:-$LUA_PREFIX/include}
    lib=lua
    flags=
    if [ ! -f "$inc/lua.h" ] && [ -f "$inc/lua/lua.h" ]; then inc="$inc/lua"; fi
fi
${CC:-cc} -O2 $flags -Wall -Wextra -I"$inc" bench.c -L"$LUA_PREFIX/lib" -Wl,-rpath,"$LUA_PREFIX/lib" -l"$lib" -o "$tmp/bench"
if [ "${1:-}" = --build ]; then
    cp "$tmp/bench" "${2:?pass an absolute output path}"
    exit 0
fi
data=../../internal/workload/testdata
for name in ${WORKLOADS:-Price Scan Fib Sieve Mandelbrot}; do
    sample=0
    while [ "$sample" -lt "${SAMPLES:-5}" ]; do
        "$tmp/bench" "$data/guest.lua" "$data/expected.txt" "$name" "${ITERATIONS:-20000}"
        sample=$((sample+1))
    done
done
