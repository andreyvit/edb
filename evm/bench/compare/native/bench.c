// Native embedding benchmark. Build against LuaJIT or PUC Lua.
#include <lauxlib.h>
#include <lualib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int readings[256], limits[256];

static int sensor(lua_State *L, const int *data) {
    int i = luaL_checkinteger(L, 1);
    if (i < 0 || i >= 256) return luaL_error(L, "bad sensor index");
    lua_pushinteger(L, data[i]);
    return 1;
}
static int reading(lua_State *L) { return sensor(L, readings); }
static int limit(lua_State *L) { return sensor(L, limits); }

static double seconds(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + t.tv_nsec * 1e-9;
}

static void checked_call(lua_State *L, int fn, int seed, int want) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, fn);
    lua_pushinteger(L, seed);
    if (lua_pcall(L, 1, 1, 0)) {
        fprintf(stderr, "%s\n", lua_tostring(L, -1));
        exit(1);
    }
    if (lua_type(L, -1) != LUA_TNUMBER || lua_tonumber(L, -1) != want) {
        fprintf(stderr, "seed %d: got %g, want %d\n", seed, lua_tonumber(L,-1), want);
        exit(1);
    }
    lua_pop(L, 1);
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "usage: bench guest.lua expected.txt workload iterations\n");
        return 1;
    }
    int count = atoi(argv[4]), want[4] = {-1, -1, -1, -1};
    if (count < 1) return 1;
    FILE *file = fopen(argv[2], "r");
    if (!file) return 1;
    char name[32];
    int seed, answer;
    while (fscanf(file, "%31s %d %d", name, &seed, &answer) == 3) {
        if (!strcmp(name, argv[3]) && seed >= 0 && seed < 4) want[seed] = answer;
    }
    fclose(file);
    for (int i = 0; i < 4; ++i) if (want[i] < 0) return 1;
    for (int i = 0; i < 256; ++i) {
        readings[i] = (i*73+19)%211;
        limits[i] = 90+i*17%67;
    }
    lua_State *L = luaL_newstate();
    if (!L) return 1;
    luaL_openlibs(L);
    lua_register(L, "reading", reading);
    lua_register(L, "limit", limit);
#ifdef CHECK_LUAJIT
    const char *mode = getenv("LUAJIT_MODE");
    int jit_off = mode && !strcmp(mode, "off");
    if (jit_off && luaL_dostring(L, "jit.off(); assert(not jit.status())")) return 1;
#endif
    if (luaL_dofile(L, argv[1])) {
        fprintf(stderr, "%s\n", lua_tostring(L,-1));
        return 1;
    }
    lua_getglobal(L, argv[3]);
    int fn = luaL_ref(L, LUA_REGISTRYINDEX);
    for (int i = 0; i < 32; ++i) checked_call(L, fn, i%4, want[i%4]);
#ifdef CHECK_LUAJIT
    if (!jit_off && luaL_dostring(L, "assert(jit.status()); assert(require('jit.util').traceinfo(1))")) {
        fprintf(stderr, "expected active JIT traces: %s\n", lua_tostring(L, -1));
        return 1;
    }
#endif
    double start = seconds();
    for (int i = 0; i < count; ++i) checked_call(L, fn, i%4, want[i%4]);
    double elapsed = seconds() - start;
    printf("%s %d %.1f ns/op\n", argv[3], count, elapsed*1e9/count);
    lua_close(L);
    return 0;
}
