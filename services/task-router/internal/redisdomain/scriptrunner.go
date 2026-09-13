package redisdomain

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptsFS embed.FS

// scripts caches the loaded source of every embedded Lua script, keyed by
// base filename without extension (e.g. "create_agent"), and the
// *redis.Script wrapper that handles EVALSHA-with-fallback-to-EVAL
// transparently per script per client.
type scriptSet struct {
	mu      sync.Mutex
	sources map[string]string
	loaded  map[*redis.Client]map[string]*redis.Script
}

var globalScripts = &scriptSet{
	sources: mustLoadScripts(),
	loaded:  make(map[*redis.Client]map[string]*redis.Script),
}

func mustLoadScripts() map[string]string {
	entries, err := fs.ReadDir(scriptsFS, "scripts")
	if err != nil {
		panic(fmt.Sprintf("redisdomain: reading embedded scripts: %v", err))
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lua") {
			continue
		}
		data, err := scriptsFS.ReadFile("scripts/" + e.Name())
		if err != nil {
			panic(fmt.Sprintf("redisdomain: reading embedded script %s: %v", e.Name(), err))
		}
		name := strings.TrimSuffix(e.Name(), ".lua")
		out[name] = string(data)
	}
	return out
}

// script returns the *redis.Script for the given embedded script name
// (e.g. "create_agent"), lazily wrapping it for the given client the
// first time it's requested.
func (s *scriptSet) script(client *redis.Client, name string) *redis.Script {
	s.mu.Lock()
	defer s.mu.Unlock()

	src, ok := s.sources[name]
	if !ok {
		panic(fmt.Sprintf("redisdomain: unknown embedded script %q", name))
	}
	perClient, ok := s.loaded[client]
	if !ok {
		perClient = make(map[string]*redis.Script)
		s.loaded[client] = perClient
	}
	sc, ok := perClient[name]
	if !ok {
		sc = redis.NewScript(src)
		perClient[name] = sc
	}
	return sc
}

// runScript executes the named embedded Lua script against client with
// the given keys/args, transparently using EVALSHA with EVAL fallback
// (handled by redis.Script.Run).
func runScript(ctx context.Context, client *redis.Client, name string, keys []string, args ...any) (any, error) {
	sc := globalScripts.script(client, name)
	res, err := sc.Run(ctx, client, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdomain: script %s: %w", name, err)
	}
	return res, nil
}
