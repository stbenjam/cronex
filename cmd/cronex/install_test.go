package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stbenjam/cronex/internal/cronex"
)

func TestInstallPreservesConfigAndIsIdempotent(t *testing.T) {
	home := filepath.Join(t.TempDir(), "Codex home with 'quotes' and $dollars")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	original := "# Keep my comments\nmodel = 'my-model'\n[mcp_servers.existing]\ncommand = 'existing'\n[[hooks.PostToolUse]]\n[[hooks.PostToolUse.hooks]]\ntype = 'command'\ncommand = 'echo existing'\n"
	path := filepath.Join(home, "config.toml")
	if err := os.WriteFile(path, []byte(original), 0640); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := install(home, filepath.Join(home, "cronex", "crons.sqlite3"), "codex", &output); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(first, []byte(original)) {
		t.Fatal("existing content changed")
	}
	var doc map[string]any
	if err := toml.Unmarshal(first, &doc); err != nil {
		t.Fatal(err)
	}
	hooks := doc["hooks"].(map[string]any)
	if len(hooks["PostToolUse"].([]any)) != 2 {
		t.Fatal("existing hook lost or duplicates added")
	}
	servers := doc["mcp_servers"].(map[string]any)
	server := servers["cronex"].(map[string]any)
	want := filepath.Join(home, "cronex", "bin", "cronex")
	if server["command"] != want {
		t.Fatalf("wrong binary path: %v", server["command"])
	}
	if info, err := os.Stat(want); err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("binary: %v %v", info, err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("config mode: %v %v", info, err)
	}
	backups, err := filepath.Glob(path + ".cronex-backup-*")
	if err != nil || len(backups) != 1 {
		t.Fatalf("backups %v %v", backups, err)
	}
	backup, _ := os.ReadFile(backups[0])
	if string(backup) != original {
		t.Fatal("backup changed contents")
	}
	if err := install(home, filepath.Join(home, "cronex", "crons.sqlite3"), "codex", &output); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(path)
	if !bytes.Equal(first, second) {
		t.Fatal("second install changed config")
	}
	backups, _ = filepath.Glob(path + ".cronex-backup-*")
	if len(backups) != 1 {
		t.Fatal("no-op install made another backup")
	}
	if err := install(home, filepath.Join(home, "new.sqlite3"), "codex", &output); err != nil {
		t.Fatal(err)
	}
	updated, _ := os.ReadFile(path)
	if bytes.Count(updated, []byte(beginMarker)) != 1 || !bytes.Contains(updated, []byte("new.sqlite3")) {
		t.Fatal("upgrade did not replace managed block")
	}
}

func TestInstallRefusesInvalidOrUnmanagedConfig(t *testing.T) {
	for _, original := range []string{"invalid [ toml", "[mcp_servers.cronex]\ncommand='manual'\n", beginMarker, endMarker} {
		home := t.TempDir()
		path := filepath.Join(home, "config.toml")
		if err := os.WriteFile(path, []byte(original), 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := install(home, filepath.Join(home, "db"), "codex", &out); err == nil {
			t.Fatalf("accepted %q", original)
		}
		actual, _ := os.ReadFile(path)
		if string(actual) != original {
			t.Fatal("modified config on error")
		}
		if _, err := os.Stat(filepath.Join(home, "cronex", "bin", "cronex")); !os.IsNotExist(err) {
			t.Fatal("installed before config validation")
		}
	}
}

func TestInstallPreservesConfigSymlink(t *testing.T) {
	home := t.TempDir()
	target := filepath.Join(t.TempDir(), "real.toml")
	if err := os.WriteFile(target, []byte("# original\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "config.toml")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := install(home, filepath.Join(home, "db"), "codex", &out); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink replaced")
	}
	data, _ := os.ReadFile(target)
	if !bytes.Contains(data, []byte(beginMarker)) {
		t.Fatal("target not updated")
	}
}

func TestInstallPreservesDanglingConfigSymlink(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "config.toml")
	target := filepath.Join(home, "missing.toml")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := install(home, filepath.Join(home, "db"), "codex", &out); err == nil {
		t.Fatal("dangling symlink accepted")
	}
	if actual, err := os.Readlink(path); err != nil || actual != target {
		t.Fatalf("symlink changed: %s %v", actual, err)
	}
}

func TestInstallPreservesSuspendedSchedules(t *testing.T) {
	home := t.TempDir()
	db := filepath.Join(home, "cronex", "crons.sqlite3")
	s, err := cronex.Open(db, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.EnsureSession(ctx, "session", true); err != nil {
		t.Fatal(err)
	}
	j, err := s.Create(ctx, "session", cronex.CreateInput{Prompt: "keep scheduled", EverySeconds: 600}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EndSession(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := install(home, db, "codex", &out); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(ctx, "session", time.Now())
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID || !jobs[0].NextRunAt.Equal(j.NextRunAt) {
		t.Fatalf("install changed schedule: %+v %v", jobs, err)
	}
	if generation, err := s.StartWatch(ctx, "session"); err != nil || generation != "" {
		t.Fatalf("install reactivated closed session: %q %v", generation, err)
	}
}

func TestMergePreservesCodexTrustInsideManagedBlock(t *testing.T) {
	var fragment bytes.Buffer
	if err := configFor(&fragment, "/cronex", "/db", "codex"); err != nil {
		t.Fatal(err)
	}
	trust := "[hooks.state]\n\n[hooks.state.\"config:stop:0:0\"]\ntrusted_hash = 'sha256:existing'\nenabled = false\n\n"
	for _, location := range []string{"before", "between", "after", "outside"} {
		t.Run(location, func(t *testing.T) {
			block, suffix := fragment.String(), ""
			switch location {
			case "before":
				block = trust + block
			case "between":
				block = strings.Replace(block, "[[hooks.Stop]]", trust+"[[hooks.Stop]]", 1)
			case "after":
				block += trust
			case "outside":
				suffix = trust
			}
			merged, err := mergeConfig([]byte(beginMarker+block+endMarker+suffix), fragment.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(merged, []byte(trust)) || bytes.Count(merged, []byte(trust)) != 1 {
				t.Fatal("trust bytes changed or duplicated")
			}
			second, err := mergeConfig(merged, fragment.Bytes())
			if err != nil || !bytes.Equal(second, merged) {
				t.Fatalf("trust-preserving reinstall not idempotent: %v", err)
			}
			updated := bytes.ReplaceAll(fragment.Bytes(), []byte("/db"), []byte("/new-db"))
			changed, err := mergeConfig(merged, updated)
			if err != nil || !bytes.Contains(changed, []byte(trust)) || !bytes.Contains(changed, []byte("/new-db")) {
				t.Fatalf("merge changed trust state before explicit approval: %v", err)
			}
		})
	}
}
