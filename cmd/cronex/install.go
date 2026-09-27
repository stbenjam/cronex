package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/pelletier/go-toml/v2/unstable"
	"github.com/stbenjam/cronex/internal/cronex"
	"golang.org/x/sys/unix"
)

const beginMarker = "# BEGIN CRONEX MANAGED CONFIG\n"
const endMarker = "# END CRONEX MANAGED CONFIG\n"

// Codex may append trust tables inside our marker comments. Keep their bytes
// intact when replacing hook definitions; Codex still validates their hashes.
func hookStateTables(block []byte) ([]byte, error) {
	var parser unstable.Parser
	parser.Reset(block)
	var saved []byte
	start, keep := 0, false
	for parser.NextExpression() {
		node := parser.Expression()
		if node.Kind != unstable.Table && node.Kind != unstable.ArrayTable {
			continue
		}
		key := node.Key()
		var parts []string
		position := 0
		for key.Next() {
			if len(parts) == 0 {
				position = bytes.LastIndexByte(block[:key.Node().Raw.Offset], '\n') + 1
			}
			parts = append(parts, string(key.Node().Data))
		}
		if keep {
			saved = append(saved, block[start:position]...)
		}
		start = position
		keep = len(parts) >= 2 && parts[0] == "hooks" && parts[1] == "state"
	}
	if keep {
		saved = append(saved, block[start:]...)
	}
	return saved, parser.Error()
}

func hookState(doc map[string]any) any {
	hooks, _ := doc["hooks"].(map[string]any)
	return hooks["state"]
}

// Keep the user's comments and formatting byte-for-byte. Only replace our
// delimited section. Validate the complete TOML before any file is changed.
func mergeConfig(original, fragment []byte) ([]byte, error) {
	var parsed map[string]any
	if err := toml.Unmarshal(original, &parsed); err != nil {
		return nil, fmt.Errorf("existing config is invalid TOML: %w", err)
	}
	state := hookState(parsed)
	source := string(original)
	block := beginMarker + string(fragment) + endMarker
	start, end := strings.Index(source, beginMarker), strings.Index(source, endMarker)
	var candidate string
	if start >= 0 || end >= 0 {
		if start < 0 || end < start || strings.Count(source, beginMarker) != 1 || strings.Count(source, endMarker) != 1 {
			return nil, errors.New("incomplete or duplicate Cronex managed markers; repair config.toml before installing")
		}
		preserved, err := hookStateTables(original[start+len(beginMarker) : end])
		if err != nil {
			return nil, fmt.Errorf("read managed hook trust: %w", err)
		}
		block = beginMarker + string(fragment) + string(preserved) + endMarker
		candidate = source[:start] + block + source[end+len(endMarker):]
	} else {
		if servers, ok := parsed["mcp_servers"].(map[string]any); ok && servers["cronex"] != nil {
			return nil, errors.New("mcp_servers.cronex already exists outside a managed block; remove the old Cronex server/hooks before installing")
		}
		candidate = source
		if candidate != "" && !strings.HasSuffix(candidate, "\n") {
			candidate += "\n"
		}
		candidate += "\n" + block
	}
	parsed = nil
	if err := toml.Unmarshal([]byte(candidate), &parsed); err != nil {
		return nil, fmt.Errorf("merged config is invalid TOML (no changes made): %w", err)
	}
	if !reflect.DeepEqual(state, hookState(parsed)) {
		return nil, errors.New("could not preserve existing hooks.state (no changes made)")
	}
	return []byte(candidate), nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".cronex-install-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if err := f.Chmod(mode); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func install(home, db, codex string, out io.Writer) error {
	home, err := filepath.Abs(home)
	if err != nil {
		return err
	}
	db, err = filepath.Abs(db)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(home, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(home, ".cronex-install.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("another Cronex installation is running: %w", err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	configPath := filepath.Join(home, "config.toml")
	// Preserve a user's symlink and update its actual target atomically.
	if resolved, err := filepath.EvalSymlinks(configPath); err == nil {
		configPath = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if info, statErr := os.Lstat(configPath); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("config.toml is a dangling symlink; create its target before installing")
	}
	original, err := os.ReadFile(configPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	mode := os.FileMode(0600)
	if info, err := os.Stat(configPath); err == nil {
		mode = info.Mode().Perm()
	}
	destination := filepath.Join(home, "cronex", "bin", "cronex")
	var fragment bytes.Buffer
	if err := configFor(&fragment, destination, db, codex); err != nil {
		return err
	}
	updated, err := mergeConfig(original, fragment.Bytes())
	if err != nil {
		return err
	}
	source, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return err
	}
	// Upgrade persistent state before replacing the executable: already-running
	// Codex sessions can invoke the new hook binary immediately after the rename.
	store, err := cronex.Open(db, true)
	if err != nil {
		return fmt.Errorf("initialize cron database: %w", err)
	}
	if err := store.Close(); err != nil {
		return err
	}
	if err := atomicWrite(destination, binary, 0700); err != nil {
		return err
	}
	if !bytes.Equal(original, updated) {
		if len(original) > 0 {
			backup := configPath + ".cronex-backup-" + time.Now().UTC().Format("20060102T150405.000000000Z")
			if err := atomicWrite(backup, original, 0600); err != nil {
				return err
			}
			fmt.Fprintln(out, "Backed up configuration to", backup)
		}
		if err := atomicWrite(configPath, updated, mode); err != nil {
			return err
		}
	}
	fmt.Fprintln(out, "Installed", destination)
	fmt.Fprintln(out, "Configured MCP server and lifecycle hooks in", configPath)
	return nil
}
