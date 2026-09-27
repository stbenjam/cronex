package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sys/unix"
)

const beginMarker = "# BEGIN CRONEX MANAGED CONFIG\n"
const endMarker = "# END CRONEX MANAGED CONFIG\n"

// Keep the user's comments and formatting byte-for-byte. Only replace our
// delimited section. Validate the complete TOML before any file is changed.
func mergeConfig(original, fragment []byte) ([]byte, error) {
	var parsed map[string]any
	if err := toml.Unmarshal(original, &parsed); err != nil {
		return nil, fmt.Errorf("existing config is invalid TOML: %w", err)
	}
	source := string(original)
	block := beginMarker + string(fragment) + endMarker
	start, end := strings.Index(source, beginMarker), strings.Index(source, endMarker)
	var candidate string
	if start >= 0 || end >= 0 {
		if start < 0 || end < start || strings.Count(source, beginMarker) != 1 || strings.Count(source, endMarker) != 1 {
			return nil, errors.New("incomplete or duplicate Cronex managed markers; repair config.toml before installing")
		}
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
	fmt.Fprintln(out, "Configured MCP server and SessionStart, UserPromptSubmit, Stop, PostToolUse, SessionEnd hooks in", configPath)
	fmt.Fprintln(out, "Restart Codex, then review and trust the Cronex hooks with /hooks.")
	return nil
}
