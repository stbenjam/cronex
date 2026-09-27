package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type hookMetadata struct {
	Key         string `json:"key"`
	SourcePath  string `json:"sourcePath"`
	HandlerType string `json:"handlerType"`
	EventName   string `json:"eventName"`
	Command     string `json:"command"`
	Async       bool   `json:"async"`
	CurrentHash string `json:"currentHash"`
	TrustStatus string `json:"trustStatus"`
}

type hooksList struct {
	Data []struct {
		Hooks  []hookMetadata    `json:"hooks"`
		Errors []json.RawMessage `json:"errors"`
	} `json:"data"`
}

// Select only the installed commands from the user's config file, never a
// project/plugin hook or an unrelated command that happens to mention Cronex.
func installedHooks(list hooksList, configPath, command string) ([]hookMetadata, error) {
	want := map[string]bool{
		"sessionStart": false, "subagentStart": false, "userPromptSubmit": false,
		"userPromptSubmit:watch": false, "stop": false, "stop:watch": false,
		"interrupt": false, "postToolUse": false, "sessionEnd": false,
	}
	var selected []hookMetadata
	for _, entry := range list.Data {
		if len(entry.Errors) != 0 {
			return nil, fmt.Errorf("Codex reported errors loading hooks")
		}
		for _, hook := range entry.Hooks {
			source, err := filepath.EvalSymlinks(hook.SourcePath)
			if err != nil || source != configPath || hook.HandlerType != "command" {
				continue
			}
			key, expectedCommand := hook.EventName, command
			if hook.Async {
				key += ":watch"
				expectedCommand += " --watch"
			}
			seen, ok := want[key]
			if !ok || hook.Command != expectedCommand {
				continue
			}
			if seen || hook.Key == "" || hook.CurrentHash == "" {
				return nil, fmt.Errorf("ambiguous or incomplete Cronex hook metadata for %s", key)
			}
			want[key] = true
			selected = append(selected, hook)
		}
	}
	if len(selected) != len(want) {
		return nil, fmt.Errorf("Codex discovered %d of %d installed Cronex hooks", len(selected), len(want))
	}
	return selected, nil
}

// Ask the local Codex runtime for hashes rather than duplicating its hashing
// algorithm. This short-lived stdio server starts no threads or model turns.
func trustInstalledHooks(ctx context.Context, home, db, queueBinary, codex string, out io.Writer) error {
	home, err := filepath.Abs(home)
	if err != nil {
		return err
	}
	db, err = filepath.Abs(db)
	if err != nil {
		return err
	}
	configPath, err := filepath.EvalSymlinks(filepath.Join(home, "config.toml"))
	if err != nil {
		return err
	}
	before, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	command := shellQuote(filepath.Join(home, "cronex", "bin", "cronex")) + " hook --db " + shellQuote(db) + " --codex " + shellQuote(queueBinary)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, codex, "app-server", "--stdio")
	// The npm launcher spawns a native child which inherits its streams. Own
	// the whole process group so neither success nor cancellation leaves that
	// child holding a pipe open after the launcher exits.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Dir = home // Avoid loading an unrelated repository's project config.
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "CODEX_HOME=") {
			cmd.Env = append(cmd.Env, variable)
		}
	}
	cmd.Env = append(cmd.Env, "CODEX_HOME="+home)
	cmd.Stderr = io.Discard
	input, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	killGroup := func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.Cancel = func() error {
		// Unblock a synchronous RPC read even if a descendant has detached.
		_ = output.Close()
		return killGroup()
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start Codex hook discovery: %w", err)
	}
	defer func() {
		_ = input.Close()
		_ = killGroup()
		_ = cmd.Wait()
	}()
	encoder, decoder := json.NewEncoder(input), json.NewDecoder(output)
	id := 0
	rpc := func(method string, params, result any) error {
		id++
		if err := encoder.Encode(map[string]any{"id": id, "method": method, "params": params}); err != nil {
			return err
		}
		for {
			var response struct {
				ID     json.RawMessage `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := decoder.Decode(&response); err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("%s: %w", method, ctx.Err())
				}
				return fmt.Errorf("%s: %w", method, err)
			}
			if string(response.ID) != strconv.Itoa(id) {
				continue
			}
			if response.Error != nil {
				return fmt.Errorf("%s: %s", method, response.Error.Message)
			}
			if result == nil {
				return nil
			}
			return json.Unmarshal(response.Result, result)
		}
	}
	if err := rpc("initialize", map[string]any{"clientInfo": map[string]string{"name": "cronex-install", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
		return err
	}
	if err := encoder.Encode(map[string]any{"method": "initialized"}); err != nil {
		return err
	}
	var list hooksList
	if err := rpc("hooks/list", map[string]any{"cwds": []string{home}}, &list); err != nil {
		return err
	}
	hooks, err := installedHooks(list, configPath, command)
	if err != nil {
		return err
	}
	var edits []map[string]any
	for _, hook := range hooks {
		if hook.TrustStatus != "trusted" {
			edits = append(edits, map[string]any{"keyPath": "hooks.state." + strconv.Quote(hook.Key) + ".trusted_hash", "value": hook.CurrentHash, "mergeStrategy": "upsert"})
		}
	}
	if len(edits) > 0 {
		current, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, before) {
			return fmt.Errorf("config changed during hook discovery; rerun install")
		}
		if err := rpc("config/batchWrite", map[string]any{"filePath": configPath, "edits": edits}, nil); err != nil {
			return err
		}
		if err := rpc("hooks/list", map[string]any{"cwds": []string{home}}, &list); err != nil {
			return err
		}
		hooks, err = installedHooks(list, configPath, command)
		if err != nil {
			return err
		}
	}
	for _, hook := range hooks {
		if hook.TrustStatus != "trusted" {
			return fmt.Errorf("Codex did not confirm trust for %s", hook.Key)
		}
	}
	fmt.Fprintf(out, "Verified trust for %d Cronex hooks in %s\n", len(hooks), configPath)
	return nil
}
