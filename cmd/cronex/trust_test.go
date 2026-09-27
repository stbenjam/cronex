package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Like the npm Codex launcher, this fixture has a child that inherits its
// streams. Killing just the launcher used to leave install stuck in Wait.
func TestHookDiscoveryReapsLauncherChildren(t *testing.T) {
	for _, response := range []string{"rpc-error", "timeout"} {
		t.Run(response, func(t *testing.T) {
			home := t.TempDir()
			if err := os.WriteFile(filepath.Join(home, "config.toml"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			script := "#!/bin/sh\nsleep 60 &\n"
			if response == "rpc-error" {
				script += "printf '%s\\n' '{\"id\":1,\"error\":{\"message\":\"fixture failure\"}}'\n"
			}
			script += "wait\n"
			launcher := filepath.Join(home, "codex")
			if err := os.WriteFile(launcher, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := trustInstalledHooks(ctx, home, filepath.Join(home, "jobs.sqlite3"), "codex", launcher, io.Discard)
			if err == nil || (response == "rpc-error" && !strings.Contains(err.Error(), "fixture failure")) ||
				(response == "timeout" && !strings.Contains(err.Error(), "deadline exceeded")) {
				t.Fatalf("unexpected discovery error: %v", err)
			}
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("launcher cleanup took %s", elapsed)
			}
		})
	}
}

func TestInstalledHookSelection(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, nil, 0600); err != nil {
		t.Fatal(err)
	}
	const command = "'/cronex' hook --db '/db' --codex 'codex'"
	var hooks []hookMetadata
	for _, event := range []string{"sessionStart", "subagentStart", "userPromptSubmit", "stop", "interrupt", "postToolUse", "sessionEnd"} {
		h := hookMetadata{SourcePath: config, Key: config + ":" + event, EventName: event, HandlerType: "command", Command: command, CurrentHash: "sha256:exact-definition"}
		hooks = append(hooks, h)
		if event == "stop" || event == "userPromptSubmit" {
			h.Async, h.Key, h.Command = true, h.Key+":watch", command+" --watch"
			hooks = append(hooks, h)
		}
	}
	makeList := func(items []hookMetadata) hooksList {
		data, _ := json.Marshal(map[string]any{"data": []any{map[string]any{"hooks": items, "errors": []any{}}}})
		var list hooksList
		if err := json.Unmarshal(data, &list); err != nil {
			t.Fatal(err)
		}
		return list
	}
	unrelated := hooks[0]
	unrelated.Command += " && echo unrelated"
	project := hooks[0]
	project.SourcePath = filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(project.SourcePath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	selected, err := installedHooks(makeList(append(append([]hookMetadata{}, hooks...), unrelated, project)), config, command)
	if err != nil || len(selected) != 9 {
		t.Fatalf("selection: %+v %v", selected, err)
	}
	for _, items := range [][]hookMetadata{hooks[:8], append(append([]hookMetadata{}, hooks...), hooks[0])} {
		if _, err := installedHooks(makeList(items), config, command); err == nil {
			t.Fatal("accepted incomplete or ambiguous hook set")
		}
	}
}
