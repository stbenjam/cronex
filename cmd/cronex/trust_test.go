package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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
