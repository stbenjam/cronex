package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stbenjam/cronex/internal/cronex"
)

func TestRootLifecycleAndSubagentExclusion(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "db")
	hook := func(event, agent, turn string) string {
		t.Helper()
		data, _ := json.Marshal(cronex.HookInput{SessionID: "parent", AgentID: agent, TurnID: turn, Event: event})
		var out bytes.Buffer
		if err := run(ctx, []string{"hook", "--db", db}, bytes.NewReader(data), &out, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	hook("SessionStart", "", "")
	s, err := cronex.Open(db, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hook("UserPromptSubmit", "", "current")
	gen, err := s.StartWatch(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	oneShot := false
	if _, err := s.Create(ctx, "parent", cronex.CreateInput{Prompt: "root scheduled task", EverySeconds: 1, Recurring: &oneShot}, time.Now().Add(-2*time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "PostToolUse", "Stop", "SessionEnd"} {
		if out := hook(event, "child", "child-turn"); out != "" {
			t.Fatalf("child hook output: %s", out)
		}
	}
	jobs, err := s.List(ctx, "parent", time.Now())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("child hook changed scheduled work: %+v %v", jobs, err)
	}
	if out := hook("PostToolUse", "", "current"); !strings.Contains(out, "root scheduled task") {
		t.Fatalf("root did not receive scheduled work: %s", out)
	}
	jobs, err = s.List(ctx, "parent", time.Now())
	if err != nil || len(jobs) != 0 {
		t.Fatalf("root delivery did not acknowledge job: %+v %v", jobs, err)
	}
	if ready, err := s.CanDeliver(ctx, "parent", gen); err != nil || ready {
		t.Fatalf("child changed parent lifecycle: %v %v", ready, err)
	}
	hook("SubagentStart", "child", "current")
	if _, err := s.Create(ctx, "child", cronex.CreateInput{Prompt: "unsupported", EverySeconds: 60}, time.Now()); err == nil {
		t.Fatal("child scheduling accepted")
	}
	hook("Stop", "", "old-turn")
	if ready, _ := s.CanDeliver(ctx, "parent", gen); ready {
		t.Fatal("late stop enabled delivery")
	}
	hook("Interrupt", "", "current")
	if ready, err := s.CanDeliver(ctx, "parent", gen); err != nil || !ready {
		t.Fatalf("interrupt lost observer: %v %v", ready, err)
	}
	if _, err := s.Create(ctx, "parent", cronex.CreateInput{Prompt: "registered", EverySeconds: 60}, time.Now()); err != nil {
		t.Fatal(err)
	}
	hook("SessionEnd", "", "")
	jobs, err = s.List(ctx, "parent", time.Now())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("SessionEnd lost persistent jobs: %+v %v", jobs, err)
	}
	if ready, err := s.CanDeliver(ctx, "parent", gen); err != nil || ready {
		t.Fatalf("SessionEnd left watcher running: %v %v", ready, err)
	}
	hook("SessionStart", "", "")
	jobs, err = s.List(ctx, "parent", time.Now())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("SessionStart lost persistent jobs: %+v %v", jobs, err)
	}

	// Legacy child schedules still get removed, without deleting parent jobs.
	if err := s.EnsureSession(ctx, "child", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "child", cronex.CreateInput{Prompt: "legacy", EverySeconds: 60}, time.Now()); err != nil {
		t.Fatal(err)
	}
	hook("SubagentStart", "child", "current")
	jobs, err = s.List(ctx, "child", time.Now())
	if err != nil || len(jobs) != 0 {
		t.Fatalf("legacy child jobs retained: %+v %v", jobs, err)
	}
}
