package cronex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliveryTokenOnlyAcknowledgesLeadingMarker(t *testing.T) {
	const token = "63e6f670-b602-428c-919c-9f52b728f42a"
	for _, prompt := range []string{"", "ordinary user message", deliveryPrefix + "invalid", "quoted message\n" + deliveryPrefix + token} {
		if got := (HookInput{Prompt: prompt}).DeliveryToken(); got != "" {
			t.Fatalf("unrelated prompt acknowledged %q", got)
		}
	}
	if got := (HookInput{Prompt: deliveryPrefix + token + "\nscheduled task"}).DeliveryToken(); got != token {
		t.Fatalf("valid delivery was not acknowledged: %q", got)
	}
}

func TestProbePreservesResultAndIsSilentWithoutDueJobs(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "check PR", EverySeconds: 600})
	var out bytes.Buffer
	var before, after int
	if err := s.db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := Probe(ctx, s, "A", &out, epoch); err != nil || out.Len() != 0 {
		t.Fatalf("fast path: %q %v", out.String(), err)
	}
	if err := s.db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("no-due path wrote to SQLite")
	}
	if err := Probe(ctx, s, "A", &out, epoch.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["decision"]; ok {
		t.Fatal("probe must not block or replace a tool result")
	}
	if _, ok := result["continue"]; ok {
		t.Fatal("probe must not stop processing")
	}
	if !strings.Contains(out.String(), "check PR") {
		t.Fatal("missing scheduled prompt")
	}
	out.Reset()
	if err := Probe(ctx, s, "A", &out, epoch.Add(10*time.Minute)); err != nil || out.Len() != 0 {
		t.Fatal("duplicate delivery", err, out.String())
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestProbeFailedWriteReleasesJob(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "p", EverySeconds: 1})
	if err := Probe(ctx, s, "A", brokenWriter{}, epoch.Add(time.Second)); err == nil {
		t.Fatal("wanted write failure")
	}
	if due, err := s.Due(ctx, "A", epoch.Add(time.Second)); !due || err != nil {
		t.Fatalf("job lost after write failure: %v %v", due, err)
	}
}

func TestWatcherQueuesOwningSessionAndRetriesFailure(t *testing.T) {
	s, _ := testStore(t)
	for _, item := range []struct{ session, prompt string }{{"A", "watch PR"}, {"B", "other PR"}} {
		if _, err := s.Create(ctx, item.session, CreateInput{Prompt: item.prompt, EverySeconds: 28800}, time.Now().Add(-9*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	var calls int
	queue := func(_ context.Context, session, prompt string) error {
		calls++
		if session != "A" || !strings.Contains(prompt, "watch PR") || strings.Contains(prompt, "other PR") {
			t.Errorf("wrong routing: %s %s", session, prompt)
		}
		if calls == 1 {
			return errors.New("temporarily unavailable")
		}
		return nil
	}
	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := Watch(deadline, s, "A", queue, time.Millisecond, io.Discard); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("%d calls", calls)
	}
	if due, err := s.Due(ctx, "A", time.Now()); due || err != nil {
		t.Fatalf("not advanced: %v %v", due, err)
	}
	if due, err := s.Due(ctx, "B", time.Now()); !due || err != nil {
		t.Fatalf("other session changed: %v %v", due, err)
	}
}

func TestWatcherCleanupAndCancellation(t *testing.T) {
	for _, end := range []bool{true, false} {
		t.Run(map[bool]string{true: "end", false: "delete"}[end], func(t *testing.T) {
			s, _ := testStore(t)
			j, err := s.Create(ctx, "A", CreateInput{Prompt: "future", EverySeconds: 28800}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- Watch(deadline, s, "A", func(context.Context, string, string) error { calls.Add(1); return nil }, time.Millisecond, io.Discard)
			}()
			// Wait for the watcher to register before changing its state.
			for {
				var generation string
				if err := s.db.QueryRow(`SELECT generation FROM sessions WHERE id='A'`).Scan(&generation); err != nil {
					t.Fatal(err)
				}
				if generation != "" {
					break
				}
				select {
				case <-deadline.Done():
					t.Fatal("watcher never started")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			if end {
				err = s.EndSession(ctx, "A")
			} else {
				_, err = s.Delete(ctx, "A", j.ID)
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if calls.Load() != 0 {
				t.Fatal("queued after cleanup")
			}
		})
	}
}

func TestQueueTreatsPromptAsData(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "codex")
	output := filepath.Join(dir, "args")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CRONEX_TEST_ARGS\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CRONEX_TEST_ARGS", output)
	prompt := "literal $(touch SHOULD_NOT_EXIST) `date`; \"quotes\"\nsecond line"
	if err := CodexQueue(binary)(ctx, "session A", prompt); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	want := "queue\n--thread\nsession A\n--message\n" + prompt + "\n"
	if string(raw) != want {
		t.Fatalf("arguments changed: %q", raw)
	}
}

func TestQueueReportsBoundedDiagnostics(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "queue")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho 'daemon unavailable' >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	err := CodexQueue(binary)(ctx, "A", "scheduled task")
	if err == nil || !strings.Contains(err.Error(), "daemon unavailable") {
		t.Fatalf("missing diagnostic: %v", err)
	}
	var out boundedOutput
	if n, err := io.Copy(&out, strings.NewReader(strings.Repeat("x", 10000))); err != nil || n != 10000 || len(out.String()) != 4096 {
		t.Fatalf("diagnostic limit: %d %v %d", n, err, len(out.String()))
	}
}
