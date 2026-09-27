package cronex

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestShutdownAndResumeCoalescesMissedRuns(t *testing.T) {
	s, path := testStore(t)
	oneShot := false
	for _, in := range []CreateInput{
		{Prompt: "interval", EverySeconds: 60},
		{Prompt: "calendar", Cron: "* * * * *"},
		{Prompt: "one-shot", EverySeconds: 60, Recurring: &oneShot},
		{Prompt: "expired", EverySeconds: 60, ExpiresAt: epoch.Add(2 * time.Minute).Format(time.RFC3339)},
	} {
		create(t, s, "A", in)
	}
	before, err := s.List(ctx, "A", epoch)
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	// Repeated shutdown, stale turn hooks, and an MCP reconnect cannot revive it.
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSession(ctx, "A", false); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishTurn(ctx, "A", "initial"); err != nil {
		t.Fatal(err)
	}
	if generation, err := s.StartWatch(ctx, "A"); err != nil || generation != "" {
		t.Fatalf("closed session started watcher: %q %v", generation, err)
	}
	resumedAt := epoch.Add(48 * time.Hour)
	for _, generation := range []string{old, ""} {
		d, err := s.Claim(ctx, "A", generation, resumedAt)
		if err != nil || len(d.Jobs) != 0 {
			t.Fatalf("closed session delivered: %+v %v", d, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err := s.List(ctx, "A", epoch)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("restart changed schedules: %+v %v", after, err)
	}
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTurn(ctx, "A", "resumed-turn"); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishTurn(ctx, "A", "resumed-turn"); err != nil {
		t.Fatal(err)
	}
	current, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := s.CanDeliver(ctx, "A", old); err != nil || ready {
		t.Fatalf("old watcher survived resume: %v %v", ready, err)
	}
	d, err := s.Claim(ctx, "A", current, resumedAt)
	if err != nil || len(d.Jobs) != 3 {
		t.Fatalf("want one delivery per unexpired task, got %+v %v", d, err)
	}
	seen := map[string]bool{}
	for _, j := range d.Jobs {
		if seen[j.ID] || j.Prompt == "expired" {
			t.Fatalf("duplicate or expired task: %+v", j)
		}
		seen[j.ID] = true
	}
	if err := s.Complete(ctx, d, true, resumedAt); err != nil {
		t.Fatal(err)
	}
	if next, err := s.Claim(ctx, "A", current, resumedAt); err != nil || len(next.Jobs) != 0 {
		t.Fatalf("missed intervals replayed: %+v %v", next, err)
	}
	jobs, err := s.List(ctx, "A", resumedAt)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("one-shot retained or recurring job lost: %+v %v", jobs, err)
	}
	for _, j := range jobs {
		if !j.NextRunAt.Equal(resumedAt.Add(time.Minute)) {
			t.Fatalf("schedule did not advance past downtime: %+v", j)
		}
	}
}

func TestSuspensionKeepsInFlightLease(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "pending", EverySeconds: 1})
	now := epoch.Add(time.Second)
	d, err := s.Claim(ctx, "A", "", now)
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("claim: %+v %v", d, err)
	}
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := s.Claim(ctx, "A", "", now); err != nil || len(duplicate.Jobs) != 0 {
		t.Fatalf("resume cleared an in-flight lease: %+v %v", duplicate, err)
	}
	if retry, err := s.Claim(ctx, "A", "", now.Add(leaseTime)); err != nil || len(retry.Jobs) != 1 {
		t.Fatalf("abandoned lease did not recover: %+v %v", retry, err)
	}
}

func TestPendingQueueSurvivesResumeWithoutAnotherCatchUp(t *testing.T) {
	s, path := testStore(t)
	j := create(t, s, "A", CreateInput{Prompt: "queued", EverySeconds: 60})
	generation, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Claim(ctx, "A", generation, epoch.Add(time.Minute))
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("claim: %+v %v", d, err)
	}
	if err := s.Complete(ctx, d, true, epoch.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTurn(ctx, "A", "resume"); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishTurn(ctx, "A", "resume"); err != nil {
		t.Fatal(err)
	}
	generation, err = s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	now := epoch.Add(48 * time.Hour)
	if err := s.AcknowledgeQueue(ctx, "B", d.Token, now); err != nil {
		t.Fatal(err)
	}
	if due, err := s.Due(ctx, "A", now); err != nil || due {
		t.Fatalf("already queued job was due: %v %v", due, err)
	}
	var out bytes.Buffer
	if err := Probe(ctx, s, "A", &out, now); err != nil || out.Len() != 0 {
		t.Fatalf("probe duplicated queued prompt: %s %v", out.String(), err)
	}
	if next, err := s.Claim(ctx, "A", generation, now); err != nil || len(next.Jobs) != 0 {
		t.Fatalf("watcher duplicated queued prompt: %+v %v", next, err)
	}
	if err := s.AcknowledgeQueue(ctx, "A", d.Token, now); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(ctx, "A", now)
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID || !jobs[0].NextRunAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("queued prompt did not cover missed ticks: %+v %v", jobs, err)
	}
	if due, err := s.Due(ctx, "A", now); err != nil || due {
		t.Fatalf("catch-up repeated after queue consumption: %v %v", due, err)
	}
	if due, err := s.Due(ctx, "A", now.Add(time.Minute)); err != nil || !due {
		t.Fatalf("future recurrence lost: %v %v", due, err)
	}
	// A duplicate acknowledgement must not skip a later legitimate recurrence.
	if err := s.AcknowledgeQueue(ctx, "A", d.Token, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if due, err := s.Due(ctx, "A", now.Add(2*time.Minute)); err != nil || !due {
		t.Fatalf("stale acknowledgement changed recurrence: %v %v", due, err)
	}
}

func TestQueueAcknowledgementBeforeAcceptanceIsRecorded(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "fast consumer", EverySeconds: 60})
	generation, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	now := epoch.Add(time.Minute)
	d, err := s.Claim(ctx, "A", generation, now)
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("claim: %+v %v", d, err)
	}
	if err := s.AcknowledgeQueue(ctx, "A", d.Token, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, d, true, now); err != nil {
		t.Fatal(err)
	}
	if due, err := s.Due(ctx, "A", now.Add(time.Minute)); err != nil || !due {
		t.Fatalf("fast acknowledgement left job pending forever: %v %v", due, err)
	}
}

func TestUnconfirmedQueueRecoversAfterCrash(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "crashed before queue", EverySeconds: 1})
	generation, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	now := epoch.Add(time.Second)
	d, err := s.Claim(ctx, "A", generation, now)
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("claim: %+v %v", d, err)
	}
	// No Complete: the process died. An unconfirmed queue intent must not stick.
	retry, err := s.Claim(ctx, "A", generation, now.Add(leaseTime))
	if err != nil || len(retry.Jobs) != 1 || retry.Token == d.Token {
		t.Fatalf("abandoned queue intent did not recover: %+v %v", retry, err)
	}
	if err := s.Complete(ctx, d, true, now.Add(leaseTime)); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, retry, false, now.Add(leaseTime)); err != nil {
		t.Fatal(err)
	}
	if due, err := s.Due(ctx, "A", now.Add(leaseTime)); err != nil || !due {
		t.Fatalf("old completion or failed queue blocked retry: %v %v", due, err)
	}
}

func TestDelayedStopCannotEnableIdleDelivery(t *testing.T) {
	s, _ := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "scheduled", EverySeconds: 1})
	if err := s.BeginTurn(ctx, "A", "new-turn"); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishTurn(ctx, "A", "initial"); err != nil {
		t.Fatal(err)
	}
	// A delayed observer may register, but cannot change the active turn.
	generation, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Claim(ctx, "A", generation, epoch.Add(time.Second))
	if err != nil || len(d.Jobs) != 0 {
		t.Fatalf("active turn claimed: %+v %v", d, err)
	}
	var out bytes.Buffer
	if err := Probe(ctx, s, "A", &out, epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "scheduled") {
		t.Fatal("active probe missed job")
	}
}

func TestInterruptAndCompactionRetainWatcher(t *testing.T) {
	s, _ := testStore(t)
	generation, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTurn(ctx, "A", "interrupted"); err != nil {
		t.Fatal(err)
	}
	// Compaction must preserve the active turn and its existing observer.
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.CanDeliver(ctx, "A", generation); ready || err != nil {
		t.Fatalf("active: %v %v", ready, err)
	}
	create(t, s, "A", CreateInput{Prompt: "after interruption", EverySeconds: 1})
	if err := s.FinishTurn(ctx, "A", "interrupted"); err != nil {
		t.Fatal(err)
	}
	d, err := s.Claim(ctx, "A", generation, epoch.Add(time.Second))
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("interrupted turn lost delivery: %+v %v", d, err)
	}
}

func TestCreatePrunesOnlyOwningSessionsExpiredRows(t *testing.T) {
	s, _ := testStore(t)
	for _, id := range []string{"A", "B"} {
		create(t, s, id, CreateInput{Prompt: "expires", EverySeconds: 1, ExpiresAt: epoch.Add(2 * time.Second).Format(time.RFC3339)})
	}
	if _, err := s.Create(ctx, "A", CreateInput{Prompt: "new", EverySeconds: 1}, epoch.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("rows: %d %v", count, err)
	}
}

func TestExistingDatabaseUpgradeKeepsJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE sessions(id TEXT PRIMARY KEY, ended INTEGER NOT NULL DEFAULT 0, generation TEXT NOT NULL DEFAULT ''); INSERT INTO sessions(id) VALUES ('A'); CREATE TABLE jobs(id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), body TEXT NOT NULL, next_ms INTEGER NOT NULL, expires_ms INTEGER, lease TEXT NOT NULL DEFAULT '', lease_until_ms INTEGER NOT NULL DEFAULT 0)`)
	if err != nil {
		t.Fatal(err)
	}
	legacy := Job{ID: "legacy-job", SessionID: "A", Prompt: "preserve me", NextRunAt: epoch.Add(time.Hour), Timezone: "UTC"}
	body, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs(id,session_id,body,next_ms) VALUES (?,?,?,?)`, legacy.ID, legacy.SessionID, string(body), legacy.NextRunAt.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	jobs, err := s.List(ctx, "A", epoch)
	if err != nil || len(jobs) != 1 || jobs[0].ID != legacy.ID || jobs[0].Prompt != legacy.Prompt {
		t.Fatalf("migration changed job: %+v %v", jobs, err)
	}
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "A", CreateInput{Prompt: "registered", EverySeconds: 1}, epoch); err != nil {
		t.Fatal(err)
	}
}
