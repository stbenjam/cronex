package cronex

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
