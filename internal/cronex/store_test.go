package cronex

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var epoch = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
var ctx = context.Background()

func testStore(t testing.TB) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state", "jobs.sqlite3")
	s, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for _, id := range []string{"A", "B"} {
		if err := s.EnsureSession(ctx, id, false); err != nil {
			t.Fatal(err)
		}
		if err := s.BeginTurn(ctx, id, "initial"); err != nil {
			t.Fatal(err)
		}
		if err := s.FinishTurn(ctx, id, "initial"); err != nil {
			t.Fatal(err)
		}
	}
	return s, path
}

func create(t *testing.T, s *Store, session string, in CreateInput) Job {
	t.Helper()
	j, err := s.Create(ctx, session, in, epoch)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestSessionIsolationAndSuspension(t *testing.T) {
	s, _ := testStore(t)
	a := create(t, s, "A", CreateInput{Prompt: "A", EverySeconds: 600})
	b := create(t, s, "B", CreateInput{Prompt: "B", EverySeconds: 600})
	if deleted, err := s.Delete(ctx, "B", a.ID); deleted || err != nil {
		t.Fatalf("cross-session delete: %v %v", deleted, err)
	}
	jobs, err := s.List(ctx, "A", epoch)
	if err != nil || len(jobs) != 1 || jobs[0].ID != a.ID {
		t.Fatalf("list: %+v %v", jobs, err)
	}
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSession(ctx, "A", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "A", CreateInput{Prompt: "late create", EverySeconds: 1}, epoch); err == nil {
		t.Fatal("late MCP request resurrected ended session")
	}
	jobs, err = s.List(ctx, "A", epoch)
	if err != nil || len(jobs) != 1 || jobs[0].ID != a.ID {
		t.Fatalf("suspension lost job: %+v %v", jobs, err)
	}
	jobs, err = s.List(ctx, "B", epoch)
	if err != nil || len(jobs) != 1 || jobs[0].ID != b.ID {
		t.Fatalf("other session: %+v %v", jobs, err)
	}
	if err := s.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	create(t, s, "A", CreateInput{Prompt: "resumed", EverySeconds: 600})
}

func TestClaimsSerializeAcrossConnections(t *testing.T) {
	s, path := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "once", EverySeconds: 1})
	var wg sync.WaitGroup
	results := make(chan Delivery, 12)
	for range 12 {
		wg.Go(func() {
			other, err := Open(path, false)
			if err != nil {
				t.Error(err)
				return
			}
			defer other.Close()
			d, err := other.Claim(ctx, "A", "", epoch.Add(time.Second))
			if err != nil {
				t.Error(err)
				return
			}
			results <- d
		})
	}
	wg.Wait()
	close(results)
	count := 0
	for d := range results {
		count += len(d.Jobs)
	}
	if count != 1 {
		t.Fatalf("claimed %d copies", count)
	}
}

func TestDeliveryRetryLeaseAndCoalescing(t *testing.T) {
	s, _ := testStore(t)
	j := create(t, s, "A", CreateInput{Prompt: "watch", EverySeconds: 600})
	now := epoch.Add(25 * time.Minute)
	d, err := s.Claim(ctx, "A", "", now)
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("claim: %+v %v", d, err)
	}
	if due, err := s.Due(ctx, "A", now); err != nil || due {
		t.Fatalf("leased job was due: %v %v", due, err)
	}
	// A process crash leaves a lease that another hook may recover.
	newer, err := s.Claim(ctx, "A", "", now.Add(leaseTime))
	if err != nil || len(newer.Jobs) != 1 {
		t.Fatalf("lease recovery: %+v %v", newer, err)
	}
	if err := s.Complete(ctx, d, true, now.Add(leaseTime)); err != nil {
		t.Fatal(err)
	}
	if due, _ := s.Due(ctx, "A", now.Add(leaseTime)); due {
		t.Fatal("old completion stole new lease")
	}
	if err := s.Complete(ctx, newer, false, now.Add(leaseTime)); err != nil {
		t.Fatal(err)
	}
	retry, err := s.Claim(ctx, "A", "", now.Add(leaseTime))
	if err != nil || len(retry.Jobs) != 1 {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	if err := s.Complete(ctx, retry, true, now.Add(leaseTime)); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(ctx, "A", now)
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID || !jobs[0].NextRunAt.Equal(epoch.Add(30*time.Minute)) {
		t.Fatalf("coalesce: %+v %v", jobs, err)
	}
}

func TestOneShotExpirationAndDeletionDuringDelivery(t *testing.T) {
	s, _ := testStore(t)
	no := false
	create(t, s, "A", CreateInput{Prompt: "one-shot", EverySeconds: 1, Recurring: &no})
	d, err := s.Claim(ctx, "A", "", epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, d, true, epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, _ := s.List(ctx, "A", epoch)
	if len(jobs) != 0 {
		t.Fatal("one-shot retained")
	}
	j := create(t, s, "A", CreateInput{Prompt: "expired", EverySeconds: 1, ExpiresAt: epoch.Add(2 * time.Second).Format(time.RFC3339)})
	if due, err := s.Due(ctx, "A", epoch.Add(2*time.Second)); due || err != nil {
		t.Fatalf("expired due: %v %v", due, err)
	}
	jobs, _ = s.List(ctx, "A", epoch.Add(2*time.Second))
	if len(jobs) != 0 {
		t.Fatal("expired listed")
	}
	if _, err := s.Delete(ctx, "A", j.ID); err != nil {
		t.Fatal(err)
	}
	j = create(t, s, "A", CreateInput{Prompt: "deleted", EverySeconds: 1})
	d, err = s.Claim(ctx, "A", "", epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(ctx, "A", j.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, d, true, epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, _ = s.List(ctx, "A", epoch)
	if len(jobs) != 0 {
		t.Fatal("completion resurrected deleted job")
	}
}

func TestWatcherGenerationAndResume(t *testing.T) {
	s, path := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "watcher", EverySeconds: 28800})
	old, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.StartWatch(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := s.WatchState(ctx, "A", old, epoch)
	if active || err != nil {
		t.Fatalf("old watcher active: %v %v", active, err)
	}
	d, err := s.Claim(ctx, "A", old, epoch.Add(8*time.Hour))
	if err != nil || len(d.Jobs) != 0 {
		t.Fatalf("old watcher claimed: %+v %v", d, err)
	}
	active, wait, err := s.WatchState(ctx, "A", current, epoch)
	if !active || wait != 8*time.Hour || err != nil {
		t.Fatalf("watch state: %v %v %v", active, wait, err)
	}
	if err := s.BeginTurn(ctx, "A", "next"); err != nil {
		t.Fatal(err)
	}
	active, _, err = s.WatchState(ctx, "A", current, epoch)
	if !active || err != nil {
		t.Fatalf("active-turn handoff killed watcher: %v %v", active, err)
	}
	d, err = s.Claim(ctx, "A", current, epoch.Add(8*time.Hour))
	if err != nil || len(d.Jobs) != 0 {
		t.Fatalf("suspended watcher claimed: %+v %v", d, err)
	}
	d, err = s.Claim(ctx, "A", "", epoch.Add(8*time.Hour))
	if err != nil || len(d.Jobs) != 1 {
		t.Fatalf("active-turn probe could not claim: %+v %v", d, err)
	}
	if err := s.Complete(ctx, d, false, epoch.Add(8*time.Hour)); err != nil {
		t.Fatal(err)
	}
	other, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	jobs, err := other.List(ctx, "A", epoch)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("restart lost jobs: %+v %v", jobs, err)
	}
	if err := other.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	jobs, err = other.List(ctx, "A", epoch)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("compaction lost jobs: %+v %v", jobs, err)
	}
}

func BenchmarkNotDue(b *testing.B) {
	s, _ := testStore(b)
	if _, err := s.Create(ctx, "A", CreateInput{Prompt: "later", EverySeconds: 28800}, epoch); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.Due(ctx, "A", epoch); err != nil {
			b.Fatal(err)
		}
	}
}
