package cronex

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func idleSince(t *testing.T, s *Store, session string, at time.Time) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE session_keepalive SET activity_ms=? WHERE session_id=?`, at.UnixMilli(), session); err != nil {
		t.Fatal(err)
	}
}

func TestKeepAliveDeadlineAndLifecycle(t *testing.T) {
	s, _ := testStore(t)
	epoch := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	j, err := s.Create(ctx, "A", CreateInput{Prompt: "later", EverySeconds: 28800}, epoch)
	if err != nil {
		t.Fatal(err)
	}
	idleSince(t, s, "A", epoch)
	gen, _ := s.StartWatch(ctx, "A")
	const interval = 27 * time.Minute
	if wait, eligible, err := s.KeepAliveWait(ctx, "A", gen, interval, epoch.Add(26*time.Minute)); err != nil || !eligible || wait != time.Minute {
		t.Fatalf("deadline: %v %v %v", wait, eligible, err)
	}
	if token, err := s.ClaimKeepAlive(ctx, "A", gen, interval, epoch.Add(26*time.Minute)); token != "" || err != nil {
		t.Fatalf("early delivery: %q %v", token, err)
	}
	newGen, _ := s.StartWatch(ctx, "A")
	if token, err := s.ClaimKeepAlive(ctx, "A", gen, interval, epoch.Add(interval)); token != "" || err != nil {
		t.Fatalf("superseded watcher: %q %v", token, err)
	}
	if wait, eligible, err := s.KeepAliveWait(ctx, "A", newGen, interval, epoch.Add(interval)); err != nil || !eligible || wait != 0 {
		t.Fatalf("replacement postponed deadline: %v %v %v", wait, eligible, err)
	}
	if err := s.BeginTurn(ctx, "A", "working"); err != nil {
		t.Fatal(err)
	}
	idleSince(t, s, "A", epoch)
	if token, err := s.ClaimKeepAlive(ctx, "A", newGen, interval, epoch.Add(interval)); token != "" || err != nil {
		t.Fatalf("active delivery: %q %v", token, err)
	}
	if err := s.FinishTurn(ctx, "A", "working"); err != nil {
		t.Fatal(err)
	}
	if wait, eligible, err := s.KeepAliveWait(ctx, "A", newGen, interval, time.Now()); err != nil || !eligible || wait < interval-time.Second {
		t.Fatalf("turn did not reset timer: %v %v %v", wait, eligible, err)
	}
	idleSince(t, s, "A", epoch)
	for _, turn := range []string{"old-turn", "working"} {
		if err := s.FinishTurn(ctx, "A", turn); err != nil {
			t.Fatal(err)
		}
	}
	if wait, _, err := s.KeepAliveWait(ctx, "A", newGen, interval, epoch.Add(interval)); err != nil || wait != 0 {
		t.Fatalf("stale/duplicate stop postponed timer: %v %v", wait, err)
	}
	if _, err := s.Delete(ctx, "A", j.ID); err != nil {
		t.Fatal(err)
	}
	if token, err := s.ClaimKeepAlive(ctx, "A", newGen, interval, epoch.Add(interval)); token != "" || err != nil {
		t.Fatalf("empty session delivery: %q %v", token, err)
	}
}

func TestKeepAlivePendingAcrossRestartAndAcknowledgement(t *testing.T) {
	s, path := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "later", EverySeconds: 28800})
	idleSince(t, s, "A", epoch)
	gen, _ := s.StartWatch(ctx, "A")
	now := epoch.Add(time.Hour)
	token, err := s.ClaimKeepAlive(ctx, "A", gen, time.Minute, now)
	if err != nil || token == "" {
		t.Fatalf("claim: %q %v", token, err)
	}
	if err := s.CompleteKeepAlive(ctx, "A", token, true); err != nil {
		t.Fatal(err)
	}
	if err := s.EndSession(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimKeepAlive(ctx, "A", gen, time.Minute, now.Add(time.Hour)); err != nil || next != "" {
		t.Fatalf("ended session: %q %v", next, err)
	}
	other, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.EnsureSession(ctx, "A", true); err != nil {
		t.Fatal(err)
	}
	if err := other.BeginTurn(ctx, "A", "resume"); err != nil {
		t.Fatal(err)
	}
	if err := other.FinishTurn(ctx, "A", "resume"); err != nil {
		t.Fatal(err)
	}
	idleSince(t, other, "A", epoch)
	gen, _ = other.StartWatch(ctx, "A")
	if err := other.AcknowledgeQueue(ctx, "B", token, now); err != nil {
		t.Fatal(err)
	}
	if next, err := other.ClaimKeepAlive(ctx, "A", gen, time.Minute, now.Add(time.Hour)); err != nil || next != "" {
		t.Fatalf("pending keepalive duplicated: %q %v", next, err)
	}
	if err := other.AcknowledgeQueue(ctx, "A", token, now); err != nil {
		t.Fatal(err)
	}
	// Completion can arrive after the consumer's acknowledgement.
	if err := other.CompleteKeepAlive(ctx, "A", token, true); err != nil {
		t.Fatal(err)
	}
	if next, err := other.ClaimKeepAlive(ctx, "A", gen, time.Minute, now.Add(time.Hour)); err != nil || next == "" || next == token {
		t.Fatalf("acknowledged keepalive still blocked: %q %v", next, err)
	}
}

func TestKeepAliveConcurrentClaimsAndRetry(t *testing.T) {
	s, path := testStore(t)
	create(t, s, "A", CreateInput{Prompt: "later", EverySeconds: 28800})
	idleSince(t, s, "A", epoch)
	gen, _ := s.StartWatch(ctx, "A")
	now := epoch.Add(time.Hour)
	var wg sync.WaitGroup
	results := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			other, err := Open(path, false)
			if err != nil {
				t.Error(err)
				return
			}
			defer other.Close()
			token, err := other.ClaimKeepAlive(ctx, "A", gen, time.Minute, now)
			if err != nil {
				t.Error(err)
			}
			if token != "" {
				results <- token
			}
		})
	}
	wg.Wait()
	close(results)
	if len(results) != 1 {
		t.Fatalf("%d competing claims", len(results))
	}
	first := <-results
	if err := s.CompleteKeepAlive(ctx, "A", first, false); err != nil {
		t.Fatal(err)
	}
	second, err := s.ClaimKeepAlive(ctx, "A", gen, time.Minute, now)
	if err != nil || second == "" {
		t.Fatalf("failure retry: %q %v", second, err)
	}
	third, err := s.ClaimKeepAlive(ctx, "A", gen, time.Minute, now.Add(leaseTime))
	if err != nil || third == "" || third == second {
		t.Fatalf("crash recovery: %q %v", third, err)
	}
	if err := s.CompleteKeepAlive(ctx, "A", second, true); err != nil {
		t.Fatal(err)
	}
	var stored string
	var accepted bool
	if err := s.db.QueryRow(`SELECT token,accepted FROM session_keepalive WHERE session_id='A'`).Scan(&stored, &accepted); err != nil || stored != third || accepted {
		t.Fatalf("stale completion: %q %v %v", stored, accepted, err)
	}
}

func TestKeepAliveSkipsDuePendingExpiredAndDisabled(t *testing.T) {
	for _, condition := range []string{"due", "leased", "pending", "expired", "disabled"} {
		t.Run(condition, func(t *testing.T) {
			s, _ := testStore(t)
			input := CreateInput{Prompt: "task", EverySeconds: 28800}
			if condition == "due" || condition == "leased" || condition == "pending" {
				input.EverySeconds = 60
			}
			if condition == "expired" {
				expiry := epoch.Add(30 * time.Minute)
				input.EverySeconds = 60
				input.ExpiresAt = expiry.Format(time.RFC3339)
			}
			create(t, s, "A", input)
			idleSince(t, s, "A", epoch)
			gen, _ := s.StartWatch(ctx, "A")
			now := epoch.Add(time.Hour)
			if condition == "pending" || condition == "leased" {
				d, err := s.Claim(ctx, "A", gen, now)
				if err != nil || len(d.Jobs) != 1 {
					t.Fatalf("cron claim: %+v %v", d, err)
				}
				if condition == "pending" {
					if err := s.Complete(ctx, d, true, now); err != nil {
						t.Fatal(err)
					}
				}
			}
			interval := time.Minute
			if condition == "disabled" {
				interval = 0
			}
			if token, err := s.ClaimKeepAlive(ctx, "A", gen, interval, now); err != nil || token != "" {
				t.Fatalf("unexpected keepalive: %q %v", token, err)
			}
		})
	}
}

func TestWatcherKeepAliveAndCronPriority(t *testing.T) {
	for _, due := range []bool{false, true} {
		s, _ := testStore(t)
		now := time.Now()
		created := now
		if due {
			created = now.Add(-9 * time.Hour)
		}
		j, err := s.Create(ctx, "A", CreateInput{Prompt: "real task", EverySeconds: 28800}, created)
		if err != nil {
			t.Fatal(err)
		}
		idleSince(t, s, "A", now.Add(-time.Hour))
		deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
		var calls int
		err = Watch(deadline, s, "A", func(_ context.Context, session, prompt string) error {
			calls++
			if session != "A" || strings.Contains(prompt, keepAlivePrompt) == due || strings.Contains(prompt, "real task") != due {
				t.Errorf("wrong delivery: %s %s", session, prompt)
			}
			if calls == 1 {
				return errors.New("temporary queue failure")
			}
			return nil
		}, time.Millisecond, time.Minute, io.Discard)
		cancel()
		if err != nil || calls != 2 {
			t.Fatalf("watch: %d %v", calls, err)
		}
		jobs, err := s.List(ctx, "A", now)
		if err != nil || len(jobs) != 1 || (!due && !jobs[0].NextRunAt.Equal(j.NextRunAt)) {
			t.Fatalf("keepalive changed schedule: %+v %v", jobs, err)
		}
	}
}
