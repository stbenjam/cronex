package cronex

import (
	"testing"
	"time"
)

func TestSchedules(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input CreateInput
		want  time.Time
	}{
		{"ten minutes", CreateInput{Prompt: "p", EverySeconds: 600}, epoch.Add(10 * time.Minute)},
		{"watcher", CreateInput{Prompt: "p", EverySeconds: 28800}, epoch.Add(8 * time.Hour)},
		{"cron", CreateInput{Prompt: "p", Cron: "*/10 * * * *"}, epoch.Add(10 * time.Minute)},
		{"timezone", CreateInput{Prompt: "p", Cron: "0 9 * * *", Timezone: "America/New_York"}, epoch.Add(time.Hour)},
		{"one-shot", CreateInput{Prompt: "p", At: "2026-09-28T01:00:00+02:00"}, time.Date(2026, 9, 27, 23, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j, err := newJob("A", tc.input, epoch)
			if err != nil || !j.NextRunAt.Equal(tc.want) {
				t.Fatalf("%v %v; want %v", j.NextRunAt, err, tc.want)
			}
		})
	}
}

func TestInvalidSchedules(t *testing.T) {
	yes := true
	for _, in := range []CreateInput{
		{Prompt: "p"}, {Prompt: "  ", EverySeconds: 1}, {Prompt: "p", EverySeconds: -1},
		{Prompt: "p", EverySeconds: 31536001}, {Prompt: "p", EverySeconds: 1, Cron: "* * * * *"},
		{Prompt: "p", Cron: "0 * * * * *"}, {Prompt: "p", Cron: "* * * * *", Timezone: "bad zone"},
		{Prompt: "p", Cron: "0 0 30 2 *"}, {Prompt: "p", Cron: "@every 1m"},
		{Prompt: "p", At: "2020-01-01T00:00:00Z"}, {Prompt: "p", At: "2027-01-01T00:00:00"},
		{Prompt: "p", At: "2027-01-01T00:00:00Z", Recurring: &yes},
		{Prompt: "p", EverySeconds: 1, ExpiresAt: epoch.Format(time.RFC3339)},
		{Prompt: "p", EverySeconds: 1, Timezone: "UTC"},
	} {
		if _, err := newJob("A", in, epoch); err == nil {
			t.Errorf("accepted %+v", in)
		}
	}
}

func TestDSTAndWeeklongWatcher(t *testing.T) {
	before := time.Date(2026, 3, 7, 15, 0, 0, 0, time.UTC)
	j, err := newJob("A", CreateInput{Prompt: "p", Cron: "0 9 * * *", Timezone: "America/New_York"}, before)
	want := time.Date(2026, 3, 8, 13, 0, 0, 0, time.UTC)
	if err != nil || !j.NextRunAt.Equal(want) {
		t.Fatalf("DST: %v %v", j.NextRunAt, err)
	}
	j, err = newJob("A", CreateInput{Prompt: "p", EverySeconds: 28800, ExpiresAt: epoch.Add(7 * 24 * time.Hour).Format(time.RFC3339)}, epoch)
	if err != nil {
		t.Fatal(err)
	}
	// Unlike ephemeral three-day cron implementations, the watcher lives a week.
	next, err := j.Next(epoch.Add(6 * 24 * time.Hour))
	if err != nil || !next.Equal(epoch.Add(6*24*time.Hour+8*time.Hour)) {
		t.Fatalf("week: %v %v", next, err)
	}
}
