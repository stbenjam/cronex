package cronex

import (
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
)

// CreateInput accepts either a wall-clock cron, a relative interval, or a date.
type CreateInput struct {
	Prompt       string `json:"prompt" jsonschema:"Prompt to deliver to this Codex session when the job fires."`
	Cron         string `json:"cron,omitempty" jsonschema:"Five-field cron expression (minute hour day month weekday), in timezone (UTC by default)."`
	EverySeconds int64  `json:"every_seconds,omitempty" jsonschema:"Interval in seconds, measured from creation. Use for backoff, e.g. 600 or 28800. Specify exactly one of cron, every_seconds, at."`
	At           string `json:"at,omitempty" jsonschema:"One-shot RFC3339 timestamp with timezone."`
	Recurring    *bool  `json:"recurring,omitempty" jsonschema:"Defaults to true for cron and intervals. Set false to fire only once. at is always one-shot."`
	Timezone     string `json:"timezone,omitempty" jsonschema:"IANA timezone for cron expressions; defaults to UTC."`
	Name         string `json:"name,omitempty" jsonschema:"Optional descriptive name, e.g. pr-loop owner/repo#123 watcher."`
	ExpiresAt    string `json:"expires_at,omitempty" jsonschema:"Optional RFC3339 deadline after which the job stops firing."`
}

type Job struct {
	ID           string     `json:"id"`
	SessionID    string     `json:"session_id"`
	Name         string     `json:"name,omitempty"`
	Prompt       string     `json:"prompt"`
	Cron         string     `json:"cron,omitempty"`
	EverySeconds int64      `json:"every_seconds,omitempty"`
	Timezone     string     `json:"timezone"`
	Recurring    bool       `json:"recurring"`
	CreatedAt    time.Time  `json:"created_at"`
	NextRunAt    time.Time  `json:"next_run_at"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func schedule(expression, zone string) (cron.Schedule, error) {
	if len(strings.Fields(expression)) != 5 {
		return nil, errors.New("cron must have exactly five fields; use timezone separately")
	}
	if _, err := time.LoadLocation(zone); err != nil {
		return nil, fmt.Errorf("invalid timezone: %w", err)
	}
	return parser.Parse("CRON_TZ=" + zone + " " + expression)
}

func newJob(session string, in CreateInput, now time.Time) (Job, error) {
	j := Job{SessionID: session, Name: in.Name, Prompt: in.Prompt, Cron: in.Cron,
		EverySeconds: in.EverySeconds, Timezone: in.Timezone, Recurring: true, CreatedAt: now.UTC()}
	if strings.TrimSpace(in.Prompt) == "" || len(in.Prompt) > 16000 || strings.ContainsRune(in.Prompt, 0) {
		return j, errors.New("prompt must contain 1–16000 bytes of nonblank text without NUL characters")
	}
	if len(in.Name) > 200 || strings.ContainsRune(in.Name, 0) {
		return j, errors.New("name must be at most 200 bytes without NUL characters")
	}
	if j.Timezone == "" {
		j.Timezone = "UTC"
	}
	if in.Recurring != nil {
		j.Recurring = *in.Recurring
	}
	n := 0
	if in.Cron != "" {
		n++
	}
	if in.EverySeconds != 0 {
		n++
	}
	if in.At != "" {
		n++
	}
	if n != 1 {
		return j, errors.New("specify exactly one of cron, every_seconds, or at")
	}
	if in.Timezone != "" && in.Cron == "" {
		return j, errors.New("timezone applies only to cron")
	}
	switch {
	case in.Cron != "":
		s, err := schedule(in.Cron, j.Timezone)
		if err != nil {
			return j, err
		}
		j.NextRunAt = s.Next(now).UTC()
		if j.NextRunAt.IsZero() {
			return j, errors.New("cron has no next occurrence")
		}
	case in.EverySeconds != 0:
		if in.EverySeconds < 1 || in.EverySeconds > 31536000 {
			return j, errors.New("every_seconds must be between 1 and 31536000")
		}
		j.NextRunAt = now.Add(time.Duration(in.EverySeconds) * time.Second).UTC()
	default:
		if in.Recurring != nil && *in.Recurring {
			return j, errors.New("at is always one-shot")
		}
		j.Recurring = false
		var err error
		j.NextRunAt, err = time.Parse(time.RFC3339Nano, in.At)
		if err != nil || !j.NextRunAt.After(now) {
			return j, errors.New("at must be a future RFC3339 timestamp")
		}
		j.NextRunAt = j.NextRunAt.UTC()
	}
	if in.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339Nano, in.ExpiresAt)
		if err != nil || !t.After(j.NextRunAt) {
			return j, errors.New("expires_at must be an RFC3339 timestamp later than the first run")
		}
		t = t.UTC()
		j.ExpiresAt = &t
	}
	return j, nil
}

// Next coalesces missed runs and retains the original interval's phase.
func (j Job) Next(now time.Time) (time.Time, error) {
	if !j.Recurring {
		return time.Time{}, nil
	}
	if j.EverySeconds > 0 {
		step := time.Duration(j.EverySeconds) * time.Second
		return j.NextRunAt.Add((now.Sub(j.NextRunAt)/step + 1) * step), nil
	}
	s, err := schedule(j.Cron, j.Timezone)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(now).UTC(), nil
}
