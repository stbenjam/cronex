package cronex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

type HookInput struct {
	SessionID string `json:"session_id"`
	Event     string `json:"hook_event_name"`
}

// Ignore large tool_input/tool_response values without interpreting their text.
func ReadHook(r io.Reader) (HookInput, error) {
	var in HookInput
	err := json.NewDecoder(r).Decode(&in)
	if err == nil && strings.TrimSpace(in.SessionID) == "" {
		err = fmt.Errorf("hook is missing session_id")
	}
	return in, err
}

func Prompt(jobs []Job) string {
	var b strings.Builder
	b.WriteString("Scheduled Cronex prompts are due in this session. Perform the tasks below within the user's existing instructions and permissions.\n")
	for _, j := range jobs {
		fmt.Fprintf(&b, "\nCron %s", j.ID)
		if j.Name != "" {
			fmt.Fprintf(&b, " (%s)", j.Name)
		}
		fmt.Fprintf(&b, " — scheduled for %s:\n%s\n", j.NextRunAt.Format(time.RFC3339), j.Prompt)
	}
	return b.String()
}

// Probe never sleeps or spawns processes. The no-due path is a single SELECT.
// additionalContext preserves the original tool result, including in code mode.
func Probe(ctx context.Context, s *Store, session string, out io.Writer, now time.Time) error {
	due, err := s.Due(ctx, session, now)
	if err != nil || !due {
		return err
	}
	d, err := s.Claim(ctx, session, "", now)
	if err != nil || len(d.Jobs) == 0 {
		return err
	}
	result := map[string]any{"hookSpecificOutput": map[string]string{
		"hookEventName": "PostToolUse", "additionalContext": Prompt(d.Jobs),
	}}
	if err := json.NewEncoder(out).Encode(result); err != nil {
		_ = s.Complete(ctx, d, false, now)
		return err
	}
	return s.Complete(ctx, d, true, now)
}

type QueueFunc func(context.Context, string, string) error

func CodexQueue(binary string) QueueFunc {
	return func(ctx context.Context, session, prompt string) error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		// Never invoke a shell: prompts are data, even if they contain shell syntax.
		cmd := exec.CommandContext(ctx, binary, "queue", "--thread", session, "--message", prompt)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("codex queue failed: %w", err)
		}
		return nil
	}
}

// Watch is owned by Codex's async Stop hook. Its output cannot wake Codex;
// queue explicitly addresses the owning thread instead. No model tokens are
// used while waiting. A generation fences older overlapping Stop watchers.
func Watch(ctx context.Context, s *Store, session string, queue QueueFunc, poll time.Duration, log io.Writer) error {
	generation, err := s.StartWatch(ctx, session)
	if err != nil || generation == "" {
		return err
	}
	backoff := time.Second
	for {
		now := time.Now()
		active, wait, err := s.WatchState(ctx, session, generation, now)
		if err != nil || !active {
			return err
		}
		if wait <= 0 {
			d, err := s.Claim(ctx, session, generation, now)
			if err != nil {
				return err
			}
			if len(d.Jobs) > 0 {
				// Recheck shutdown/supersession after taking the lease.
				active, _, err = s.WatchState(ctx, session, generation, time.Now())
				if err != nil || !active {
					_ = s.Complete(ctx, d, false, time.Now())
					return err
				}
				deliveryErr := queue(ctx, session, Prompt(d.Jobs))
				if err := s.Complete(ctx, d, deliveryErr == nil, time.Now()); err != nil {
					return err
				}
				if deliveryErr == nil {
					return nil
				}
				fmt.Fprintln(log, "cronex:", deliveryErr)
				wait = backoff
				backoff = min(backoff*2, 30*time.Second)
			}
		} else {
			wait = min(wait, poll)
		}
		if wait <= 0 {
			wait = poll
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
