package cronex

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

const keepAlivePrompt = "Cronex keepalive only. No scheduled task is due in this message. Perform no action: do not use tools, check tasks, change files, or create schedules. End this turn immediately with a brief acknowledgement."

// Activity belongs to the session, not the watcher: replacing an async hook
// must not postpone the keepalive. Ignore duplicate and stale Stop events.
func (s *Store) setTurnState(ctx context.Context, session, turn string, idle bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `UPDATE session_state SET turn_id=?,idle=0 WHERE session_id=?`
	args := []any{turn, session}
	if idle {
		query = `UPDATE session_state SET idle=1 WHERE session_id=? AND turn_id=? AND idle=0`
		args = []any{session, turn}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO session_keepalive(session_id,activity_ms) VALUES (?,?)
ON CONFLICT(session_id) DO UPDATE SET activity_ms=MAX(activity_ms,excluded.activity_ms)`, session, time.Now().UnixMilli())
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Tracked pending scheduled work already provides a wakeup. Never add a
// keepalive behind it, or while a cron delivery is leased by another process.
const keepAliveEligibility = `SELECT MAX(k.activity_ms+?,k.lease_until_ms)
FROM session_keepalive k JOIN sessions s ON s.id=k.session_id
JOIN session_state t ON t.session_id=s.id
WHERE s.id=? AND s.ended=0 AND s.generation=? AND t.idle=1 AND k.accepted=0
AND EXISTS(SELECT 1 FROM jobs WHERE session_id=s.id AND (expires_ms IS NULL OR expires_ms>?))
AND NOT EXISTS(SELECT 1 FROM jobs WHERE session_id=s.id AND (expires_ms IS NULL OR expires_ms>?)
 AND (next_ms<=? OR lease_until_ms>? OR EXISTS(SELECT 1 FROM queued_deliveries WHERE job_id=jobs.id)))`

// KeepAliveWait is a read-only check used only by opted-in background watchers.
func (s *Store) KeepAliveWait(ctx context.Context, session, generation string, interval time.Duration, now time.Time) (time.Duration, bool, error) {
	if interval <= 0 {
		return 0, false, nil
	}
	var next int64
	err := s.db.QueryRowContext(ctx, keepAliveEligibility, interval.Milliseconds(), session, generation,
		now.UnixMilli(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return time.UnixMilli(next).Sub(now), err == nil, err
}

func (s *Store) ClaimKeepAlive(ctx context.Context, session, generation string, interval time.Duration, now time.Time) (string, error) {
	if interval <= 0 {
		return "", nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var next int64
	err = tx.QueryRowContext(ctx, keepAliveEligibility, interval.Milliseconds(), session, generation,
		now.UnixMilli(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && next > now.UnixMilli()) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	token := uuid.NewString()
	_, err = tx.ExecContext(ctx, `UPDATE session_keepalive SET token=?,accepted=0,lease_until_ms=? WHERE session_id=?`, token, now.Add(leaseTime).UnixMilli(), session)
	if err != nil {
		return "", err
	}
	return token, tx.Commit()
}

func (s *Store) CompleteKeepAlive(ctx context.Context, session, token string, delivered bool) error {
	query := `UPDATE session_keepalive SET accepted=1,lease_until_ms=0 WHERE session_id=? AND token=?`
	if !delivered {
		query = `UPDATE session_keepalive SET token='',accepted=0,lease_until_ms=0 WHERE session_id=? AND token=?`
	}
	_, err := s.db.ExecContext(ctx, query, session, token)
	return err
}
