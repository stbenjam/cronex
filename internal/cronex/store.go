package cronex

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

const leaseTime = 30 * time.Second

type Store struct{ db *sql.DB }

// Open does no DDL on hook paths. The MCP server/SessionStart initializes once.
func Open(path string, initialize bool) (*Store, error) {
	if initialize {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
	} else if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(500)", "foreign_keys(1)"}, "_txlock": {"immediate"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if initialize {
		_, err = db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS sessions (
 id TEXT PRIMARY KEY, ended INTEGER NOT NULL DEFAULT 0, generation TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
 body TEXT NOT NULL, next_ms INTEGER NOT NULL, expires_ms INTEGER,
 lease TEXT NOT NULL DEFAULT '', lease_until_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs(session_id, next_ms);`)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// EnsureSession never revives an ended session; only SessionStart may do that.
func (s *Store) EnsureSession(ctx context.Context, id string, restart bool) error {
	if id == "" {
		return errors.New("missing Codex session ID")
	}
	query := `INSERT INTO sessions(id) VALUES (?) ON CONFLICT(id) DO NOTHING`
	if restart {
		query = `INSERT INTO sessions(id) VALUES (?) ON CONFLICT(id) DO UPDATE SET ended=0, generation=''`
	}
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

func (s *Store) Create(ctx context.Context, session string, in CreateInput, now time.Time) (Job, error) {
	j, err := newJob(session, in, now)
	if err != nil {
		return j, err
	}
	j.ID = uuid.NewString()
	body, err := json.Marshal(j)
	if err != nil {
		return j, err
	}
	var expiry any
	if j.ExpiresAt != nil {
		expiry = j.ExpiresAt.UnixMilli()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO jobs(id,session_id,body,next_ms,expires_ms)
SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM sessions WHERE id=? AND ended=0)`,
		j.ID, session, string(body), j.NextRunAt.UnixMilli(), expiry, session)
	if err != nil {
		return j, err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		err = errors.New("session has ended or is not registered")
	}
	return j, err
}

func (s *Store) List(ctx context.Context, session string, now time.Time) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM jobs WHERE session_id=? AND (expires_ms IS NULL OR expires_ms>?) ORDER BY next_ms,id`, session, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var j Job
		if err := json.Unmarshal([]byte(body), &j); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *Store) Delete(ctx context.Context, session, id string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE session_id=? AND id=?`, session, id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n > 0, err
}

func (s *Store) EndSession(ctx context.Context, session string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO sessions(id,ended) VALUES (?,1) ON CONFLICT(id) DO UPDATE SET ended=1,generation=''`, session); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE session_id=?`, session); err != nil {
		return err
	}
	return tx.Commit()
}

// A new user turn hands delivery back to PostToolUse. Otherwise an old idle
// watcher could queue a due job behind a long turn instead of injecting it.
func (s *Store) SuspendWatch(ctx context.Context, session string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET generation='' WHERE id=?`, session)
	return err
}

// Due is the entire PostToolUse fast path: an indexed read, no write locks.
func (s *Store) Due(ctx context.Context, session string, now time.Time) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE session_id=? AND next_ms<=? AND lease_until_ms<=? AND (expires_ms IS NULL OR expires_ms>?))`, session, now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&exists)
	return exists, err
}

type Delivery struct {
	Token string
	Jobs  []Job
}

// Claim serializes Stop/PostToolUse delivery across processes. Failed deliveries
// release their lease; killed processes leave a lease that expires automatically.
func (s *Store) Claim(ctx context.Context, session, generation string, now time.Time) (Delivery, error) {
	d := Delivery{Token: uuid.NewString()}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	var ended bool
	var current string
	if err = tx.QueryRowContext(ctx, `SELECT ended,generation FROM sessions WHERE id=?`, session).Scan(&ended, &current); err != nil {
		return d, err
	}
	if ended || (generation != "" && current != generation) {
		return d, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT body FROM jobs WHERE session_id=? AND next_ms<=? AND lease_until_ms<=? AND (expires_ms IS NULL OR expires_ms>?) ORDER BY next_ms,id LIMIT 8`, session, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return d, err
	}
	for rows.Next() {
		var body string
		if err = rows.Scan(&body); err != nil {
			break
		}
		var j Job
		if err = json.Unmarshal([]byte(body), &j); err != nil {
			break
		}
		d.Jobs = append(d.Jobs, j)
	}
	rowErr := rows.Err()
	_ = rows.Close()
	if err != nil {
		return d, err
	}
	if rowErr != nil {
		return d, rowErr
	}
	for _, j := range d.Jobs {
		if _, err = tx.ExecContext(ctx, `UPDATE jobs SET lease=?,lease_until_ms=? WHERE id=? AND session_id=?`, d.Token, now.Add(leaseTime).UnixMilli(), j.ID, session); err != nil {
			return d, err
		}
	}
	return d, tx.Commit()
}

func (s *Store) Complete(ctx context.Context, d Delivery, delivered bool, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, j := range d.Jobs {
		if !delivered {
			_, err = tx.ExecContext(ctx, `UPDATE jobs SET lease='',lease_until_ms=0 WHERE id=? AND session_id=? AND lease=?`, j.ID, j.SessionID, d.Token)
		} else {
			next, nextErr := j.Next(now)
			if nextErr != nil {
				return nextErr
			}
			if next.IsZero() || (j.ExpiresAt != nil && !next.Before(*j.ExpiresAt)) {
				_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id=? AND session_id=? AND lease=?`, j.ID, j.SessionID, d.Token)
			} else {
				j.NextRunAt = next
				body, marshalErr := json.Marshal(j)
				if marshalErr != nil {
					return marshalErr
				}
				_, err = tx.ExecContext(ctx, `UPDATE jobs SET body=?,next_ms=?,lease='',lease_until_ms=0 WHERE id=? AND session_id=? AND lease=?`, string(body), next.UnixMilli(), j.ID, j.SessionID, d.Token)
			}
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) StartWatch(ctx context.Context, session string) (string, error) {
	generation := uuid.NewString()
	r, err := s.db.ExecContext(ctx, `UPDATE sessions SET generation=? WHERE id=? AND ended=0`, generation, session)
	if err != nil {
		return "", err
	}
	n, err := r.RowsAffected()
	if err == nil && n == 0 {
		return "", nil
	}
	return generation, err
}

func (s *Store) WatchState(ctx context.Context, session, generation string, now time.Time) (bool, time.Duration, error) {
	var active bool
	var next sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT ended=0 AND generation=?,
 (SELECT MIN(MAX(next_ms,lease_until_ms)) FROM jobs WHERE session_id=? AND (expires_ms IS NULL OR expires_ms>?))
 FROM sessions WHERE id=?`, generation, session, now.UnixMilli(), session).Scan(&active, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("watch state: %w", err)
	}
	if !next.Valid {
		return false, 0, nil
	}
	return active, time.UnixMilli(next.Int64).Sub(now), nil
}
