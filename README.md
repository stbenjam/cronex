# Cronex

A local stdio MCP server that schedules prompts in **the Codex session that
created them**. It exposes exactly `CronCreate`, `CronList`, and `CronDelete`.

The asynchronous `Stop` hook waits outside the model, then wakes its owning
thread with `codex queue --thread … --message …`. A synchronous `PostToolUse`
hook injects due prompts during ongoing work. `SessionEnd` preserves that
session's jobs and invalidates its watcher until the session resumes. Multiple sessions can use the same
SQLite database, even from the same directory or different worktrees.

## Setup

Requires Go 1.25+ to build, and a Codex CLI with async command hooks,
`codex queue`, and MCP `_meta.threadId`. The runtime integration was developed
against Codex CLI 0.157.1. Linux and macOS are the intended platforms.

```sh
make install
# Or install into a different Codex home:
make install CODEX_HOME=/path/to/codex-home
```

This builds and installs `$CODEX_HOME/cronex/bin/cronex`, then configures the
MCP server and lifecycle hooks in `$CODEX_HOME/config.toml`. `CODEX_HOME` defaults
to `~/.codex`. Existing settings, comments, and hooks are preserved; changed
configurations are backed up beside the original. Dangling configuration symlinks are preserved with an error asking you to create
the target first. Repeat installs update the
managed Cronex block without duplicating hooks. An existing manually configured
`mcp_servers.cronex` is left untouched with an error asking you to remove the
old Cronex entries first.

For a manual installation, `make build` then `./bin/cronex config` prints a
TOML fragment using the current binary path. The `install` and `config` commands
both accept `--db` and `--codex` overrides; `install` also accepts `--codex-home`.

Restart Codex, then open **`/hooks` and trust the new hook definitions**. Codex
skips untrusted hooks; registering only the MCP server is insufficient to wake
an idle agent. Hooks are enabled by default; ensure `features.hooks` has not
been disabled. Hook scripts must be runnable in the Codex execution environment.
Use `/mcp` to check that the Cronex server connected.

The default database is `$CODEX_HOME/cronex/crons.sqlite3`, falling back to
`~/.codex/cronex/crons.sqlite3`. You can choose a location with
`cronex config --db /absolute/path/crons.sqlite3`; the generated configuration
passes the same path to the server and every hook. The directory and database
are created with owner-only permissions. The hooks need write access to it.

`cronex config --codex /absolute/path/to/codex` chooses the executable used to
wake sessions. For a remote Codex daemon, provide a small executable wrapper
that forwards the arguments to `codex` with the correct `--remote` options.
The queue command must connect to the daemon that owns the session.

## Tools

Session ownership comes from Codex's per-call `_meta.threadId`, **not** a
model-supplied argument, the working directory, or the MCP connection ID.
Calls without that metadata fail with a clear error. `CronCreate` also requires
registration by the root `SessionStart` hook. Subagents cannot schedule crons;
they should ask the parent agent to schedule the work. Child hooks are excluded
from parent delivery, and `SubagentStart` cleans up any legacy child jobs. For manual MCP clients
only, `cronex serve --session <id>` explicitly binds a session; conflicting
metadata is rejected. No environment variable is needed for normal Codex use.

### CronCreate

Provide `prompt` and exactly one scheduling field:

| Field | Meaning | Example |
| --- | --- | --- |
| `cron` | Five-field wall-clock schedule, UTC by default | `"*/10 * * * *"` |
| `every_seconds` | Interval measured from creation, 1–31536000 seconds | `600` |
| `at` | Future, one-shot RFC3339 timestamp with timezone | `"2026-10-01T14:00:00Z"` |

Optional fields:

- `name`: label used to identify a loop, up to 200 bytes.
- `recurring`: defaults to `true` for cron/interval jobs; set `false` for one run.
  `at` is always one-shot.
- `timezone`: IANA zone for `cron`, e.g. `America/New_York`. Zone data is
  embedded in the binary. Cron supports lists, ranges, steps, and named
  weekdays/months. Seconds fields and `@every` macros are not accepted.
- `expires_at`: RFC3339 deadline, later than the first scheduled run. No
  implicit three-day expiry; jobs can support a week-long PR loop.

Returns the job, including `id`, `session_id`, `next_run_at`, and recurrence.
Prompts are limited to 16000 bytes. Stored prompts are delivered as text;
Cronex never executes them as shell commands.

### CronList

Takes `{}`. Returns `{"jobs": [...]}` for the calling session, ordered by next
run time. Includes each job's prompt and name so skills can find existing
jobs, retain a watcher, and replace a dynamic interval. Expired jobs are omitted and reclaimed when the session creates another job.

### CronDelete

Takes `{"id": "<job-id>"}`. Returns `{"deleted": true}` when removed, or
`false` if missing or owned by another session. Safe to repeat. A prompt
already sent to Codex cannot be recalled.

## PR-loop example

The [PR-loop skill](https://github.com/stbenjam/skills/blob/main/plugins/loops/skills/pr-loop/SKILL.md)
can use these three tools directly (Codex may display an MCP namespace prefix).
Use `CronList` first to avoid registering the same PR twice.

Create the initial dynamic job:

```json
{
  "name": "pr-loop owner/repo#123 dynamic",
  "every_seconds": 600,
  "prompt": "Continue $pr-loop for https://github.com/owner/repo/pull/123. Original start: 2026-09-27T14:00:00Z. Deadline: 2026-10-04T14:00:00Z. Use the existing worktree. Check PR state, CI and reviews, then replace only the dynamic job according to elapsed-time backoff. Keep the watcher. If merged or at the deadline, delete both jobs and report the result.",
  "expires_at": "2026-10-04T14:01:00Z"
}
```

Create a second job with the same prompt/deadline, name ending in `watcher`,
and `every_seconds: 28800`. Replace the example dates with the current start
and one-week deadline. The extra minute before expiry permits a scheduled run
at the deadline; expiry itself does not inject a termination prompt.

On each iteration, `CronDelete` the dynamic job and recreate it with the
appropriate interval: 600, 1800, 14400, or 28800 seconds. Keep the watcher until
termination. Include the original start, deadline, PR URL, and worktree in the
prompt so backoff state survives compaction. If termination must happen at an
exact deadline, also create an `at` job to perform final cleanup.

After scheduling, finish the turn normally. No model-driven sleep loop is
needed. If both jobs become due together, they may arrive in one prompt; check
the PR once and reconcile the jobs before returning to idle.

## Delivery and lifecycle

1. `CronCreate` persists a job under the request's Codex thread ID.
2. `Stop` and `UserPromptSubmit` each run a short synchronous lifecycle handler
   and an async watcher. The lifecycle handler records the current turn and
   whether it is active or idle; only the matching turn can mark itself idle.
   Background watchers observe this state without changing it, so delayed hooks
   cannot undo a newer turn's state. Generations retire overlapping watchers.
   A watcher stays available even with no jobs, allowing an interrupted first
   turn to recover after creating a cron. Each successful wake ends that watcher;
   the new turn arms its replacement. At most one surviving watcher per session
   checks local state once per second; no model tokens are used while waiting.
3. `PostToolUse` does one indexed SQLite due check. When nothing is due it
   writes nothing, launches no subprocesses, sleeps nowhere, and emits no
   output. Due jobs use `hookSpecificOutput.additionalContext`, preserving
   the original tool result, including in Codex code mode.
   `UserPromptSubmit` pauses idle delivery while retaining the watcher.
   `Interrupt` marks the interrupted turn idle so that watcher resumes delivery;
   its handler finishes within the host's short interrupt timeout.
4. SQLite transactions and 30-second delivery leases arbitrate overlapping
   hooks. Queue failures release the lease and retry with bounded backoff.
   Process crashes leave a recoverable lease.
5. One-shot jobs are removed after delivery. Recurring jobs advance beyond
   the current time, coalescing missed ticks into one prompt. Interval jobs
   retain their original phase; cron jobs follow their timezone's calendar
   and daylight-saving rules.
6. `SessionEnd` suspends delivery, invalidates the watcher, and clears transient
   turn state while preserving jobs and their deadlines. An ended-session marker
   prevents an in-flight MCP call from creating more jobs. `SessionStart`
   reactivates the same session; compaction preserves its active turn state.
   After a restart, resume the original thread and send a message to re-arm
   delivery. Codex 0.157.1 defers `SessionStart` until that first resumed turn;
   merely opening the conversation does not restart the watcher.
7. On resume, each unexpired overdue job produces one catch-up run, regardless
   of how many intervals were missed. Recurring jobs then advance to their next
   future deadline; overdue one-shot jobs run once and are removed. There is no
   per-tick backlog: 482 missed intervals still produce one run per job.

Practical limits:

- Requires a running Codex host/daemon able to accept `codex queue`. Async
  hook output alone does **not** start a new turn. This is a session scheduler,
  not an OS service that launches Codex after shutdown.
- Codex currently reports the same `SessionEnd` reason (`other`) for normal
  shutdown, idle unloading, archive, and deletion. Cronex therefore preserves
  schedules for all of them. Use `CronDelete` before permanently abandoning a
  thread, or set `expires_at` when scheduling. Archiving or deleting a thread
  does not cancel its stored jobs; without expiry, those rows remain until
  explicitly deleted. See the [SessionEnd contract](https://learn.chatgpt.com/docs/hooks#sessionend).
- After an interrupted turn, Codex 0.157.1 pauses consumption of queued work.
  Cronex still delivers due prompts into that queue; they execute when you
  explicitly resume queued work in Codex. Cronex does not clear the host pause.
- During active work, delivery happens at supported tool boundaries or via
  Codex's queue. A hook cannot interrupt an individual long-running model
  request or tool. Hosted tools may not emit `PostToolUse`.
- Delivery is retryable, not exactly-once execution. A crash after Codex
  accepts a prompt but before SQLite records completion can duplicate it;
  hook stdout also has no acknowledgement from the model. Make scheduled
  tasks idempotent. Deletion/session shutdown cannot recall an already queued
  or concurrently accepted prompt.
- A hard kill can skip `SessionEnd`, leaving persisted jobs. They remain
  isolated to that session ID and can be inspected/deleted upon resuming it.
- Versions that deleted jobs on `SessionEnd` cannot recover those jobs through
  an upgrade. Recreate any already-lost schedules once in their original thread.
- Hooks fail open on database errors, logging diagnostics to stderr. This
  preserves the agent's work, but a failed watcher cannot guarantee a
  wake until a later Stop or UserPromptSubmit starts its replacement. The lightweight probe retries on the next tool.
- Local session scoping prevents accidental cross-session access; it is not
  an authentication boundary against another process with access to your
  user account/database. Cronex does not expose an HTTP listener.

### Why polling?

Cronex deliberately uses a sleeping background watcher that checks SQLite about
once per second. For the intended workload of fewer than 10 crons across roughly
10 Codex sessions, this means around 10 small state queries per second when all
10 watchers are running. Polling is per session, not per cron. Waiting uses no
model tokens or API calls, and each check runs inside the existing watcher
process without launching another process.

Polling keeps newly created jobs, deletions, turn state changes, and watcher
replacement visible through one source of truth: SQLite. Filesystem notifications
could avoid periodic wakeups, but would add platform-specific behavior and races
around notification delivery and database commits. At this scale, the expected
resource savings do not justify that complexity. This is a design tradeoff,
not a measured CPU or battery guarantee.

Changes are normally noticed within about one second. The watcher sleeps for
less when a known deadline is nearer; this is not a real-time execution guarantee,
and Codex's queue and interrupt behavior still determine when the agent runs.
Event-driven waiting is worth revisiting if substantially more concurrent
sessions or measured idle resource usage make polling a problem.

## Development

```sh
make test                     # includes the Go race detector
make check                    # go vet
make smoke                    # real Codex with a local fake model, no API calls
go test ./internal/cronex -run '^$' -bench BenchmarkNotDue -benchmem
```

Tests exercise the official MCP SDK client/server lifecycle, per-call session
routing, competing database connections, lease recovery, queue failures,
suspension/resume, coalescing, expiry, timezone scheduling, hook output, and idempotent
installation. `make smoke` requires Node 22+, Python 3, and Codex on PATH. It
uses an isolated temporary Codex home and a local fake Responses endpoint to
verify actual MCP routing, idle wakeup, active-turn handoff, recurring delivery,
interruption recovery, batching, graceful host shutdown/restart, and one catch-up
run per task after simulated long downtime. It trusts only
the generated test hooks in that temporary instance.

The integration follows Codex's [hook contract](https://learn.chatgpt.com/docs/hooks).
The Go server uses the [official MCP SDK](https://github.com/modelcontextprotocol/go-sdk).
