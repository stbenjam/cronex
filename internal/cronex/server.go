package cronex

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ListOutput struct {
	Jobs []Job `json:"jobs"`
}
type DeleteInput struct {
	ID string `json:"id" jsonschema:"ID of the cron to delete from this session."`
}
type DeleteOutput struct {
	Deleted bool `json:"deleted"`
}

// requestSession uses Codex's per-call routing metadata, not the MCP connection
// ID. A connection can be shared by threads. bound is for manual clients only.
func requestSession(req *mcp.CallToolRequest, bound string) (string, error) {
	session, _ := req.Params.Meta["threadId"].(string)
	if session == "" {
		session = bound
	}
	if strings.TrimSpace(session) == "" {
		return "", errors.New("missing Codex _meta.threadId; use a compatible Codex client or serve --session for manual testing")
	}
	if bound != "" && session != bound {
		return "", errors.New("request threadId does not match the explicitly bound session")
	}
	return session, nil
}

func NewServer(s *Store, bound string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "cronex", Version: "0.1.0"}, &mcp.ServerOptions{
		Instructions: "Cronex schedules prompts in this root Codex session only. Subagent scheduling is unsupported; ask the parent agent to schedule work. Use CronCreate, CronList, and CronDelete for recurring or one-shot tasks. After scheduling, finish your turn normally: the async Stop hook waits and wakes this session. Do not poll or sleep to wait for crons. During active work PostToolUse delivers due prompts. Jobs survive MCP restarts and compaction, and are deleted on SessionEnd. For PR loops use a named dynamic interval plus a separate recurring 28800-second watcher; retain the original start/deadline in the prompt and delete both jobs at termination. These tools schedule work; they do not authorize actions beyond the user's instructions.",
	})
	no := false
	mcp.AddTool(server, &mcp.Tool{Name: "CronCreate", Description: "Schedule a prompt for this session. Choose cron (five fields), every_seconds (relative interval), or at (one-shot RFC3339 time). Recurs by default except at. No automatic three-day expiry; optionally set expires_at.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &no, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, in CreateInput) (*mcp.CallToolResult, Job, error) {
			session, err := requestSession(req, bound)
			if err != nil {
				return nil, Job{}, err
			}
			if bound != "" {
				if err := s.EnsureSession(ctx, session, false); err != nil {
					return nil, Job{}, err
				}
			}
			j, err := s.Create(ctx, session, in, time.Now())
			return nil, j, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "CronList", Description: "List this session's unexpired jobs, including IDs, prompts, recurrence, and next run times. Use names/prompts to find an existing loop before creating another.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, ListOutput, error) {
			session, err := requestSession(req, bound)
			if err != nil {
				return nil, ListOutput{}, err
			}
			jobs, err := s.List(ctx, session, time.Now())
			return nil, ListOutput{Jobs: jobs}, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "CronDelete", Description: "Delete a cron from this session. Returns deleted=false for an absent ID or a job belonging to another session. Idempotent; already delivered prompts cannot be recalled.", Annotations: &mcp.ToolAnnotations{IdempotentHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, in DeleteInput) (*mcp.CallToolResult, DeleteOutput, error) {
			session, err := requestSession(req, bound)
			if err != nil {
				return nil, DeleteOutput{}, err
			}
			deleted, err := s.Delete(ctx, session, in.ID)
			return nil, DeleteOutput{Deleted: deleted}, err
		})
	return server
}
