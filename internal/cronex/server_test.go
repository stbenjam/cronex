package cronex

import (
	"context"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPClientToolLifecycle(t *testing.T) {
	s, _ := testStore(t)
	server := NewServer(s, "")
	st, ct := mcp.NewInMemoryTransports()
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ss, err := server.Connect(timeout, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	cs, err := client.Connect(timeout, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	listed, err := cs.ListTools(timeout, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 3 {
		t.Fatalf("tools: %+v", listed.Tools)
	}
	names := map[string]bool{}
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}
	for _, name := range []string{"CronCreate", "CronList", "CronDelete"} {
		if !names[name] {
			t.Fatal("missing", name)
		}
	}
	call := func(name string, args any) *mcp.CallToolResult {
		t.Helper()
		r, err := cs.CallTool(timeout, &mcp.CallToolParams{Name: name, Arguments: args, Meta: mcp.Meta{"threadId": "A"}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := call("CronCreate", map[string]any{"prompt": "check PR", "every_seconds": 600, "name": "dynamic"})
	if r.IsError {
		t.Fatalf("create: %+v", r)
	}
	// Only a root SessionStart (or explicit manual binding) registers ownership.
	unregistered, err := cs.CallTool(timeout, &mcp.CallToolParams{Name: "CronCreate", Arguments: map[string]any{"prompt": "unsupported", "every_seconds": 60}, Meta: mcp.Meta{"threadId": "unregistered-child"}})
	if err != nil || !unregistered.IsError {
		t.Fatalf("unregistered session accepted: %+v %v", unregistered, err)
	}
	jobs, err := s.List(ctx, "A", time.Now())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("created: %+v %v", jobs, err)
	}
	if r = call("CronList", map[string]any{}); r.IsError {
		t.Fatalf("list: %+v", r)
	}
	if r = call("CronDelete", map[string]any{"id": jobs[0].ID}); r.IsError {
		t.Fatalf("delete: %+v", r)
	}
	jobs, _ = s.List(ctx, "A", time.Now())
	if len(jobs) != 0 {
		t.Fatal("delete retained job")
	}
	if r = call("CronCreate", map[string]any{"prompt": "bad", "cron": "invalid"}); !r.IsError {
		t.Fatal("invalid cron accepted")
	}
	if r = call("CronCreate", map[string]any{"prompt": "bad", "every_seconds": "ten"}); !r.IsError {
		t.Fatal("invalid argument type accepted")
	}
	// Missing identity must fail closed, even when the server already handled A.
	r, err = cs.CallTool(timeout, &mcp.CallToolParams{Name: "CronList", Arguments: map[string]any{}})
	if err != nil || !r.IsError {
		t.Fatalf("missing identity accepted: %+v %v", r, err)
	}
	r = call("CronCreate", map[string]any{"prompt": "private to A", "every_seconds": 600})
	if r.IsError {
		t.Fatal(r)
	}
	jobs, _ = s.List(ctx, "A", time.Now())
	r, err = cs.CallTool(timeout, &mcp.CallToolParams{Name: "CronDelete", Arguments: map[string]any{"id": jobs[0].ID}, Meta: mcp.Meta{"threadId": "B"}})
	if err != nil || r.IsError {
		t.Fatalf("B delete: %+v %v", r, err)
	}
	jobs, _ = s.List(ctx, "A", time.Now())
	if len(jobs) != 1 {
		t.Fatal("B deleted A's job through the shared MCP connection")
	}
}
