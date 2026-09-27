// Real Codex host + local fake model. No API key, API usage, or user config edits.
// Requires Node >=22, codex on PATH, and `make build`.
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import http from 'node:http';
import net from 'node:net';
import { once } from 'node:events';

const binary = resolve('bin/cronex');
const root = mkdtempSync(join(tmpdir(), 'cronex-smoke-'));
const env = { ...process.env, CODEX_HOME: root };
delete env.CODEX_SESSION_ID;
delete env.CODEX_THREAD_ID;
const workspace = join(root, 'workspace');
mkdirSync(workspace);
const requests = [];
const notifications = [];
let app, ws, stderr = '';
const model = http.createServer(async (req, res) => {
  let body = '';
  for await (const chunk of req) body += chunk;
  if (!req.url.endsWith('/responses')) {
    res.writeHead(404).end(); return;
  }
  requests.push(JSON.parse(body));
  const id = `resp_${requests.length}`;
  const input = requests.at(-1).input;
  const probe = JSON.stringify(input).includes('CRONEX_POST_TOOL_TURN') &&
    !input.some(item => item.type === 'function_call_output' && item.call_id === 'cronex_probe_tool');
  const message = probe ? { id: `fc_${requests.length}`, type: 'function_call', name: 'exec_command',
    call_id: 'cronex_probe_tool', arguments: JSON.stringify({ cmd: 'sleep 2', yield_time_ms: 10000 }), status: 'completed' } :
    { id: `msg_${requests.length}`, type: 'message', role: 'assistant', status: 'completed',
    phase: 'final_answer', content: [{ type: 'output_text', text: 'Smoke test turn complete.', annotations: [] }] };
  const response = { id, object: 'response', model: 'test-model', status: 'completed', output: [message],
    usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } };
  res.writeHead(200, { 'Content-Type': 'text/event-stream' });
  let sequence_number = 0;
  const event = (type, data) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: sequence_number++, ...data })}\n\n`);
  event('response.created', { response: { ...response, status: 'in_progress', output: [] } });
  event('response.output_item.added', { output_index: 0, item: { ...message, status: 'in_progress' } });
  if (!probe) event('response.output_text.delta', { item_id: message.id, output_index: 0, content_index: 0, delta: 'Smoke test turn complete.' });
  event('response.output_item.done', { output_index: 0, item: message });
  event('response.completed', { response });
  res.end();
});

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(test, description, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await test()) return;
    await sleep(50);
  }
  throw new Error(`Timed out: ${description}`);
}

try {
  model.listen(0, '127.0.0.1'); await once(model, 'listening');
  const reservation = net.createServer();
  reservation.listen(0, '127.0.0.1'); await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const remote = `ws://127.0.0.1:${port}`;
  const wrapper = join(root, 'queue-codex');
  writeFileSync(wrapper, `#!/bin/sh\nexec codex "$@" --remote '${remote}'\n`, { mode: 0o700 });
  const fragment = execFileSync(binary, ['config', '--db', join(root, 'jobs.sqlite3'), '--codex', wrapper], { encoding: 'utf8', env });
  writeFileSync(join(root, 'config.toml'), `model = "test-model"\nmodel_provider = "mock"\n` +
    `[model_providers.mock]\nname = "Local smoke fixture"\nbase_url = "http://127.0.0.1:${model.address().port}/v1"\n` +
    `wire_api = "responses"\nrequires_openai_auth = false\nsupports_websockets = false\n\n` + fragment);
  app = spawn('codex', ['--dangerously-bypass-hook-trust', 'app-server', '--listen', remote], { env, cwd: workspace, stdio: ['ignore', 'pipe', 'pipe'] });
  app.stderr.on('data', chunk => { stderr += chunk; });
  app.stdout.on('data', () => {});
  await until(async () => {
    const candidate = new WebSocket(remote);
    const opened = await new Promise(resolve => {
      candidate.addEventListener('open', () => resolve(true), { once: true });
      candidate.addEventListener('error', () => resolve(false), { once: true });
    });
    if (opened) ws = candidate;
    return opened;
  }, 'app-server listener');
  let nextID = 1;
  const pending = new Map();
  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(data);
    if (message.id !== undefined && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id); pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error))); else resolve(message.result);
    } else { notifications.push(message); }
  });
  const rpc = (method, params) => new Promise((resolve, reject) => {
    const id = nextID++;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`RPC timeout: ${method}`)); }, 15000);
    pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } });
    ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
  });
  await rpc('initialize', { clientInfo: { name: 'cronex-smoke', version: '1' }, capabilities: { experimentalApi: true } });
  // Only trust the generated hooks in this isolated fixture, never user hooks.
  const a = (await rpc('thread/start', { cwd: workspace, config: { bypass_hook_trust: true } })).thread.id;
  const b = (await rpc('thread/start', { cwd: workspace, config: { bypass_hook_trust: true } })).thread.id;
  async function call(threadId, tool, args = {}) {
    const result = await rpc('mcpServer/tool/call', { threadId, server: 'cronex', tool, arguments: args });
    assert.ok(!result.isError, JSON.stringify(result));
    return result.structuredContent;
  }
  const job = await call(a, 'CronCreate', { prompt: 'CRONEX_SMOKE_WAKE_A', every_seconds: 2, recurring: false });
  assert.equal(job.session_id, a);
  assert.equal((await call(b, 'CronList')).jobs.length, 0);
  assert.equal((await call(b, 'CronDelete', { id: job.id })).deleted, false);
  await rpc('turn/start', { threadId: a, input: [{ type: 'text', text: 'Finish this smoke test turn.' }] });
  await until(() => requests.some(r => JSON.stringify(r.input).includes('CRONEX_SMOKE_WAKE_A')), 'async Stop to queue to model wakeup', 20000);
  assert.equal((await call(a, 'CronList')).jobs.length, 0, 'one-shot is acknowledged');
  assert.ok(notifications.some(n => n.method === 'hook/started'), 'Codex ran hooks');
  // B has never stopped, so it has no idle watcher: only PostToolUse can deliver.
  await call(b, 'CronCreate', { prompt: 'CRONEX_SMOKE_PROBE_B', every_seconds: 1, recurring: false });
  await rpc('turn/start', { threadId: b, input: [{ type: 'text', text: 'CRONEX_POST_TOOL_TURN: run the smoke test command.' }] });
  await until(() => requests.some(r => JSON.stringify(r.input).includes('CRONEX_SMOKE_PROBE_B')), 'PostToolUse prompt delivery');
  const probeRequest = requests.find(r => JSON.stringify(r.input).includes('CRONEX_SMOKE_PROBE_B'));
  assert.ok(probeRequest.input.some(item => item.type === 'function_call_output' && item.call_id === 'cronex_probe_tool'), 'original tool output preserved');
  assert.equal((await call(b, 'CronList')).jobs.length, 0, 'probe acknowledged the one-shot');
  const other = await call(b, 'CronCreate', { prompt: 'Keep B', every_seconds: 28800 });
  await call(a, 'CronCreate', { prompt: 'Clean up A', every_seconds: 28800 });
  await rpc('thread/archive', { threadId: a });
  // SessionEnd happens as the real thread shuts down. Inspect our own scratch DB.
  const query = `import sqlite3,json,sys\nc=sqlite3.connect(sys.argv[1]);print(json.dumps(c.execute('SELECT session_id,id FROM jobs').fetchall()))`;
  await until(() => {
    const rows = JSON.parse(execFileSync('python3', ['-c', query, join(root, 'jobs.sqlite3')], { encoding: 'utf8' }));
    return rows.length === 1 && rows[0][0] === b && rows[0][1] === other.id;
  }, 'SessionEnd cleanup');
  console.log('PASS: real Codex MCP metadata, session isolation, async Stop, queue wakeup, PostToolUse delivery, one-shot acknowledgement, SessionEnd cleanup.');
} catch (error) {
  console.error(error);
  console.error('Codex stderr:', stderr.slice(-6000));
  console.error('Recent notifications:', JSON.stringify(notifications.slice(-12), null, 2));
  process.exitCode = 1;
} finally {
  ws?.close();
  if (app && app.exitCode === null) { app.kill('SIGTERM'); await Promise.race([once(app, 'exit'), sleep(3000)]); if (app.exitCode === null) app.kill('SIGKILL'); }
  model.closeAllConnections(); model.close();
  rmSync(root, { recursive: true, force: true });
}
