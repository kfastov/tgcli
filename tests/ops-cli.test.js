// The CLI commands built from the operation declarations: how options become the
// args the server gets, how bad input is reported, and what reaches stdout.

import path from 'path';
import { Command } from 'commander';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { addOperationCommands } from '../core/ops/cli.js';
import { COMMAND_GROUPS, OPERATION_LIST } from '../core/ops/index.js';

let invoke;
let stdout;
let stderr;
let logged;

function program() {
  const root = new Command()
    .name('tgcli')
    .option('--json')
    .option('--timeout <duration>')
    .option('--quiet')
    .exitOverride();
  addOperationCommands(root, { operations: OPERATION_LIST, groups: COMMAND_GROUPS, invoke });
  return root;
}

async function run(...argv) {
  await program().parseAsync(['node', 'tgcli', ...argv]);
  return invoke.mock.calls.at(-1)?.[1];
}

beforeEach(() => {
  invoke = vi.fn(async () => ({ ok: true }));
  stdout = [];
  stderr = [];
  logged = [];
  vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => stdout.push(String(chunk)));
  vi.spyOn(process.stderr, 'write').mockImplementation((chunk) => stderr.push(String(chunk)));
  vi.spyOn(console, 'log').mockImplementation((line) => logged.push(line));
  vi.spyOn(console, 'error').mockImplementation((line) => stderr.push(line));
  process.exitCode = undefined;
});

afterEach(() => {
  vi.restoreAllMocks();
  process.exitCode = undefined;
});

describe('options become args', () => {
  it('gathers a repeatable list option, splits commas, coerces numbers and lowercases a choice', async () => {
    invoke.mockResolvedValue({ source: 'live', returned: 0, hasMore: false, messages: [] });
    const call = await run('messages', 'list', '--chat', '700000001', '--chat', '700000002,700000003',
      '--limit', '5', '--source', 'LIVE', '--offset-id', '9');
    expect(call).toEqual({
      op: 'messagesList',
      args: { channelIds: ['700000001', '700000002', '700000003'], limit: 5, source: 'live', beforeId: 9 },
      invokeTimeoutMs: undefined,
    });
  });

  it('reads a negated option as the field it names', async () => {
    invoke.mockResolvedValue({ channelId: '700000001', messageId: 3 });
    const call = await run('send', 'text', '--to', '700000001', '--message', 'hi', '--no-preview', '--no-forwards');
    expect(call.args).toMatchObject({ channelId: '700000001', text: 'hi', noPreview: true, noForwards: true, retries: 2 });
  });

  it('turns a field off with an option that defaults it on', async () => {
    invoke.mockResolvedValue([]);
    expect((await run('tags', 'auto')).args.refreshMetadata).toBe(true);
    expect((await run('tags', 'auto', '--no-refresh-metadata')).args.refreshMetadata).toBe(false);
  });

  it('leaves a negatable setting out unless one of its two options is given', async () => {
    invoke.mockResolvedValue({ id: 3, title: 'Work' });
    const call = await run('folders', 'edit', '3', '--no-include-contacts', '--include-bots', '--chat', '700000001');
    expect(call.args).toEqual({ folder: '3', contacts: false, bots: true, includePeers: ['700000001'] });
  });

  it('joins a variadic positional, and lets the option win over it', async () => {
    invoke.mockResolvedValue({ source: 'archive', returned: 0, messages: [] });
    expect((await run('messages', 'search', 'hello', 'world')).args.query).toBe('hello world');
    expect((await run('messages', 'search', 'ignored', '--query', 'kept')).args.query).toBe('kept');
  });

  it('reads an empty filter as no filter', async () => {
    invoke.mockResolvedValue([]);
    expect((await run('channels', 'list', '--query', '')).args).toEqual({ query: '', limit: 50 });
    invoke.mockResolvedValue({ source: 'archive', returned: 0, hasMore: false, messages: [] });
    expect((await run('messages', 'list', '--chat', '')).args).toEqual({ channelIds: [], source: 'archive', limit: 50 });
  });

  it('reads an empty number as its default, and an empty option as the positional it shares', async () => {
    invoke.mockResolvedValue({ source: 'archive', returned: 0, hasMore: false, messages: [] });
    expect((await run('messages', 'list', '--limit', '')).args.limit).toBe(50);
    invoke.mockResolvedValue({ source: 'archive', returned: 0, messages: [] });
    expect((await run('messages', 'search', '--query', '', 'kept')).args.query).toBe('kept');
  });

  it('reads a blank date as no filter, keeps blank text a field takes, and compares aliases as numbers', async () => {
    invoke.mockResolvedValue({ source: 'archive', returned: 0, hasMore: false, messages: [] });
    expect((await run('messages', 'list', '--after', '')).args).toEqual({ source: 'archive', limit: 50 });
    expect((await run('messages', 'list', '--before-id', '5', '--offset-id', '05')).args.beforeId).toBe(5);
    invoke.mockResolvedValue({ userId: '700000001', notes: '' });
    expect((await run('contacts', 'notes', 'set', '--user', '700000001', '--notes', '')).args.notes).toBe('');
    invoke.mockResolvedValue({ channelId: '700000001', messageId: 3 });
    expect((await run('send', 'text', '--to', '', '--chat', '700000001', '--message', 'hi')).args.channelId).toBe('700000001');
  });

  it('refuses a blank invite instead of sending it to Telegram', async () => {
    await run('groups', 'join', '--code', '   ');
    expect(invoke).not.toHaveBeenCalled();
    expect(stderr.join('')).toContain('--code is required');
  });

  it('takes the hidden alias of an option', async () => {
    invoke.mockResolvedValue({ channelId: '700000001', messageId: 3 });
    const call = await run('send', 'text', '--chat', '700000001', '--text', 'hi');
    expect(call.args).toMatchObject({ channelId: '700000001', text: 'hi' });
  });

  it('sends the chat of `channels unwatch --missing` to no one and asks for every missing chat', async () => {
    invoke.mockResolvedValue({ channelIds: [] });
    expect((await run('channels', 'unwatch', '--missing')).args).toEqual({ missing: true });
    expect(invoke).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ op: 'channelsUnwatch' }));
    expect(logged).toEqual(['No chat is missing from your dialogs without an answer from Telegram.']);
  });
});

describe('the caller side of the seam', () => {
  it('resolves a relative download path against the caller, not the server', async () => {
    invoke.mockResolvedValue({ path: '/x', bytes: 1 });
    const call = await run('media', 'download', '--chat', '700000001', '--id', '4', '--output', 'out/photo.jpg');
    expect(call.args.outputPath).toBe(path.resolve(process.cwd(), 'out/photo.jpg'));
  });

  it('resolves the file of a send against the caller', async () => {
    invoke.mockResolvedValue({ ok: true, message_id: 5 });
    const call = await run('send', 'photo', '--to', '700000001', '--photo', 'pics/a.png');
    expect(call.args.photo).toBe(path.resolve(process.cwd(), 'pics/a.png'));
  });

  it('bounds every operation by --timeout, and a send by 30s without it', async () => {
    invoke.mockResolvedValue({ source: 'archive', returned: 0, hasMore: false, messages: [] });
    expect((await run('--timeout', '5s', 'messages', 'list')).invokeTimeoutMs).toBe(5000);
    invoke.mockResolvedValue({ channelId: '700000001', messageId: 3 });
    const send = await run('send', 'text', '--to', '700000001', '--message', 'hi');
    expect(send.invokeTimeoutMs).toBe(30000);
    expect(send.args.timeoutMs).toBe(30000);
    const unbounded = await run('--timeout', '0', 'send', 'text', '--to', '700000001', '--message', 'hi');
    expect(unbounded.invokeTimeoutMs).toBe(0);
    expect(unbounded.args.timeoutMs).toBe(0);
    const longer = await run('--timeout', '5m', 'send', 'text', '--to', '700000001', '--message', 'hi');
    expect(longer.invokeTimeoutMs).toBe(300000);
    expect(longer.args.timeoutMs).toBe(300000);
  });

  it('keeps the trailing separator that makes a download path a directory', async () => {
    invoke.mockResolvedValue({ path: '/x', bytes: 1 });
    const call = await run('media', 'download', '--chat', '700000001', '--id', '4', '--output', 'downloads/');
    expect(call.args.outputPath).toBe(`${path.resolve(process.cwd(), 'downloads')}${path.sep}`);
  });
});

describe('bad input', () => {
  it('names the option and asks nothing of the server', async () => {
    await run('messages', 'list', '--limit', 'many');
    expect(invoke).not.toHaveBeenCalled();
    expect(process.exitCode).toBe(1);
    expect(stderr.join('')).toContain('--limit must be a positive integer');
  });

  it('takes only plain integers for a number', async () => {
    await run('messages', 'list', '--limit', '0x10');
    expect(invoke).not.toHaveBeenCalled();
    expect(stderr.join('')).toContain('--limit must be a positive integer');
  });

  it('asks for a chat before reading live', async () => {
    await run('messages', 'list', '--source', 'live');
    expect(invoke).not.toHaveBeenCalled();
    expect(stderr.join('')).toContain('--chat is required for live source.');
  });

  it('rejects an option and its alias that disagree', async () => {
    await run('messages', 'list', '--before-id', '9', '--offset-id', '8');
    expect(invoke).not.toHaveBeenCalled();
    expect(stderr.join('')).toContain('--before-id and --offset-id must match when both are provided');
  });

  it('reports a send without its message as a send validation error', async () => {
    await run('--json', 'send', 'text', '--to', '700000001');
    expect(invoke).not.toHaveBeenCalled();
    const payload = JSON.parse(stderr.join(''));
    expect(payload).toMatchObject({ ok: false, error: { type: 'validation', method: 'sendText', message: '--message is required' } });
  });

  it('reports a send the caller stopped waiting for as a send timeout', async () => {
    invoke.mockRejectedValue(Object.assign(new Error('aborted'), { name: 'TimeoutError' }));
    await run('send', 'text', '--to', '700000001', '--message', 'hi');
    expect(stderr.join('')).toContain('Send timed out after 30s');
  });
});

describe('output', () => {
  it('prints the payload as JSON with --json', async () => {
    invoke.mockResolvedValue([{ id: '700000001', title: 'Chat' }]);
    await run('--json', 'channels', 'list');
    expect(JSON.parse(stdout.join(''))).toEqual([{ id: '700000001', title: 'Chat' }]);
  });

  it('renders the payload for people without it', async () => {
    invoke.mockResolvedValue([{ id: '700000001', title: 'Chat' }]);
    await run('channels', 'list');
    expect(logged).toEqual(['Chat (700000001)']);
  });

  it('shows the archive fallback note when the server says it read Telegram instead', async () => {
    invoke.mockResolvedValue({
      source: 'live',
      returned: 1,
      hasMore: false,
      usedLiveFallback: true,
      messages: [{ channelId: '700000001', messageId: 1, date: '2026-01-01T00:00:00.000Z', text: 'hi', fromId: '1' }],
    });
    await run('messages', 'list', '--chat', '700000001');
    expect(logged.at(-1)).toContain('no archived messages for 700000001');
  });
});
