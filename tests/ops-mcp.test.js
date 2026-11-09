// The MCP tools built from the operation declarations, driven through a real MCP
// client over an in-memory transport.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';

import { registerOperationTools } from '../core/ops/mcp.js';
import { executeOperation } from '../core/ops/execute.js';
import { OPERATION_LIST } from '../core/ops/index.js';

let client;
let services;
let ensureLogin;

beforeEach(async () => {
  services = {
    telegramClient: {
      listDialogs: vi.fn(async (limit) => [{ id: '700000001', title: 'Chat', limit }]),
      getFolders: vi.fn(async () => [{ id: 2, title: 'Work', type: 'filter' }]),
    },
    messageSyncService: {
      listTaggedChannels: vi.fn(() => [{ channelId: '700000001', peerTitle: 'Chat' }]),
    },
  };
  ensureLogin = vi.fn().mockResolvedValue();
  const server = new McpServer({ name: 'tgcli-test', version: '0.0.0' });
  registerOperationTools(server, OPERATION_LIST, (op, args) => executeOperation(op, args, { services, ensureLogin }));
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientTransport);
});

afterEach(async () => {
  await client.close();
});

describe('tools/list', () => {
  it('lists one tool per MCP operation, with its strict schema and hints', async () => {
    const { tools } = await client.listTools();
    expect(tools.map((tool) => tool.name).sort()).toEqual(OPERATION_LIST.filter((op) => op.mcp).map((op) => op.name).sort());
    const list = tools.find((tool) => tool.name === 'messagesList');
    expect(list.inputSchema.additionalProperties).toBe(false);
    expect(Object.keys(list.inputSchema.properties)).toEqual(expect.arrayContaining(['channelIds', 'beforeId', 'afterId']));
    expect(list.annotations).toMatchObject({ readOnlyHint: true });
    expect(tools.find((tool) => tool.name === 'groupsLeave').annotations).toMatchObject({ readOnlyHint: false, destructiveHint: true });
    expect(tools.find((tool) => tool.name === 'listTaggedChannels').annotations).toMatchObject({ openWorldHint: false });
    expect(tools.find((tool) => tool.name === 'listChannels').annotations).toMatchObject({ openWorldHint: true });
  });

  it('reaches the operations that had no tool before, and keeps CLI-only aliases off', async () => {
    const names = (await client.listTools()).tools.map((tool) => tool.name);
    expect(names).toEqual(expect.arrayContaining(['foldersList', 'messagesSendPhoto', 'channelsWatch', 'channelsShow']));
    expect(names).not.toContain('channelsSync');
  });
});

describe('tools/call', () => {
  it('runs the operation with its defaults and returns the payload as JSON text', async () => {
    const result = await client.callTool({ name: 'listChannels', arguments: {} });
    expect(result.isError).toBeFalsy();
    expect(JSON.parse(result.content[0].text)).toEqual([{ id: '700000001', title: 'Chat', limit: 50 }]);
    expect(services.telegramClient.listDialogs).toHaveBeenCalledWith(50);
    expect(ensureLogin).toHaveBeenCalled();
  });

  it('takes a folder by its numeric id, as foldersList returns it', async () => {
    services.telegramClient.showFolder = vi.fn(async (folder) => ({ id: folder, title: 'Work' }));
    const result = await client.callTool({ name: 'foldersShow', arguments: { folder: 2 } });
    expect(result.isError).toBeFalsy();
    expect(services.telegramClient.showFolder).toHaveBeenCalledWith(2, { resolve: undefined });
  });

  it('skips the login for an archive-only operation', async () => {
    await client.callTool({ name: 'listTaggedChannels', arguments: { tag: 'news' } });
    expect(ensureLogin).not.toHaveBeenCalled();
    expect(services.messageSyncService.listTaggedChannels).toHaveBeenCalledWith('news', { source: undefined, limit: 100 });
  });

  it('refuses an argument the tool does not take', async () => {
    const result = await client.callTool({ name: 'messagesList', arguments: { channelId: '700000001' } });
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toMatch(/channelId/);
  });

  it('names the param a live read needs', async () => {
    const result = await client.callTool({ name: 'messagesList', arguments: { source: 'live' } });
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain('channelIds is required for live source.');
  });

  it('reports a check across fields with the param names', async () => {
    const result = await client.callTool({ name: 'messagesList', arguments: { beforeId: 5, afterId: 9 } });
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain('afterId must be lower than beforeId');
  });
});
