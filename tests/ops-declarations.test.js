// The operation declarations: what every one of them must satisfy for the CLI,
// MCP and the control API to be built from it, and how args are validated on each
// surface.

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { COMMAND_GROUPS, OPERATION_LIST, OPERATIONS } from '../core/ops/index.js';
import { defineOperation, field, OperationInputError, parseOperationArgs } from '../core/ops/operation.js';
import { executeOperation } from '../core/ops/execute.js';

describe('the operation registry', () => {
  it('names every operation once, and the registry finds it by that name', () => {
    const names = OPERATION_LIST.map((op) => op.name);
    expect(new Set(names).size).toBe(names.length);
    for (const op of OPERATION_LIST) {
      expect(OPERATIONS[op.name]).toBe(op);
    }
  });

  it('gives every CLI command its own path under a described group', () => {
    const paths = OPERATION_LIST.filter((op) => op.command).map((op) => op.command);
    expect(new Set(paths).size).toBe(paths.length);
    for (const path of paths) {
      const groups = path.split(' ').slice(0, -1);
      for (let depth = 1; depth <= groups.length; depth += 1) {
        expect(COMMAND_GROUPS[groups.slice(0, depth).join(' ')], path).toBeTruthy();
      }
    }
  });

  it('spells every field of a CLI operation that a caller must give on the command line', () => {
    for (const op of OPERATION_LIST.filter((candidate) => candidate.command)) {
      for (const [key, value] of Object.entries(op.input)) {
        const required = !value.schema.safeParse(undefined).success;
        if (required) {
          expect(value.cli.flag || value.cli.positional, `${op.name}.${key}`).toBeTruthy();
        }
      }
    }
  });

  it('never gives two fields of one command the same option', () => {
    for (const op of OPERATION_LIST.filter((candidate) => candidate.command)) {
      const flags = Object.values(op.input).flatMap((value) => [
        value.cli.flag,
        ...(value.cli.aliases ?? []).map((alias) => alias.flag),
      ]).filter(Boolean).map((flag) => flag.split(' ')[0]);
      expect(new Set(flags).size, op.name).toBe(flags.length);
    }
  });

  it('has a JSON Schema and a description for every MCP tool', () => {
    for (const op of OPERATION_LIST.filter((candidate) => candidate.mcp)) {
      const schema = z.toJSONSchema(op.objectSchema, { io: 'input' });
      expect(schema.type, op.name).toBe('object');
      expect(schema.additionalProperties, op.name).toBe(false);
      expect(op.description, op.name).toBeTruthy();
    }
  });

  it('keeps the MCP tool names clients already know', () => {
    const tools = OPERATION_LIST.filter((op) => op.mcp).map((op) => op.name);
    expect(tools).toEqual(expect.arrayContaining([
      'listChannels', 'searchChannels', 'listActiveChannels', 'setChannelTags', 'listChannelTags',
      'listTaggedChannels', 'refreshChannelMetadata', 'getChannelMetadata', 'autoTagChannels',
      'topicsList', 'topicsSearch', 'messagesList', 'messagesGet', 'messagesContext', 'messagesSearch',
      'messagesSend', 'messagesSendFile', 'mediaDownload', 'contactsSearch', 'contactsGet',
      'contactsAliasSet', 'contactsAliasRemove', 'contactsTagsAdd', 'contactsTagsRemove',
      'contactsNotesSet', 'groupsList', 'groupsInfo', 'groupsRename', 'groupsMembersAdd',
      'groupsMembersRemove', 'groupsInviteLinkGet', 'groupsInviteLinkRevoke', 'groupsJoin',
      'groupsLeave', 'scheduleMessageSync', 'getSyncedMessageStats', 'listMessageSyncJobs',
      'markChannelRead',
    ]));
  });
});

describe('parseOperationArgs', () => {
  const list = OPERATIONS.messagesList;

  it('applies the defaults', () => {
    expect(parseOperationArgs(list, {}, 'wire')).toEqual({ source: 'archive', limit: 50 });
  });

  it('names a missing field as each surface spells it', () => {
    const show = OPERATIONS.messagesGet;
    expect(() => parseOperationArgs(show, { messageId: 1 }, 'cli')).toThrow('--chat is required');
    expect(() => parseOperationArgs(show, { messageId: 1 }, 'wire')).toThrow('channelId is required');
  });

  it('says what a field takes when the value is wrong', () => {
    expect(() => parseOperationArgs(list, { limit: 0 }, 'cli')).toThrow('--limit must be a positive integer');
    expect(() => parseOperationArgs(list, { source: 'disk' }, 'wire'))
      .toThrow('source must be one of: archive, live, both');
    expect(() => parseOperationArgs(list, { fromDate: 'yesterday' }, 'cli'))
      .toThrow('--after must be an ISO-8601 date');
  });

  it('fills the field names of a check across fields per surface', () => {
    expect(() => parseOperationArgs(list, { beforeId: 5, afterId: 7 }, 'cli'))
      .toThrow('--after-id must be lower than --before-id');
    expect(() => parseOperationArgs(list, { beforeId: 5, afterId: 7 }, 'wire'))
      .toThrow('afterId must be lower than beforeId');
  });

  it('rejects an argument the operation does not take', () => {
    expect(() => parseOperationArgs(list, { channelId: '700000001' }, 'wire'))
      .toThrow('messagesList does not take channelId');
  });

  it('throws OperationInputError, which the control API answers with a 400', () => {
    expect(() => parseOperationArgs(list, { limit: 'many' }, 'wire')).toThrow(OperationInputError);
  });

  it('names a positional the way the help shows it', () => {
    expect(() => parseOperationArgs(OPERATIONS.foldersDelete, {}, 'cli')).toThrow('folder is required');
  });
});

describe('executeOperation', () => {
  const probe = defineOperation({
    name: 'probe',
    summary: 'Probe',
    telegram: (args) => args.live === true,
    input: {
      live: field.bool('Needs Telegram').optional(),
      limit: field.int('Limit').default(3),
    },
    run: (services, args) => ({ seen: services.marker, args }),
  });

  it('validates, applies defaults and runs the handler against the services', async () => {
    const result = await executeOperation(probe, {}, { services: { marker: 'warm' } });
    expect(result).toEqual({ seen: 'warm', args: { limit: 3 } });
  });

  it('logs in only for an operation that needs Telegram for these args', async () => {
    const ensureLogin = vi.fn().mockResolvedValue();
    await executeOperation(probe, { live: false }, { services: {}, ensureLogin });
    expect(ensureLogin).not.toHaveBeenCalled();
    await executeOperation(probe, { live: true }, { services: {}, ensureLogin });
    expect(ensureLogin).toHaveBeenCalledTimes(1);
  });

  it('never runs the handler on args that fail validation', async () => {
    const run = vi.fn();
    const op = defineOperation({ name: 'strict', summary: 'Strict', input: { id: field.int('Id') }, run });
    await expect(executeOperation(op, { id: 'x' }, { services: {} })).rejects.toThrow('id must be a positive integer');
    expect(run).not.toHaveBeenCalled();
  });

  it('logs in before setting tags on a chat given by username, not by id', async () => {
    const ensureLogin = vi.fn().mockResolvedValue();
    const services = {
      messageSyncService: {
        resolveChatKey: vi.fn(async (chat) => (chat === '@somechat' ? '-1007000000001' : chat)),
        setChannelTags: vi.fn((chatId, tags) => tags),
      },
    };
    await executeOperation(OPERATIONS.setChannelTags, { channelId: '-1007000000001', tags: ['news'] }, { services, ensureLogin });
    expect(ensureLogin).not.toHaveBeenCalled();
    const result = await executeOperation(OPERATIONS.setChannelTags, { channelId: '@somechat', tags: [] }, { services, ensureLogin });
    expect(ensureLogin).toHaveBeenCalledTimes(1);
    expect(result).toEqual({ channelId: '-1007000000001', tags: [] });
  });

  it('logs in before an archive-only read by username, not by id', async () => {
    const ensureLogin = vi.fn().mockResolvedValue();
    const services = {
      messageSyncService: {
        getMessageStats: vi.fn(() => ({ total: 1 })),
        resolveChatKeyForRead: vi.fn(async () => '-1007000000001'),
      },
    };
    await executeOperation(OPERATIONS.getSyncedMessageStats, { channelId: '-1007000000001' }, { services, ensureLogin });
    expect(ensureLogin).not.toHaveBeenCalled();
    await executeOperation(OPERATIONS.getSyncedMessageStats, { channelId: '@somechat' }, { services, ensureLogin });
    expect(ensureLogin).toHaveBeenCalledTimes(1);
    expect(services.messageSyncService.getMessageStats).toHaveBeenLastCalledWith('-1007000000001');
  });
});
