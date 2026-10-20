import { describe, it, expect, vi } from 'vitest';
import TelegramClient from '../telegram-client.js';

const activeGroup = { id: 1, type: 'chat', displayName: 'Active Group' };
const archivedGroup = { id: 2, type: 'chat', displayName: 'Archived Group' };

function createMockClient() {
  const tc = Object.create(TelegramClient.prototype);
  tc.ensureLogin = vi.fn().mockResolvedValue(undefined);
  tc.client = {
    iterDialogs: async function* ({ archived = 'exclude' } = {}) {
      yield { peer: activeGroup };
      if (archived === 'keep') {
        yield { peer: archivedGroup };
      }
    },
  };
  return tc;
}

describe('archived dialogs', () => {
  it('listDialogs includes archived chats', async () => {
    const result = await createMockClient().listDialogs();
    expect(result.map((d) => d.title)).toEqual(['Active Group', 'Archived Group']);
  });

  it('local searchDialogs finds archived chats', async () => {
    const result = await createMockClient().searchDialogs('archived', 10, { local: true });
    expect(result.map((d) => d.title)).toEqual(['Archived Group']);
  });

  it('listGroups includes archived groups', async () => {
    const result = await createMockClient().listGroups();
    expect(result.map((g) => g.title)).toEqual(['Active Group', 'Archived Group']);
  });
});
