import { describe, it, expect, vi, beforeEach } from 'vitest';

// We test folder methods by creating a minimal TelegramClient instance
// with mocked internals (this.client, this.ensureLogin).
// The class constructor has heavy deps, so we import and override prototype.

import TelegramClient from '../telegram-client.js';

function createMockClient() {
  // Create instance bypassing constructor
  const tc = Object.create(TelegramClient.prototype);
  tc.ensureLogin = vi.fn().mockResolvedValue(undefined);
  tc.client = {
    getFolders: vi.fn(),
    findFolder: vi.fn(),
    createFolder: vi.fn(),
    editFolder: vi.fn(),
    deleteFolder: vi.fn(),
    setFoldersOrder: vi.fn(),
    joinChatlist: vi.fn(),
    getChat: vi.fn(),
    getFullUser: vi.fn(),
  };
  return tc;
}

describe('getFolders', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('maps dialogFilterDefault to id=0', async () => {
    tc.client.getFolders.mockResolvedValue({
      filters: [{ _: 'dialogFilterDefault' }],
    });
    const result = await tc.getFolders();
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({ id: 0, title: 'All Chats', type: 'default' });
  });

  it('maps dialogFilterChatlist type', async () => {
    tc.client.getFolders.mockResolvedValue({
      filters: [{ _: 'dialogFilterChatlist', id: 5, title: 'Shared', emoticon: null }],
    });
    const result = await tc.getFolders();
    expect(result[0].type).toBe('chatlist');
  });

  it('maps regular filter type', async () => {
    tc.client.getFolders.mockResolvedValue({
      filters: [{ _: 'dialogFilter', id: 1, title: 'Work', contacts: true, groups: true }],
    });
    const result = await tc.getFolders();
    expect(result[0]).toMatchObject({ id: 1, title: 'Work', type: 'filter', contacts: true, groups: true });
  });

  it('handles title as object with text field', async () => {
    tc.client.getFolders.mockResolvedValue({
      filters: [{ _: 'dialogFilter', id: 2, title: { text: 'Custom' } }],
    });
    const result = await tc.getFolders();
    expect(result[0].title).toBe('Custom');
  });

  it('handles null filters', async () => {
    tc.client.getFolders.mockResolvedValue({ filters: null });
    const result = await tc.getFolders();
    expect(result).toEqual([]);
  });

  it('handles undefined result', async () => {
    tc.client.getFolders.mockResolvedValue(undefined);
    const result = await tc.getFolders();
    expect(result).toEqual([]);
  });
});

describe('getFolders - error handling', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('propagates MTProto errors from getFolders', async () => {
    tc.client.getFolders.mockRejectedValue(new Error('FLOOD_WAIT_30'));
    await expect(tc.getFolders()).rejects.toThrow('FLOOD_WAIT_30');
  });
});

describe('createFolder - error handling', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('propagates MTProto errors from createFolder', async () => {
    tc.client.createFolder.mockRejectedValue(new Error('FILTER_TITLE_EMPTY'));
    await expect(tc.createFolder({ title: 'Test' })).rejects.toThrow('FILTER_TITLE_EMPTY');
  });
});

describe('editFolder - error handling', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('propagates MTProto errors from editFolder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, _: 'dialogFilter' });
    tc.client.editFolder.mockRejectedValue(new Error('FLOOD_WAIT_60'));
    await expect(tc.editFolder('1', { title: 'New' })).rejects.toThrow('FLOOD_WAIT_60');
  });
});

describe('deleteFolder - error handling', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('propagates MTProto errors from deleteFolder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 2, _: 'dialogFilter' });
    tc.client.deleteFolder.mockRejectedValue(new Error('FILTER_ID_INVALID'));
    await expect(tc.deleteFolder('2')).rejects.toThrow('FILTER_ID_INVALID');
  });
});

describe('findFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('searches by numeric id', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 3 });
    const result = await tc.findFolder('3');
    expect(tc.client.findFolder).toHaveBeenCalledWith({ id: 3 });
    expect(result).toEqual({ id: 3 });
  });

  it('searches by title for non-numeric input', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, title: 'Work' });
    await tc.findFolder('Work');
    expect(tc.client.findFolder).toHaveBeenCalledWith({ title: 'Work' });
  });

  it('throws on empty string', async () => {
    await expect(tc.findFolder('')).rejects.toThrow('Folder identifier cannot be empty');
  });

  it('throws on whitespace-only string', async () => {
    await expect(tc.findFolder('   ')).rejects.toThrow('Folder identifier cannot be empty');
  });

  it('searches id=0 by numeric path', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 0, _: 'dialogFilterDefault' });
    await tc.findFolder('0');
    expect(tc.client.findFolder).toHaveBeenCalledWith({ id: 0 });
  });
});

describe('showFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('returns folder info', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, title: 'Work', _: 'dialogFilter',
      contacts: true, groups: true,
      includePeers: [{ userId: 123 }], excludePeers: [], pinnedPeers: [],
    });
    const result = await tc.showFolder('1');
    expect(result).toMatchObject({ id: 1, title: 'Work', type: 'filter', contacts: true });
    expect(result.includePeers).toEqual([{ type: 'user', id: 123 }]);
  });

  it('returns type=default for dialogFilterDefault', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 0, title: 'All Chats', _: 'dialogFilterDefault',
    });
    const result = await tc.showFolder('0');
    expect(result.type).toBe('default');
  });

  it('throws when folder not found', async () => {
    tc.client.findFolder.mockResolvedValue(null);
    await expect(tc.showFolder('999')).rejects.toThrow('Folder not found: 999');
  });
});

describe('createFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('creates a folder with all optional params', async () => {
    tc.client.createFolder.mockResolvedValue({ id: 5, title: 'Dev' });
    const result = await tc.createFolder({
      title: 'Dev',
      emoji: '💻',
      contacts: true,
      nonContacts: false,
      groups: true,
      broadcasts: false,
      bots: true,
      excludeMuted: true,
      excludeRead: false,
      excludeArchived: true,
      includePeers: [1, 2],
      excludePeers: [3],
      pinnedPeers: [4],
    });
    expect(tc.client.createFolder).toHaveBeenCalledWith({
      title: 'Dev',
      emoticon: '💻',
      contacts: true,
      nonContacts: false,
      groups: true,
      broadcasts: false,
      bots: true,
      excludeMuted: true,
      excludeRead: false,
      excludeArchived: true,
      includePeers: [1, 2],
      excludePeers: [3],
      pinnedPeers: [4],
    });
    expect(result).toEqual({ id: 5, title: 'Dev' });
  });

  it('handles title as object in result', async () => {
    tc.client.createFolder.mockResolvedValue({ id: 6, title: { text: 'FromObj' } });
    const result = await tc.createFolder({ title: 'Test' });
    expect(result.title).toBe('FromObj');
  });

  it('throws when title is missing', async () => {
    await expect(tc.createFolder({})).rejects.toThrow('Folder title is required');
  });

  it('throws when title is empty string', async () => {
    await expect(tc.createFolder({ title: '' })).rejects.toThrow('Folder title is required');
  });

  it('omits undefined optional params', async () => {
    tc.client.createFolder.mockResolvedValue({ id: 7, title: 'Min' });
    await tc.createFolder({ title: 'Min' });
    expect(tc.client.createFolder).toHaveBeenCalledWith({ title: 'Min' });
  });

  it('passes boolean false values correctly', async () => {
    tc.client.createFolder.mockResolvedValue({ id: 8, title: 'Test' });
    await tc.createFolder({ title: 'Test', contacts: false, groups: false });
    expect(tc.client.createFolder).toHaveBeenCalledWith(
      expect.objectContaining({ title: 'Test', contacts: false, groups: false }),
    );
  });
});

describe('editFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('edits a regular folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, title: 'Old', _: 'dialogFilter' });
    tc.client.editFolder.mockResolvedValue({ id: 1, title: 'New' });
    const result = await tc.editFolder('1', { title: 'New' });
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: { id: 1, title: 'Old', _: 'dialogFilter' },
      modification: { title: 'New' },
    });
    expect(result.title).toBe('New');
  });

  it('throws on default folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 0, _: 'dialogFilterDefault' });
    await expect(tc.editFolder('0', { title: 'X' })).rejects.toThrow('Cannot modify the default "All Chats" folder');
  });

  it('throws when folder not found', async () => {
    tc.client.findFolder.mockResolvedValue(null);
    await expect(tc.editFolder('99', { title: 'X' })).rejects.toThrow('Folder not found: 99');
  });

  it('passes empty modification object', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, _: 'dialogFilter' });
    tc.client.editFolder.mockResolvedValue({ id: 1, title: 'Same' });
    const result = await tc.editFolder('1', {});
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: { id: 1, _: 'dialogFilter' },
      modification: {},
    });
    expect(result.title).toBe('Same');
  });

  it('maps emoji to emoticon in modification', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, _: 'dialogFilter' });
    tc.client.editFolder.mockResolvedValue({ id: 1, title: 'T' });
    await tc.editFolder('1', { emoji: '🎮' });
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: { id: 1, _: 'dialogFilter' },
      modification: { emoticon: '🎮' },
    });
  });

  it('passes boolean false correctly', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 1, _: 'dialogFilter' });
    tc.client.editFolder.mockResolvedValue({ id: 1, title: 'T' });
    await tc.editFolder('1', { contacts: false });
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: { id: 1, _: 'dialogFilter' },
      modification: { contacts: false },
    });
  });
});

describe('deleteFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('deletes a regular folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 2, _: 'dialogFilter' });
    tc.client.deleteFolder.mockResolvedValue(undefined);
    const result = await tc.deleteFolder('2');
    expect(result).toEqual({ deleted: true, id: 2 });
  });

  it('throws on default folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 0, _: 'dialogFilterDefault' });
    await expect(tc.deleteFolder('0')).rejects.toThrow('Cannot delete the default "All Chats" folder');
  });

  it('throws when folder not found', async () => {
    tc.client.findFolder.mockResolvedValue(null);
    await expect(tc.deleteFolder('999')).rejects.toThrow('Folder not found: 999');
  });
});

describe('setFoldersOrder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('reorders folders', async () => {
    tc.client.setFoldersOrder.mockResolvedValue(undefined);
    const result = await tc.setFoldersOrder([1, 2, 3]);
    expect(tc.client.setFoldersOrder).toHaveBeenCalledWith([1, 2, 3]);
    expect(result).toEqual({ ok: true });
  });

  it('throws on empty array', () => {
    return expect(tc.setFoldersOrder([])).rejects.toThrow('At least one folder ID is required');
  });

  it('throws on negative id', () => {
    return expect(tc.setFoldersOrder([-1, 2])).rejects.toThrow('Invalid folder ID: -1');
  });

  it('throws on NaN string', () => {
    return expect(tc.setFoldersOrder(['abc'])).rejects.toThrow('Invalid folder ID: abc');
  });

  it('throws on non-integer', () => {
    return expect(tc.setFoldersOrder([1.5])).rejects.toThrow('Invalid folder ID: 1.5');
  });

  it('throws on duplicate ids', () => {
    return expect(tc.setFoldersOrder([1, 2, 1])).rejects.toThrow('Duplicate folder IDs');
  });

  it('accepts id=0 (default folder)', async () => {
    tc.client.setFoldersOrder.mockResolvedValue(undefined);
    const result = await tc.setFoldersOrder([0, 1, 2]);
    expect(tc.client.setFoldersOrder).toHaveBeenCalledWith([0, 1, 2]);
    expect(result).toEqual({ ok: true });
  });
});

describe('addChatToFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('adds a chat to folder', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter', includePeers: [],
    });
    tc.client.editFolder.mockResolvedValue({ id: 1 });
    const result = await tc.addChatToFolder('1', 123);
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: expect.objectContaining({ id: 1 }),
      modification: { includePeers: [123] },
    });
    expect(result).toEqual({ ok: true, folderId: 1 });
  });

  it('adds chat when includePeers is null', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter', includePeers: null,
    });
    tc.client.editFolder.mockResolvedValue({ id: 1 });
    const result = await tc.addChatToFolder('1', 123);
    expect(result).toEqual({ ok: true, folderId: 1 });
  });

  it('throws if chat already in folder', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter',
      includePeers: [{ userId: 123 }],
    });
    await expect(tc.addChatToFolder('1', 123)).rejects.toThrow('Chat 123 already in folder 1');
  });

  it('detects duplicate by channelId', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter',
      includePeers: [{ channelId: 456 }],
    });
    await expect(tc.addChatToFolder('1', 456)).rejects.toThrow('Chat 456 already in folder 1');
  });

  it('throws when folder not found', async () => {
    tc.client.findFolder.mockResolvedValue(null);
    await expect(tc.addChatToFolder('999', 123)).rejects.toThrow('Folder not found: 999');
  });

  it('throws on default folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 0, _: 'dialogFilterDefault' });
    await expect(tc.addChatToFolder('0', 123)).rejects.toThrow('Cannot modify the default');
  });
});

describe('removeChatFromFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('removes a chat from folder', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter',
      includePeers: [{ userId: 123 }, { userId: 456 }],
    });
    tc.client.editFolder.mockResolvedValue({ id: 1 });
    const result = await tc.removeChatFromFolder('1', 123);
    expect(tc.client.editFolder).toHaveBeenCalledWith({
      folder: expect.objectContaining({ id: 1 }),
      modification: { includePeers: [{ userId: 456 }] },
    });
    expect(result).toEqual({ ok: true, folderId: 1 });
  });

  it('throws if chat not found in folder', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, _: 'dialogFilter', includePeers: [{ userId: 456 }],
    });
    await expect(tc.removeChatFromFolder('1', 123)).rejects.toThrow('Chat 123 not found in folder 1');
  });

  it('throws on default folder', async () => {
    tc.client.findFolder.mockResolvedValue({ id: 0, _: 'dialogFilterDefault' });
    await expect(tc.removeChatFromFolder('0', 123)).rejects.toThrow('Cannot modify the default');
  });
});

describe('_isDefaultFolder', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('returns true for id=0', () => {
    expect(tc._isDefaultFolder({ id: 0 })).toBe(true);
  });

  it('returns true for dialogFilterDefault type', () => {
    expect(tc._isDefaultFolder({ id: 0, _: 'dialogFilterDefault' })).toBe(true);
  });

  it('returns false for regular folder', () => {
    expect(tc._isDefaultFolder({ id: 1, _: 'dialogFilter' })).toBe(false);
  });
});

describe('_extractPeerId', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('extracts userId', () => {
    expect(tc._extractPeerId({ userId: 123 })).toBe('123');
  });

  it('extracts channelId', () => {
    expect(tc._extractPeerId({ channelId: 456 })).toBe('456');
  });

  it('extracts chatId', () => {
    expect(tc._extractPeerId({ chatId: 789 })).toBe('789');
  });

  it('returns string for primitive', () => {
    expect(tc._extractPeerId(123)).toBe('123');
  });

  it('throws for null peer', () => {
    expect(() => tc._extractPeerId(null)).toThrow('Peer is null, cannot extract ID');
  });

  it('throws for object without recognized fields', () => {
    expect(() => tc._extractPeerId({ foo: 'bar' })).toThrow('Peer object has no recognizable ID field');
  });
});

describe('_normalizePeer', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('normalizes channel peer', () => {
    expect(tc._normalizePeer({ _: 'inputPeerChannel', channelId: 1951583351 }))
      .toEqual({ type: 'channel', id: 1951583351 });
  });

  it('normalizes user peer', () => {
    expect(tc._normalizePeer({ _: 'inputPeerUser', userId: 272066824 }))
      .toEqual({ type: 'user', id: 272066824 });
  });

  it('normalizes chat peer', () => {
    expect(tc._normalizePeer({ _: 'inputPeerChat', chatId: 555 }))
      .toEqual({ type: 'chat', id: 555 });
  });

  it('throws for null peer', () => {
    expect(() => tc._normalizePeer(null)).toThrow();
  });

  it('throws for peer without recognizable fields', () => {
    expect(() => tc._normalizePeer({ foo: 'bar' })).toThrow();
  });
});

describe('joinChatlist', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('joins a valid chatlist link', async () => {
    tc.client.joinChatlist.mockResolvedValue({ id: 10, title: 'Shared List' });
    const result = await tc.joinChatlist('https://t.me/addlist/abc123');
    expect(result).toMatchObject({ id: 10, title: 'Shared List', type: 'chatlist' });
  });

  it('accepts link with trailing slash', async () => {
    tc.client.joinChatlist.mockResolvedValue({ id: 10, title: 'Shared' });
    const result = await tc.joinChatlist('https://t.me/addlist/abc123/');
    expect(result).toMatchObject({ id: 10, type: 'chatlist' });
  });

  it('throws on invalid link', async () => {
    await expect(tc.joinChatlist('https://t.me/invalid')).rejects.toThrow('Invalid chatlist link');
  });

  it('throws when API returns null', async () => {
    tc.client.joinChatlist.mockResolvedValue(null);
    await expect(tc.joinChatlist('https://t.me/addlist/abc123')).rejects.toThrow('Failed to join chatlist');
  });

  it('rejects path traversal in slug', async () => {
    await expect(tc.joinChatlist('https://t.me/addlist/abc/../../evil')).rejects.toThrow('Invalid chatlist link');
  });
});

describe('showFolder - peer normalization', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('normalizes peers by default (no resolve)', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, title: 'AI', _: 'dialogFilter',
      includePeers: [{ channelId: 1951583351 }, { userId: 272066824 }],
      excludePeers: [{ chatId: 555 }],
      pinnedPeers: [],
    });
    const result = await tc.showFolder('1');
    expect(result.includePeers).toEqual([
      { type: 'channel', id: 1951583351 },
      { type: 'user', id: 272066824 },
    ]);
    expect(result.excludePeers).toEqual([{ type: 'chat', id: 555 }]);
    expect(result.pinnedPeers).toEqual([]);
  });

  it('resolves peer names with resolve=true', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, title: 'AI', _: 'dialogFilter',
      includePeers: [{ channelId: 1951583351 }, { userId: 272066824 }],
      excludePeers: [], pinnedPeers: [],
    });
    tc.client.getChat.mockResolvedValue({ displayName: 'ИИшница' });
    tc.client.getFullUser.mockResolvedValue({ displayName: 'Иван Иванов' });

    const result = await tc.showFolder('1', { resolve: true });
    expect(result.includePeers).toEqual([
      { type: 'channel', id: 1951583351, title: 'ИИшница' },
      { type: 'user', id: 272066824, name: 'Иван Иванов' },
    ]);
  });

  it('marks unresolved peers with (unresolved)', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, title: 'AI', _: 'dialogFilter',
      includePeers: [{ channelId: 999 }],
      excludePeers: [], pinnedPeers: [],
    });
    tc.client.getChat.mockRejectedValue(new Error('PEER_NOT_FOUND'));

    const result = await tc.showFolder('1', { resolve: true });
    expect(result.includePeers).toEqual([
      { type: 'channel', id: 999, title: '(unresolved)' },
    ]);
  });

  it('handles empty peer arrays', async () => {
    tc.client.findFolder.mockResolvedValue({
      id: 1, title: 'AI', _: 'dialogFilter',
      includePeers: [], excludePeers: null, pinnedPeers: undefined,
    });
    const result = await tc.showFolder('1');
    expect(result.includePeers).toEqual([]);
    expect(result.excludePeers).toEqual([]);
    expect(result.pinnedPeers).toEqual([]);
  });
});

describe('_resolvePeerName', () => {
  let tc;
  beforeEach(() => { tc = createMockClient(); });

  it('resolves channel name via getChat', async () => {
    tc.client.getChat.mockResolvedValue({ displayName: 'ИИшница' });
    const result = await tc._resolvePeerName('channel', 1951583351);
    expect(result).toBe('ИИшница');
  });

  it('resolves user name via getFullUser', async () => {
    tc.client.getFullUser.mockResolvedValue({ displayName: 'Иван Иванов' });
    const result = await tc._resolvePeerName('user', 272066824);
    expect(result).toBe('Иван Иванов');
  });

  it('returns null on error', async () => {
    tc.client.getChat.mockRejectedValue(new Error('PEER_NOT_FOUND'));
    const result = await tc._resolvePeerName('channel', 999);
    expect(result).toBeNull();
  });
});
