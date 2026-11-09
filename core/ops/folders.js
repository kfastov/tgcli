// Chat folders (dialog filters) and shared folder links.

import { defineOperation, field } from './operation.js';

const folderInput = () => field.folder('Folder ID or title').positional('<folder>');

// The folder settings create and edit share: field key, flag name, help.
const FOLDER_FLAGS = [
  ['contacts', 'include-contacts', 'Include contacts'],
  ['nonContacts', 'include-non-contacts', 'Include non-contacts'],
  ['groups', 'include-groups', 'Include groups'],
  ['broadcasts', 'include-channels', 'Include channels'],
  ['bots', 'include-bots', 'Include bots'],
  ['excludeMuted', 'exclude-muted', 'Exclude muted'],
  ['excludeRead', 'exclude-read', 'Exclude read'],
  ['excludeArchived', 'exclude-archived', 'Exclude archived'],
];

const FOLDER_PEERS = [
  ['includePeers', '--chat <id>', 'Chat to include (repeatable)'],
  ['excludePeers', '--exclude-chat <id>', 'Chat to exclude (repeatable)'],
  ['pinnedPeers', '--pin-chat <id>', 'Pin chat in folder (repeatable)'],
];

const FOLDER_KEYS = ['title', 'emoji', ...FOLDER_FLAGS.map(([key]) => key), ...FOLDER_PEERS.map(([key]) => key)];

// Folder settings as create and edit take them; edit leaves out what is not given.
function folderSettingsInput({ edit }) {
  const input = {
    title: edit
      ? field.text('Folder name (max 12 chars)').optional().flag('--title <name>')
      : field.text('Folder name (max 12 chars)').flag('--title <name>'),
    emoji: field.textOrEmpty('Emoji icon').optional().flag('--emoji <emoji>'),
  };
  for (const [key, flag, help] of FOLDER_FLAGS) {
    input[key] = field.bool(help).optional().flag(`--${flag}`, { negatable: edit, help });
  }
  for (const [key, flag, help] of FOLDER_PEERS) {
    input[key] = field.chats(help).optional().flag(flag, { help });
  }
  return input;
}

function checkTitle(args, fail) {
  if (args.title !== undefined && args.title.length > 12) {
    fail('{title} must be 1-12 characters');
  }
}

// The settings given, with each chat list as a list; an empty chat list is left out.
function folderSettings(args) {
  const settings = {};
  for (const key of FOLDER_KEYS) {
    const isPeerList = FOLDER_PEERS.some(([peerKey]) => peerKey === key);
    const value = isPeerList && args[key] !== undefined ? [args[key]].flat() : args[key];
    if (value !== undefined && !(isPeerList && value.length === 0)) {
      settings[key] = value;
    }
  }
  return settings;
}

function printResolvedPeers(peers, label, print) {
  if (!peers?.length) return;
  if (label) print(`  ${label}:`);
  const indent = label ? '    ' : '  ';
  const grouped = {};
  for (const peer of peers) {
    const group = `${peer.type}s`;
    if (!grouped[group]) grouped[group] = [];
    grouped[group].push(`${peer.name ?? peer.title ?? '(unresolved)'} (${peer.id})`);
  }
  for (const [group, items] of Object.entries(grouped)) {
    print(`${indent}${group}:`);
    for (const item of items) print(`${indent}  - ${item}`);
  }
}

function printPeers(peers, label, print) {
  if (!peers?.length) {
    print(`  ${label}: (none)`);
    return;
  }
  print(`  ${label}:`);
  for (const peer of peers) print(`    - ${peer.type}:${peer.id}`);
}

export const foldersList = defineOperation({
  name: 'foldersList',
  command: 'folders list',
  summary: 'List all folders',
  annotations: { readOnly: true },
  run: (ctx) => ctx.telegramClient.getFolders(),
  render(folders, { print }) {
    for (const folder of folders) {
      print(`${folder.title} (id=${folder.id}, type=${folder.type})`);
    }
  },
});

export const foldersShow = defineOperation({
  name: 'foldersShow',
  command: 'folders show',
  summary: 'Show folder details',
  annotations: { readOnly: true },
  input: {
    folder: folderInput(),
    resolve: field.bool('Resolve peer IDs to names (slower, requires API calls)').optional().flag('--resolve'),
  },
  run: (ctx, args) => ctx.telegramClient.showFolder(args.folder, { resolve: args.resolve }),
  render(info, { args, print }) {
    print(`${info.title} (id=${info.id}, type=${info.type})`);
    if (info.emoji) print(`  emoji: ${info.emoji}`);
    const includes = ['contacts', 'nonContacts', 'groups', 'broadcasts', 'bots'].filter((key) => info[key]);
    if (includes.length) print(`  includes: ${includes.join(', ')}`);
    const excludes = ['excludeMuted', 'excludeRead', 'excludeArchived'].filter((key) => info[key]);
    if (excludes.length) print(`  excludes: ${excludes.join(', ')}`);
    if (args.resolve) {
      printResolvedPeers(info.includePeers, null, print);
      printResolvedPeers(info.excludePeers, 'excluded', print);
      printResolvedPeers(info.pinnedPeers, 'pinned', print);
    } else {
      printPeers(info.includePeers, 'includePeers', print);
      printPeers(info.excludePeers, 'excludePeers', print);
      printPeers(info.pinnedPeers, 'pinnedPeers', print);
    }
  },
});

export const foldersCreate = defineOperation({
  name: 'foldersCreate',
  command: 'folders create',
  summary: 'Create a new folder',
  input: folderSettingsInput({ edit: false }),
  refine: checkTitle,
  run: (ctx, args) => ctx.telegramClient.createFolder(folderSettings(args)),
  render(result, { print }) {
    print(`Created folder: ${result.title} (id=${result.id})`);
  },
});

export const foldersEdit = defineOperation({
  name: 'foldersEdit',
  command: 'folders edit',
  summary: 'Edit an existing folder',
  description: 'Edits a folder; settings left out stay as they are, and a non-empty chat list replaces the old one.',
  annotations: { idempotent: true, destructive: true },
  input: { folder: folderInput(), ...folderSettingsInput({ edit: true }) },
  refine: checkTitle,
  run: (ctx, args) => ctx.telegramClient.editFolder(args.folder, folderSettings(args)),
  render(result, { print }) {
    print(`Updated folder: ${result.title} (id=${result.id})`);
  },
});

export const foldersDelete = defineOperation({
  name: 'foldersDelete',
  command: 'folders delete',
  summary: 'Delete a folder',
  annotations: { destructive: true },
  input: { folder: folderInput() },
  run: (ctx, args) => ctx.telegramClient.deleteFolder(args.folder),
  render(result, { print }) {
    print(`Deleted folder id=${result.id}`);
  },
});

export const foldersReorder = defineOperation({
  name: 'foldersReorder',
  command: 'folders reorder',
  summary: 'Reorder folders',
  annotations: { idempotent: true },
  input: {
    ids: field.ints('Folder IDs in the desired order').flag('--ids <id1,id2,...>', { help: 'Comma-separated folder IDs in desired order' }),
  },
  run: (ctx, args) => ctx.telegramClient.setFoldersOrder(args.ids),
  render(_result, { print }) {
    print('Folders reordered');
  },
});

const folderChatInput = () => ({
  folder: folderInput(),
  channelId: field.chat('Chat ID or username').flag('--chat <chatId>', { help: 'Chat identifier' }),
});

export const foldersChatsAdd = defineOperation({
  name: 'foldersChatsAdd',
  command: 'folders chats add',
  summary: 'Add chat to folder',
  input: folderChatInput(),
  run: (ctx, args) => ctx.telegramClient.addChatToFolder(args.folder, args.channelId),
  render(result, { print }) {
    print(`Chat added to folder id=${result.folderId}`);
  },
});

export const foldersChatsRemove = defineOperation({
  name: 'foldersChatsRemove',
  command: 'folders chats remove',
  summary: 'Remove chat from folder',
  annotations: { destructive: true },
  input: folderChatInput(),
  run: (ctx, args) => ctx.telegramClient.removeChatFromFolder(args.folder, args.channelId),
  render(result, { print }) {
    print(`Chat removed from folder id=${result.folderId}`);
  },
});

export const foldersJoin = defineOperation({
  name: 'foldersJoin',
  command: 'folders join',
  summary: 'Join shared folder via invite link',
  input: { link: field.text('Shared folder invite link').positional('<link>') },
  run: (ctx, args) => ctx.telegramClient.joinChatlist(args.link),
  render(result, { print }) {
    print(`Joined folder: ${result.title} (id=${result.id})`);
  },
});

export const FOLDER_OPERATIONS = [
  foldersList,
  foldersShow,
  foldersCreate,
  foldersEdit,
  foldersDelete,
  foldersReorder,
  foldersChatsAdd,
  foldersChatsRemove,
  foldersJoin,
];
