// Groups and supergroups: listing, details, membership and invite links.

import { defineOperation, field, toIsoDate } from './operation.js';
import { chatOption } from './chat-keys.js';

const groupInput = () => chatOption('Group ID or username', 'Group identifier');

function formatInviteLink(link) {
  if (!link) {
    return null;
  }
  return {
    link: link.link ?? null,
    isPrimary: typeof link.isPrimary === 'boolean' ? link.isPrimary : null,
    isRevoked: typeof link.isRevoked === 'boolean' ? link.isRevoked : null,
    createdAt: toIsoDate(link.date),
    startDate: toIsoDate(link.startDate),
    endDate: toIsoDate(link.endDate),
    usageLimit: typeof link.usageLimit === 'number' ? link.usageLimit : null,
    usage: typeof link.usage === 'number' ? link.usage : null,
    approvalNeeded: typeof link.approvalNeeded === 'boolean' ? link.approvalNeeded : null,
    pendingApprovals: typeof link.pendingApprovals === 'number' ? link.pendingApprovals : null,
  };
}

function renderInviteLink(link, { print }) {
  print(link?.link ?? '');
}

// An invite as people paste it: a t.me link, a '+hash', an '@username' or a bare hash.
function normalizeInviteCode(value) {
  const trimmed = value.trim();
  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
    return trimmed;
  }
  if (trimmed.startsWith('t.me/')) {
    return `https://${trimmed}`;
  }
  if (trimmed.startsWith('+')) {
    return `https://t.me/${trimmed}`;
  }
  if (trimmed.startsWith('@')) {
    return trimmed;
  }
  return `https://t.me/joinchat/${trimmed}`;
}

export const groupsList = defineOperation({
  name: 'groupsList',
  command: 'groups list',
  summary: 'List groups',
  description: 'Lists the group chats and supergroups the account is in.',
  annotations: { readOnly: true },
  input: {
    query: field.textOrEmpty('Search by title').optional().flag('--query <text>'),
    limit: field.int('Maximum number of groups to return').default(100).flag('--limit <n>', { help: 'Limit results' }),
  },
  run: (ctx, args) => ctx.telegramClient.listGroups({ query: args.query, limit: args.limit }),
  render(groups, { print }) {
    for (const group of groups) {
      print(`${group.title} (${group.id})`);
    }
  },
});

export const groupsInfo = defineOperation({
  name: 'groupsInfo',
  command: 'groups info',
  summary: 'Show group info',
  description: 'Fetches group information and metadata.',
  annotations: { readOnly: true },
  input: { channelId: groupInput() },
  run: (ctx, args) => ctx.telegramClient.getGroupInfo(args.channelId),
});

export const groupsRename = defineOperation({
  name: 'groupsRename',
  command: 'groups rename',
  summary: 'Rename group',
  description: 'Renames a group chat or supergroup.',
  annotations: { idempotent: true, destructive: true },
  input: {
    channelId: groupInput(),
    name: field.text('New group title').flag('--name <text>', { help: 'New name' }),
  },
  async run(ctx, args) {
    await ctx.telegramClient.renameGroup(args.channelId, args.name);
    return { channelId: args.channelId, name: args.name };
  },
  render(result, { print }) {
    print(`Group renamed: ${result.name}`);
  },
});

const usersInput = (help) => field.users(help).flag('--user <id>', { help: 'User id' });

export const groupsMembersAdd = defineOperation({
  name: 'groupsMembersAdd',
  command: 'groups members add',
  summary: 'Add members',
  description: 'Adds members to a group.',
  input: { channelId: groupInput(), userIds: usersInput('User IDs or usernames to add') },
  async run(ctx, args) {
    const failed = await ctx.telegramClient.addGroupMembers(args.channelId, args.userIds);
    return { channelId: args.channelId, failed };
  },
  render(result, { print }) {
    if (result.failed.length) {
      print(`Some members failed: ${JSON.stringify(result.failed, null, 2)}`);
    } else {
      print('Members added.');
    }
  },
});

export const groupsMembersRemove = defineOperation({
  name: 'groupsMembersRemove',
  command: 'groups members remove',
  summary: 'Remove members',
  description: 'Removes members from a group.',
  annotations: { destructive: true },
  input: { channelId: groupInput(), userIds: usersInput('User IDs or usernames to remove') },
  async run(ctx, args) {
    const result = await ctx.telegramClient.removeGroupMembers(args.channelId, args.userIds);
    return { channelId: args.channelId, ...result };
  },
  render(result, { print }) {
    print(`Removed: ${result.removed.join(', ')}`);
    if (result.failed.length) {
      print(`Failed: ${JSON.stringify(result.failed, null, 2)}`);
    }
  },
});

export const groupsInviteLinkGet = defineOperation({
  name: 'groupsInviteLinkGet',
  command: 'groups invite get',
  summary: 'Get invite link',
  description: 'Gets the primary invite link of a group.',
  annotations: { readOnly: true },
  input: { channelId: groupInput() },
  run: async (ctx, args) => formatInviteLink(await ctx.telegramClient.getGroupInviteLink(args.channelId)),
  render: renderInviteLink,
});

export const groupsInviteLinkRevoke = defineOperation({
  name: 'groupsInviteLinkRevoke',
  command: 'groups invite revoke',
  summary: 'Revoke invite link',
  description: 'Revokes the primary invite link of a group and returns the new one.',
  annotations: { destructive: true },
  input: { channelId: groupInput() },
  async run(ctx, args) {
    const existing = await ctx.telegramClient.getGroupInviteLink(args.channelId);
    return formatInviteLink(await ctx.telegramClient.revokeGroupInviteLink(args.channelId, existing));
  },
  render: renderInviteLink,
});

export const groupsJoin = defineOperation({
  name: 'groupsJoin',
  command: 'groups join',
  summary: 'Join via invite code',
  description: 'Joins a group by invite link, invite code or username.',
  annotations: { idempotent: true },
  input: {
    invite: field.text('Invite link, code or @username').flag('--code <invite-code>', { help: 'Invite code' }),
  },
  refine(args, fail) {
    if (!args.invite.trim()) {
      fail('{invite} must be an invite link, code or @username');
    }
  },
  async run(ctx, args) {
    const chat = await ctx.telegramClient.joinGroup(normalizeInviteCode(args.invite));
    return {
      id: chat.id?.toString?.() ?? null,
      title: chat.displayName || chat.title || 'Unknown',
      username: chat.username ?? null,
      chatType: typeof chat.chatType === 'string' ? chat.chatType : null,
    };
  },
  render(result, { print }) {
    print(`Joined: ${result.title}`);
  },
});

export const groupsLeave = defineOperation({
  name: 'groupsLeave',
  command: 'groups leave',
  summary: 'Leave group',
  description: 'Leaves a group chat or channel.',
  annotations: { destructive: true },
  input: { channelId: groupInput() },
  async run(ctx, args) {
    await ctx.telegramClient.leaveGroup(args.channelId);
    return { channelId: args.channelId, left: true };
  },
  render(result, { print }) {
    print(`Left ${result.channelId}`);
  },
});

export const GROUP_OPERATIONS = [
  groupsList,
  groupsInfo,
  groupsRename,
  groupsMembersAdd,
  groupsMembersRemove,
  groupsInviteLinkGet,
  groupsInviteLinkRevoke,
  groupsJoin,
  groupsLeave,
];
