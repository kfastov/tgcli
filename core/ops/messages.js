// Reading messages: list, search, show and context, from the archive, from Telegram,
// or both.

import { defineOperation, field } from './operation.js';
import { archiveKey, archiveKeys, chatOption, toChatList } from './chat-keys.js';

// --- reading from one source or both ---

// Resolve a single result (a message or a context bundle) from one source,
// shared by the message ops that return one item. fetchLive/fetchArchive each
// return the formatted result or null when absent; the whole result comes from a
// single source so live and archive data never mix:
//   live  -> live only
//   both  -> live first, then archive
//   archive -> archive first, then live as a fallback (flags usedLiveFallback)
// Returns { result, source, usedLiveFallback }, or null when nothing is found.
async function resolveFromSource({ source, fetchLive, fetchArchive }) {
  let result = null;
  let resolvedFrom = null;
  let usedLiveFallback = false;

  if (source === 'live' || source === 'both') {
    result = await fetchLive();
    if (result) {
      resolvedFrom = 'live';
    }
  }

  if (!result && (source === 'archive' || source === 'both')) {
    result = await fetchArchive();
    if (result) {
      resolvedFrom = 'archive';
    }
  }

  if (!result && source === 'archive') {
    result = await fetchLive();
    if (result) {
      resolvedFrom = 'live';
      usedLiveFallback = true;
    }
  }

  if (!result) {
    return null;
  }
  return { result, source: resolvedFrom ?? source, usedLiveFallback };
}

function parseDateMs(value, label) {
  if (!value) {
    return null;
  }
  const ts = Date.parse(value);
  if (Number.isNaN(ts)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return ts;
}

function filterLiveMessagesByDate(messages, fromDate, toDate) {
  const fromMs = parseDateMs(fromDate, 'after');
  const toMs = parseDateMs(toDate, 'before');
  if (!fromMs && !toMs) {
    return messages;
  }
  return messages.filter((message) => {
    const ts = typeof message.date === 'number' ? message.date * 1000 : null;
    if (!ts) {
      return false;
    }
    if (fromMs && ts < fromMs) {
      return false;
    }
    if (toMs && ts > toMs) {
      return false;
    }
    return true;
  });
}

function getMessageIdValue(message) {
  const value = message?.messageId ?? message?.id;
  return Number.isFinite(value) ? value : null;
}

function filterMessagesById(messages, beforeId, afterId) {
  if (!beforeId && !afterId) {
    return messages;
  }
  return messages.filter((message) => {
    const messageId = getMessageIdValue(message);
    if (!messageId) {
      return false;
    }
    if (beforeId && messageId >= beforeId) {
      return false;
    }
    if (afterId && messageId <= afterId) {
      return false;
    }
    return true;
  });
}

function formatLiveMessage(message, context) {
  const dateIso = message.date ? new Date(message.date * 1000).toISOString() : null;
  return {
    channelId: context.channelId ?? message.peer_id ?? null,
    peerTitle: context.peerTitle ?? null,
    username: context.username ?? null,
    messageId: message.id,
    date: dateIso,
    fromId: message.from_id ?? null,
    fromUsername: message.from_username ?? null,
    fromDisplayName: message.from_display_name ?? null,
    fromPeerType: message.from_peer_type ?? null,
    fromIsBot: typeof message.from_is_bot === 'boolean' ? message.from_is_bot : null,
    text: message.text ?? message.message ?? '',
    urls: message.urls ?? null,
    media: message.media ?? null,
    topicId: message.topic_id ?? null,
  };
}

function messageDateMs(message) {
  const ts = Date.parse(message.date ?? '');
  return Number.isNaN(ts) ? 0 : ts;
}

function mergeMessageSets(sets, limit) {
  const map = new Map();
  for (const list of sets) {
    for (const message of list) {
      const channelId = message.channelId ?? '';
      const messageId = message.messageId ?? message.id;
      const key = `${String(channelId)}:${String(messageId)}`;
      if (!map.has(key) || message.source === 'live') {
        map.set(key, message);
      }
    }
  }
  const merged = Array.from(map.values());
  merged.sort((a, b) => messageDateMs(b) - messageDateMs(a));
  return limit && limit > 0 ? merged.slice(0, limit) : merged;
}

// Pick the final message set and the source label it reports, shared by the
// list/search ops. A 'both' request merges the two sets; otherwise the result is
// drawn from a single source so live and archive rows never mix. An archive
// request that fell back to live reports 'live'. Returns { messages, source }.
function selectMessageResults({ source, archivedResults, liveResults, usedLiveFallback, limit }) {
  if (source === 'both') {
    return { messages: mergeMessageSets([archivedResults, liveResults], limit), source };
  }
  if (source === 'live' || usedLiveFallback) {
    return { messages: liveResults, source: 'live' };
  }
  return { messages: archivedResults, source };
}

function buildMessageListPayload(source, messages, requestedLimit) {
  const hasMore = requestedLimit ? messages.length === requestedLimit : false;
  const payload = {
    source,
    returned: messages.length,
    hasMore,
    messages,
  };
  if (hasMore) {
    const nextBeforeId = getMessageIdValue(messages[messages.length - 1]);
    if (nextBeforeId) {
      payload.nextBeforeId = nextBeforeId;
    }
  }
  return payload;
}

// Resolve peerTitle/username for a channel, preferring cached metadata and
// falling back to a live peer lookup so live-formatted messages carry a label.
async function resolveLiveMetadata(ctx, channelId, fallback = {}) {
  const meta = ctx.messageSyncService.getChannelMetadata(channelId);
  let peerTitle = meta?.peerTitle ?? fallback.peerTitle ?? null;
  let username = meta?.username ?? fallback.username ?? null;
  if (!peerTitle || !username) {
    const live = await ctx.telegramClient.getPeerMetadata(channelId);
    peerTitle = peerTitle ?? live?.peerTitle ?? null;
    username = username ?? live?.username ?? null;
  }
  return { peerTitle, username };
}

async function fetchLiveMessagesForChannel(ctx, id, options = {}) {
  const { query, topicId, limit, beforeId, afterId } = options;
  let peerTitle = null;
  let username = null;
  let liveMessages = [];

  if (query) {
    const response = await ctx.telegramClient.searchChannelMessages(id, {
      query,
      limit,
      topicId,
      beforeId,
      afterId,
    });
    liveMessages = response.messages;
    peerTitle = response.peerTitle ?? null;
  } else if (topicId) {
    const response = await ctx.telegramClient.getTopicMessages(id, topicId, limit, {
      beforeId,
      afterId,
    });
    liveMessages = response.messages;
  } else {
    const fetchOptions = {};
    if (beforeId) fetchOptions.beforeId = beforeId;
    if (afterId) fetchOptions.afterId = afterId;
    const response = await ctx.telegramClient.getMessagesByChannelId(id, limit, fetchOptions);
    liveMessages = response.messages;
    peerTitle = response.peerTitle ?? null;
  }

  const meta = await resolveLiveMetadata(ctx, id, { peerTitle, username });
  return { messages: liveMessages, peerTitle: meta.peerTitle, username: meta.username };
}

// Lists messages from the archive and/or live API. With source 'archive' and no
// archived rows, falls back to live and flags usedLiveFallback.
async function listMessages(ctx, args) {
  const resolvedSource = args.source;
  const requested = toChatList(args.channelIds);
  // One key per chat for archive and live alike, or --source both shows a message twice.
  let channelIds = await archiveKeys(ctx, requested, { telegram: resolvedSource !== 'archive' });
  const topicId = args.topicId ?? null;
  const limit = args.limit;
  const beforeId = args.beforeId ?? null;
  const afterId = args.afterId ?? null;

  let archivedResults = [];
  let liveResults = [];
  let usedLiveFallback = false;

  const fetchLive = async (liveChannelIds) => {
    const results = [];
    for (const id of liveChannelIds) {
      const fetched = await fetchLiveMessagesForChannel(ctx, id, {
        topicId,
        limit,
        beforeId,
        afterId,
      });
      const filtered = filterMessagesById(
        filterLiveMessagesByDate(fetched.messages, args.fromDate, args.toDate),
        beforeId,
        afterId,
      );
      results.push(...filtered.map((message) => ({
        ...formatLiveMessage(message, {
          channelId: String(id),
          peerTitle: fetched.peerTitle,
          username: fetched.username,
        }),
        source: 'live',
      })));
    }
    return results;
  };

  if (resolvedSource === 'archive' || resolvedSource === 'both') {
    const archived = ctx.messageSyncService.listArchivedMessages({
      channelIds: channelIds.length ? channelIds : null,
      topicId,
      fromDate: args.fromDate,
      toDate: args.toDate,
      beforeId,
      afterId,
      limit,
    });
    archivedResults = archived.map((message) => ({ ...message, source: 'archive' }));
  }

  if (resolvedSource === 'live' || resolvedSource === 'both') {
    liveResults = await fetchLive(channelIds);
  }

  if (resolvedSource === 'archive' && archivedResults.length === 0 && channelIds.length) {
    channelIds = await archiveKeys(ctx, requested, { telegram: true });
    liveResults = await fetchLive(channelIds);
    usedLiveFallback = true;
  }

  const { messages, source: outputSource } = selectMessageResults({
    source: resolvedSource,
    archivedResults,
    liveResults,
    usedLiveFallback,
    limit,
  });

  return { ...buildMessageListPayload(outputSource, messages, limit), usedLiveFallback };
}

// Searches archive and/or live. Falls back to live when archive search is empty.
async function searchMessages(ctx, args) {
  const resolvedSource = args.source;
  const requested = toChatList(args.channelIds);
  let channelIds = await archiveKeys(ctx, requested, { telegram: resolvedSource !== 'archive' });
  const tagList = toChatList(args.tags);
  const topicId = args.topicId ?? null;
  const limit = args.limit;
  const beforeId = args.beforeId ?? null;
  const afterId = args.afterId ?? null;
  const caseInsensitive = args.caseInsensitive !== false;
  const query = args.query;

  let archivedResults = [];
  let liveResults = [];
  let usedLiveFallback = false;

  const buildLiveChannelIds = () => {
    let liveChannelIds = channelIds;
    if (!liveChannelIds.length && tagList.length) {
      const tagged = new Map();
      for (const tag of tagList) {
        const channels = ctx.messageSyncService.listTaggedChannels(tag, { limit: 200 });
        for (const channel of channels) {
          tagged.set(channel.channelId, channel);
        }
      }
      liveChannelIds = Array.from(tagged.keys());
    }
    return liveChannelIds;
  };

  const fetchLive = async (liveChannelIds) => {
    let liveRegex = null;
    if (args.regex) {
      try {
        liveRegex = new RegExp(args.regex, caseInsensitive ? 'i' : '');
      } catch (error) {
        throw new Error(`Invalid regex: ${error.message}`);
      }
    }
    const results = [];
    for (const id of liveChannelIds) {
      const fetched = await fetchLiveMessagesForChannel(ctx, id, {
        query,
        topicId,
        limit,
        beforeId,
        afterId,
      });
      let filtered = filterLiveMessagesByDate(fetched.messages, args.fromDate, args.toDate);
      filtered = filterMessagesById(filtered, beforeId, afterId);
      if (liveRegex) {
        filtered = filtered.filter((message) =>
          liveRegex.test(message.text ?? message.message ?? ''),
        );
      }
      results.push(...filtered.map((message) => ({
        ...formatLiveMessage(message, {
          channelId: String(id),
          peerTitle: fetched.peerTitle,
          username: fetched.username,
        }),
        source: 'live',
      })));
    }
    return results;
  };

  if (resolvedSource === 'archive' || resolvedSource === 'both') {
    const archived = ctx.messageSyncService.searchArchiveMessages({
      query,
      regex: args.regex,
      tags: tagList.length ? tagList : null,
      channelIds: channelIds.length ? channelIds : null,
      topicId,
      fromDate: args.fromDate,
      toDate: args.toDate,
      beforeId,
      afterId,
      limit,
      caseInsensitive,
    });
    archivedResults = archived.map((message) => ({ ...message, source: 'archive' }));
  }

  if (resolvedSource === 'live' || resolvedSource === 'both') {
    const liveChannelIds = buildLiveChannelIds();
    if (!liveChannelIds.length) {
      throw new Error(`No chat carries the tags ${tagList.join(', ')}, so there is none to search live.`);
    }
    liveResults = await fetchLive(liveChannelIds);
  }

  if (resolvedSource === 'archive' && archivedResults.length === 0) {
    if (requested.length) {
      channelIds = await archiveKeys(ctx, requested, { telegram: true });
    }
    const liveChannelIds = buildLiveChannelIds();
    if (liveChannelIds.length) {
      liveResults = await fetchLive(liveChannelIds);
      usedLiveFallback = true;
    }
  }

  const { messages, source: outputSource } = selectMessageResults({
    source: resolvedSource,
    archivedResults,
    liveResults,
    usedLiveFallback,
    limit,
  });

  return { source: outputSource, returned: messages.length, messages, usedLiveFallback };
}

async function getMessage(ctx, args) {
  const { channelId, messageId } = args;

  const fetchLive = async () => {
    const live = await ctx.telegramClient.getMessageById(channelId, messageId);
    if (!live) {
      return null;
    }
    const meta = await resolveLiveMetadata(ctx, channelId);
    return {
      ...formatLiveMessage(live, { channelId: String(channelId), ...meta }),
      source: 'live',
    };
  };

  const fetchArchive = async () => {
    const archived = ctx.messageSyncService.getArchivedMessage({ channelId: await archiveKey(ctx, channelId), messageId });
    return archived ? { ...archived, source: 'archive' } : null;
  };

  const resolved = await resolveFromSource({ source: args.source, fetchLive, fetchArchive });
  if (!resolved) {
    throw new Error('Message not found.');
  }

  return { source: resolved.source, message: resolved.result, usedLiveFallback: resolved.usedLiveFallback };
}

async function getMessageContext(ctx, args) {
  const { channelId, messageId } = args;
  const safeBefore = args.before;
  const safeAfter = args.after;

  const fetchLive = async () => {
    const liveContext = await ctx.telegramClient.getMessageContext(channelId, messageId, {
      before: safeBefore,
      after: safeAfter,
    });
    if (!liveContext.target) {
      return null;
    }
    const meta = await resolveLiveMetadata(ctx, channelId);
    const toLive = (message) => ({
      ...formatLiveMessage(message, { channelId: String(channelId), ...meta }),
      source: 'live',
    });
    return {
      target: toLive(liveContext.target),
      before: liveContext.before.map(toLive),
      after: liveContext.after.map(toLive),
    };
  };

  const fetchArchive = async () => {
    const archiveContext = ctx.messageSyncService.getArchivedMessageContext({
      channelId: await archiveKey(ctx, channelId),
      messageId,
      before: safeBefore,
      after: safeAfter,
    });
    if (!archiveContext.target) {
      return null;
    }
    return {
      target: { ...archiveContext.target, source: 'archive' },
      before: archiveContext.before.map((message) => ({ ...message, source: 'archive' })),
      after: archiveContext.after.map((message) => ({ ...message, source: 'archive' })),
    };
  };

  const resolved = await resolveFromSource({ source: args.source, fetchLive, fetchArchive });
  if (!resolved) {
    throw new Error('Message not found.');
  }

  return { source: resolved.source, ...resolved.result, usedLiveFallback: resolved.usedLiveFallback };
}

// --- human output ---

function getMessageSenderLabel(message) {
  return message.fromDisplayName || message.fromUsername || message.fromId || null;
}

function groupMessagesByChannel(messages) {
  const groups = new Map();
  for (const message of messages) {
    const channelId = message.channelId ?? 'unknown';
    const key = String(channelId);
    let group = groups.get(key);
    if (!group) {
      group = {
        channelId: key,
        peerTitle: message.peerTitle ?? null,
        username: message.username ?? null,
        messages: [],
      };
      groups.set(key, group);
    } else {
      if (!group.peerTitle && message.peerTitle) {
        group.peerTitle = message.peerTitle;
      }
      if (!group.username && message.username) {
        group.username = message.username;
      }
    }
    group.messages.push(message);
  }
  return Array.from(groups.values());
}

function formatPeerHeaderLabel(group) {
  const title = typeof group.peerTitle === 'string' ? group.peerTitle.trim() : '';
  let username = typeof group.username === 'string' ? group.username.trim() : '';
  if (username.startsWith('@')) {
    username = username.slice(1);
  }
  const handle = username ? `@${username}` : '';
  if (title) {
    if (handle && !title.includes(handle)) {
      return `"${title}" (${handle})`;
    }
    return `"${title}"`;
  }
  if (handle) {
    return `${handle}`;
  }
  return group.channelId || 'unknown';
}

function renderMessageGroups(messages, outputSource, print) {
  const groups = groupMessagesByChannel(messages);
  for (const group of groups) {
    const label = formatPeerHeaderLabel(group);
    const count = group.messages.length;
    print(`Showing ${count} message${count === 1 ? '' : 's'} for ${label}:`);
    for (const message of group.messages) {
      const sender = getMessageSenderLabel(message) || 'unknown';
      const text = (message.text || '').replace(/\s+/g, ' ').trim();
      const prefix = outputSource === 'both' ? `[${message.source}] ` : '';
      print(`${prefix}${message.date ?? ''} ${sender} #${message.messageId}: ${text}`);
    }
    if (groups.length > 1) {
      print('');
    }
  }
}

function colorizeNote(message) {
  if (process.env.NO_COLOR || !process.stdout.isTTY) {
    return message;
  }
  return `\x1b[33m${message}\x1b[0m`;
}

function printArchiveFallbackNote(channelIds, print) {
  if (!channelIds?.length) {
    return;
  }
  const prefix = 'Note:';
  if (channelIds.length === 1) {
    const id = channelIds[0];
    const message = `${prefix} no archived messages for ${id}. Showing live results. ` +
      `To archive: tgcli channels watch --chat ${id}; ` +
      'tgcli backfill --once (or --follow).';
    print(colorizeNote(message));
    return;
  }
  const message = `${prefix} no archived messages for chats: ${channelIds.join(', ')}. ` +
    'Showing live results. To archive: tgcli channels watch --chat <id>; ' +
    'tgcli backfill --once (or --follow).';
  print(colorizeNote(message));
}

// --- declarations ---

const source = () => field.choice(['archive', 'live', 'both'], 'Where to read: archive, live (Telegram), or both')
  .default('archive')
  .flag('--source <source>', { help: 'archive|live|both' });
const fromDate = () => field.date('Earliest ISO-8601 timestamp to include').optional()
  .flag('--after <iso>', { help: 'Filter messages after date' });
const toDate = () => field.date('Latest ISO-8601 timestamp to include').optional()
  .flag('--before <iso>', { help: 'Filter messages before date' });
const beforeId = () => field.int('Only messages older than this message ID').optional()
  .flag('--before-id <id>', { aliases: ['--offset-id <id>'] });
const afterId = () => field.int('Only messages newer than this message ID').optional()
  .flag('--after-id <id>');
const topicId = () => field.int('Forum topic ID').optional().flag('--topic <id>', { help: 'Forum topic id' });

function checkIdWindow(args, fail) {
  if (args.beforeId && args.afterId && args.afterId >= args.beforeId) {
    fail('{afterId} must be lower than {beforeId}');
  }
}

export const messagesList = defineOperation({
  name: 'messagesList',
  command: 'messages list',
  summary: 'List messages',
  description: 'Lists messages from the archive or live Telegram API, newest first.',
  annotations: { readOnly: true },
  input: {
    channelIds: field.chats('Chat IDs or usernames to read (one or a list)').optional()
      .flag('--chat <id|username>', { help: 'Channel identifier' }),
    topicId: topicId(),
    source: source(),
    fromDate: fromDate(),
    toDate: toDate(),
    limit: field.int('Maximum number of messages to return').default(50).flag('--limit <n>', { help: 'Limit results' }),
    beforeId: beforeId(),
    afterId: afterId(),
  },
  refine(args, fail) {
    if (args.source !== 'archive' && !toChatList(args.channelIds).length) {
      fail('{channelIds} is required for live source.');
    }
    checkIdWindow(args, fail);
  },
  run: listMessages,
  render(payload, { args, print }) {
    renderMessageGroups(payload.messages, payload.source, print);
    if (payload.usedLiveFallback) {
      printArchiveFallbackNote(toChatList(args.channelIds), print);
    }
  },
});

export const messagesSearch = defineOperation({
  name: 'messagesSearch',
  command: 'messages search',
  summary: 'Search messages',
  description: 'Searches messages across the archive or live Telegram API.',
  annotations: { readOnly: true },
  input: {
    query: field.textOrEmpty('Full-text query (archive) or search text (live)').optional()
      .flag('--query <text>', { help: 'Search query' })
      .positional('[query...]'),
    regex: field.textOrEmpty('Regex filter for message text').optional().flag('--regex <pattern>', { help: 'Regex pattern' }),
    source: source(),
    channelIds: field.chats('Chat IDs or usernames to search (one or a list)').optional()
      .flag('--chat <id|username>', { help: 'Channel identifier' }),
    tags: field.strings('Only chats carrying these tags').optional()
      .flag('--tag <tag>', { help: 'Filter by tag', aliases: [{ flag: '--tags <tags>', help: 'Comma-separated tags' }] }),
    topicId: topicId(),
    fromDate: fromDate(),
    toDate: toDate(),
    limit: field.int('Maximum number of matches to return').default(100).flag('--limit <n>', { help: 'Limit results' }),
    beforeId: beforeId(),
    afterId: afterId(),
    caseInsensitive: field.bool('Whether regex matching ignores case').default(true)
      .flag('--case-sensitive', { presence: false, help: 'Disable case-insensitive search' }),
  },
  refine(args, fail) {
    if (!args.query && !args.regex && !args.tags?.length) {
      fail('Provide {query}, {regex}, or {tags} for messages search.');
    }
    if (args.source !== 'archive' && !toChatList(args.channelIds).length && !args.tags?.length) {
      fail('{channelIds} or {tags} is required for live search.');
    }
    checkIdWindow(args, fail);
  },
  run: searchMessages,
  render(payload, { args, print }) {
    renderMessageGroups(payload.messages, payload.source, print);
    if (payload.usedLiveFallback) {
      // The hint names whatever the search resolved against.
      const chats = toChatList(args.channelIds);
      printArchiveFallbackNote(chats.length ? chats : args.tags, print);
    }
  },
});

export const messagesGet = defineOperation({
  name: 'messagesGet',
  command: 'messages show',
  summary: 'Show a message',
  description: 'Fetches a specific message from the archive or live Telegram API.',
  annotations: { readOnly: true },
  input: {
    channelId: chatOption(),
    messageId: field.int('Message ID').flag('--id <msgId>', { help: 'Message id' }),
    source: source(),
  },
  run: getMessage,
  render(payload, { args, print }) {
    print(JSON.stringify({ source: payload.source, message: payload.message }, null, 2));
    if (payload.usedLiveFallback) {
      printArchiveFallbackNote([args.channelId], print);
    }
  },
});

export const messagesContext = defineOperation({
  name: 'messagesContext',
  command: 'messages context',
  summary: 'Show message context',
  description: 'Returns surrounding messages for a target message.',
  annotations: { readOnly: true },
  input: {
    channelId: chatOption(),
    messageId: field.int('Message ID').flag('--id <msgId>', { help: 'Message id' }),
    source: source(),
    before: field.int('Number of messages to include before the target', { min: 0 }).default(20)
      .flag('--before <n>', { help: 'Messages before' }),
    after: field.int('Number of messages to include after the target', { min: 0 }).default(20)
      .flag('--after <n>', { help: 'Messages after' }),
  },
  run: getMessageContext,
  render(payload, { args, print }) {
    const { source: from, target, before, after } = payload;
    print(JSON.stringify({ source: from, target, before, after }, null, 2));
    if (payload.usedLiveFallback) {
      printArchiveFallbackNote([args.channelId], print);
    }
  },
});

export const MESSAGE_OPERATIONS = [messagesList, messagesSearch, messagesGet, messagesContext];
