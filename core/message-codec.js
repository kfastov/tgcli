// The message codec: the only module that reads Telegram's TL messages.
//
// A message lives in three forms, named in GLOSSARY.md:
//
//   mtcute Message --encodeMessage--> stored message --decodeMessage--> canonical message
//
// The stored message is what the archive keeps in `raw_json` and what the live reads of
// telegram-client.js hand around: a snapshot of the sender plus the whole TL message under
// `raw`. Its keys are the ones tgcli has always written, so older and newer versions read
// each other's rows. The canonical message is decoded from `raw` on every read, so a field
// added here shows up for every archived message at once, without a migration or a refetch.
//
// Everything here is a pure function of its input: no client, no database, no network.

import { Long, _messageMediaFromTl, getMarkedPeerId } from '@mtcute/core';

const PEER_CONSTRUCTORS = new Set(['peerUser', 'peerChat', 'peerChannel']);
// Keys of a service action that hold user ids, which tgcli writes as strings everywhere.
const USER_ID_KEYS = new Set(['users', 'userId', 'inviterId']);
// Keys of a service action that are noise or secrets in JSON.
const DROPPED_ACTION_KEYS = new Set(['accessHash', 'fileReference', 'bytes', 'sizes', 'videoSizes', 'stripped']);
const MAX_ACTION_DEPTH = 3;

function isTl(value) {
  return Boolean(value) && typeof value === 'object' && typeof value._ === 'string';
}

function isTlMessage(value) {
  return isTl(value) && (value._ === 'message' || value._ === 'messageService');
}

// --- stored TL ---

function isSerializedLong(value) {
  const keys = Object.keys(value);
  return keys.length === 3
    && typeof value.low === 'number'
    && typeof value.high === 'number'
    && typeof value.unsigned === 'boolean';
}

function isSerializedBytes(keys) {
  return keys.every((key, index) => key === String(index));
}

// JSON keeps a TL object's Long values as { low, high, unsigned } and its bytes as
// { "0": …, "1": … }. Every other object in TL has a constructor name in `_`, so an
// object without one is one of those two, and an empty one is empty bytes.
export function reviveTl(value) {
  if (Array.isArray(value)) {
    return value.map(reviveTl);
  }
  if (!value || typeof value !== 'object' || value instanceof Uint8Array || Long.isLong(value)) {
    return value;
  }
  if (!('_' in value)) {
    if (isSerializedLong(value)) {
      return Long.fromBits(value.low, value.high, value.unsigned);
    }
    const keys = Object.keys(value);
    if (isSerializedBytes(keys)) {
      return Uint8Array.from(keys, (key) => value[key]);
    }
  }
  const revived = {};
  for (const [key, entry] of Object.entries(value)) {
    revived[key] = reviveTl(entry);
  }
  return revived;
}

// Parses the `raw_json` of an archived row into a stored message, or null when the row
// holds none.
export function parseStoredMessage(rawJson) {
  if (typeof rawJson !== 'string' || !rawJson) {
    return null;
  }
  let parsed;
  try {
    parsed = JSON.parse(rawJson);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object') {
    return null;
  }
  return isTl(parsed.raw) ? { ...parsed, raw: reviveTl(parsed.raw) } : parsed;
}

// --- peers ---

function peerKey(peer) {
  if (!isTl(peer) || !PEER_CONSTRUCTORS.has(peer._)) {
    return null;
  }
  try {
    return String(getMarkedPeerId(peer));
  } catch {
    return null;
  }
}

function idString(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (Long.isLong(value)) {
    return value.toString();
  }
  if (typeof value === 'number' || typeof value === 'bigint' || typeof value === 'string') {
    return String(value);
  }
  return peerKey(value);
}

// The archive's word for the kind of chat or sender, from an mtcute User or Chat.
export function normalizePeerType(peer) {
  if (!peer) return 'chat';
  if (peer.type === 'user' || peer.type === 'bot') return 'user';
  if (peer.type === 'channel') return 'channel';
  if (peer.type === 'chat' && peer.chatType && peer.chatType !== 'group') return 'channel';
  return 'chat';
}

function displayNameOf(peer) {
  if (typeof peer?.displayName === 'string' && peer.displayName.trim()) {
    return peer.displayName.trim();
  }
  const parts = [peer?.firstName, peer?.lastName].filter(Boolean);
  return parts.length ? parts.join(' ') : null;
}

// What an mtcute User or Chat tells about a sender, for the archive's users table.
export function describePeer(peer) {
  if (!peer?.id) {
    return null;
  }
  return {
    id: peer.id.toString(),
    peerType: normalizePeerType(peer),
    username: typeof peer.username === 'string' && peer.username ? peer.username : null,
    displayName: displayNameOf(peer),
    phone: typeof peer.phoneNumber === 'string' && peer.phoneNumber.trim() ? peer.phoneNumber.trim() : null,
    isContact: typeof peer.isContact === 'boolean' ? peer.isContact : null,
    isBot: typeof peer.isBot === 'boolean' ? peer.isBot : null,
  };
}

// --- media ---

export function normalizeMediaText(value) {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value);
  }
  return null;
}

function normalizeMediaNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

// mtcute media getters throw for what a file lacks, such as the file id of a poll.
function readMediaProperty(media, prop) {
  if (!media) {
    return null;
  }
  try {
    return media[prop];
  } catch {
    return null;
  }
}

function buildWebpageExtras(media) {
  if (!media || media.type !== 'webpage' || !media.preview) {
    return null;
  }
  const preview = media.preview;
  const previewData = {
    url: preview.url ?? null,
    displayUrl: preview.displayUrl ?? null,
    siteName: preview.siteName ?? null,
    title: preview.title ?? null,
    description: preview.description ?? null,
    author: preview.author ?? null,
    previewType: preview.previewType ?? null,
  };
  const extras = {};
  if (Object.values(previewData).some((value) => value)) {
    extras.preview = previewData;
  }
  if (typeof media.displaySize === 'string') {
    extras.displaySize = media.displaySize;
  }
  if (typeof media.manual === 'boolean') {
    extras.manual = media.manual;
  }
  return Object.keys(extras).length ? extras : null;
}

// The media summary of an mtcute media object (Photo, Document, WebPage…): what a reader
// needs to tell the file and fetch it.
export function summarizeMedia(media) {
  if (!media || typeof media !== 'object') {
    return null;
  }
  const type = normalizeMediaText(media.type);
  if (!type) {
    return null;
  }

  const summary = {
    type,
    fileId: normalizeMediaText(readMediaProperty(media, 'fileId') ?? media.file_id),
    uniqueFileId: normalizeMediaText(readMediaProperty(media, 'uniqueFileId') ?? media.unique_file_id),
    fileName: normalizeMediaText(readMediaProperty(media, 'fileName') ?? media.file_name),
    mimeType: normalizeMediaText(readMediaProperty(media, 'mimeType') ?? media.mime_type),
    fileSize: normalizeMediaNumber(readMediaProperty(media, 'fileSize') ?? media.file_size),
    width: normalizeMediaNumber(readMediaProperty(media, 'width') ?? media.width),
    height: normalizeMediaNumber(readMediaProperty(media, 'height') ?? media.height),
    duration: normalizeMediaNumber(readMediaProperty(media, 'duration') ?? media.duration),
    extras: null,
  };

  if (!summary.mimeType && type === 'photo') {
    summary.mimeType = 'image/jpeg';
  }

  summary.extras = media.extras && typeof media.extras === 'object'
    ? media.extras
    : buildWebpageExtras(media);

  if (type === 'webpage' && media.preview) {
    const previewSummary = summarizeMedia(media.preview.document ?? media.preview.photo ?? null);
    if (previewSummary) {
      for (const key of ['fileId', 'uniqueFileId', 'fileName', 'mimeType', 'fileSize', 'width', 'height', 'duration']) {
        summary[key] = summary[key] ?? previewSummary[key];
      }
    }
  }

  return summary;
}

// mtcute needs peers for the media of a few kinds, polls among them, and the stored
// summary, made with peers at hand, answers for those.
function decodeMedia(raw, stored) {
  if (raw._ === 'message' && isTl(raw.media) && raw.media._ !== 'messageMediaEmpty') {
    try {
      const summary = summarizeMedia(_messageMediaFromTl(null, raw.media));
      if (summary) {
        return summary;
      }
    } catch {
      // falls back to the stored summary
    }
  }
  return stored?.media ?? null;
}

// --- content ---

function textOf(value) {
  if (typeof value === 'string') {
    return value;
  }
  if (isTl(value) && typeof value.text === 'string') {
    return value.text;
  }
  return '';
}

function extractUrls(text, entities) {
  if (!Array.isArray(entities)) {
    return null;
  }
  const urls = [];
  for (const entity of entities) {
    if (entity?._ === 'messageEntityTextUrl' && entity.url) {
      urls.push(entity.url);
    } else if (entity?._ === 'messageEntityUrl') {
      const url = text.slice(entity.offset, entity.offset + entity.length);
      if (url) {
        urls.push(url);
      }
    }
  }
  return urls.length ? urls : null;
}

// The forum topic a message is in, or the thread of a channel post it comments on.
function decodeTopicId(raw) {
  const header = raw.replyTo;
  if (header?._ === 'messageReplyHeader') {
    const topicId = header.forumTopic
      ? header.replyToTopId ?? header.replyToMsgId
      : header.replyToTopId;
    if (topicId) {
      return topicId;
    }
  }
  if (raw._ === 'messageService' && raw.action?._ === 'messageActionTopicCreate') {
    return raw.id;
  }
  return null;
}

// In a forum every message of a topic points at the topic's first message, and only one
// that also names the topic answers a message of its own.
function decodeReplyTo(raw) {
  const header = raw.replyTo;
  if (header?._ === 'messageReplyStoryHeader') {
    return { messageId: null, topId: null, chatId: peerKey(header.peer), storyId: header.storyId ?? null, quote: null };
  }
  if (header?._ !== 'messageReplyHeader') {
    return null;
  }
  if (header.forumTopic && !header.replyToTopId) {
    return null;
  }
  if (!header.replyToMsgId && !header.replyToPeerId) {
    return null;
  }
  return {
    messageId: header.replyToMsgId ?? null,
    topId: header.replyToTopId ?? null,
    chatId: peerKey(header.replyToPeerId),
    storyId: null,
    quote: typeof header.quoteText === 'string' && header.quoteText ? header.quoteText : null,
  };
}

// A message imported from another app carries the date it was first sent here.
function decodeForward(raw) {
  const header = raw.fwdFrom;
  if (header?._ !== 'messageFwdHeader') {
    return null;
  }
  return {
    date: header.date ?? null,
    fromId: peerKey(header.fromId),
    fromName: header.fromName ?? null,
    messageId: header.channelPost ?? null,
    postAuthor: header.postAuthor ?? null,
    imported: Boolean(header.imported),
  };
}

function lowerFirst(value) {
  return value ? value[0].toLowerCase() + value.slice(1) : value;
}

function actionValue(key, value, depth) {
  if (value === null || value === undefined || value instanceof Uint8Array) {
    return undefined;
  }
  if (Long.isLong(value)) {
    return value.toString();
  }
  if (Array.isArray(value)) {
    return value.map((entry) => actionValue(key, entry, depth)).filter((entry) => entry !== undefined);
  }
  if (isTl(value)) {
    if (PEER_CONSTRUCTORS.has(value._)) {
      return peerKey(value);
    }
    if (value._ === 'textWithEntities') {
      return value.text;
    }
    return depth < MAX_ACTION_DEPTH ? actionObject(value, depth + 1) : { type: value._ };
  }
  if (typeof value === 'object') {
    return undefined;
  }
  return USER_ID_KEYS.has(key) && typeof value === 'number' ? String(value) : value;
}

function actionObject(tl, depth) {
  const result = { type: tl._ };
  for (const [key, value] of Object.entries(tl)) {
    if (key === '_' || DROPPED_ACTION_KEYS.has(key)) {
      continue;
    }
    const converted = actionValue(key, value, depth);
    if (converted !== undefined) {
      result[key] = converted;
    }
  }
  return result;
}

// What a service message did: { type: 'chatAddUser', users: ['…'] }, named after the TL
// constructor without its messageAction prefix.
function decodeAction(raw) {
  if (raw._ !== 'messageService' || !isTl(raw.action)) {
    return null;
  }
  const action = actionObject(raw.action, 1);
  action.type = lowerFirst(raw.action._.replace(/^messageAction/, ''));
  return action;
}

function decodeTodo(raw) {
  const media = raw.media;
  if (media?._ !== 'messageMediaToDo' || !isTl(media.todo)) {
    return null;
  }
  const completions = new Map();
  for (const completion of media.completions ?? []) {
    completions.set(completion.id, completion);
  }
  return {
    title: textOf(media.todo.title),
    othersCanAppend: Boolean(media.todo.othersCanAppend),
    othersCanComplete: Boolean(media.todo.othersCanComplete),
    items: (media.todo.list ?? []).map((item) => {
      const completion = completions.get(item.id);
      return {
        id: item.id,
        text: textOf(item.title),
        completedBy: completion ? idString(completion.completedBy) : null,
        completedAt: completion?.date ?? null,
      };
    }),
  };
}

function sameBytes(a, b) {
  if (!(a instanceof Uint8Array) || !(b instanceof Uint8Array) || a.length !== b.length) {
    return false;
  }
  return a.every((byte, index) => byte === b[index]);
}

function decodePoll(raw) {
  const media = raw.media;
  if (media?._ !== 'messageMediaPoll' || !isTl(media.poll)) {
    return null;
  }
  const results = media.results?.results ?? [];
  return {
    question: textOf(media.poll.question),
    closed: Boolean(media.poll.closed),
    quiz: Boolean(media.poll.quiz),
    multipleChoice: Boolean(media.poll.multipleChoice),
    publicVoters: Boolean(media.poll.publicVoters),
    totalVoters: media.results?.totalVoters ?? null,
    answers: (media.poll.answers ?? []).map((answer) => {
      const result = results.find((entry) => sameBytes(entry.option, answer.option));
      return {
        text: textOf(answer.text),
        voters: result?.voters ?? null,
        chosen: Boolean(result?.chosen),
        correct: typeof result?.correct === 'boolean' ? result.correct : null,
      };
    }),
  };
}

// Whom the message is from, as mtcute's Message.sender tells it: a message without a
// sender in a private chat is the other user's, anywhere else the chat's own.
function decodeFromId(raw) {
  if (raw.fromId) {
    return peerKey(raw.fromId);
  }
  if (raw.peerId?._ === 'peerUser') {
    return String(raw.peerId.userId);
  }
  return peerKey(raw.peerId);
}

function decodeSender(stored) {
  const sender = {
    username: stored?.from_username ?? null,
    displayName: stored?.from_display_name ?? null,
    peerType: stored?.from_peer_type ?? null,
    isBot: typeof stored?.from_is_bot === 'boolean' ? stored.from_is_bot : null,
  };
  return Object.values(sender).some((value) => value !== null) ? sender : null;
}

function storedFromId(stored) {
  const fromId = stored?.from_id;
  return fromId && fromId !== 'unknown' ? String(fromId) : null;
}

function decodeTl(raw, stored) {
  const text = raw._ === 'message' ? textOf(raw.message) : '';
  return {
    id: raw.id,
    chatId: peerKey(raw.peerId),
    date: raw.date ?? null,
    editDate: raw._ === 'message' ? raw.editDate ?? null : null,
    out: Boolean(raw.out),
    pinned: Boolean(raw.pinned),
    post: Boolean(raw.post),
    fromId: storedFromId(stored) ?? decodeFromId(raw),
    sender: decodeSender(stored),
    text,
    urls: raw._ === 'message' ? extractUrls(text, raw.entities) : null,
    topicId: decodeTopicId(raw),
    replyTo: decodeReplyTo(raw),
    forward: raw._ === 'message' ? decodeForward(raw) : null,
    action: decodeAction(raw),
    media: decodeMedia(raw, stored),
    todo: raw._ === 'message' ? decodeTodo(raw) : null,
    poll: raw._ === 'message' ? decodePoll(raw) : null,
    groupedId: raw._ === 'message' ? idString(raw.groupedId) : null,
    viaBotId: raw._ === 'message' ? idString(raw.viaBotId) : null,
    postAuthor: raw._ === 'message' ? raw.postAuthor ?? null : null,
  };
}

// A stored message with no TL under `raw`, as test doubles and the oldest rows have.
function decodeLegacy(stored) {
  const text = typeof stored.text === 'string' ? stored.text : (typeof stored.message === 'string' ? stored.message : '');
  return {
    id: Number.isFinite(stored.id) ? stored.id : Number(stored.id ?? 0),
    chatId: stored.peer_id && stored.peer_id !== 'unknown' ? String(stored.peer_id) : null,
    date: Number.isFinite(stored.date) ? stored.date : null,
    editDate: null,
    out: false,
    pinned: false,
    post: false,
    fromId: storedFromId(stored),
    sender: decodeSender(stored),
    text,
    urls: Array.isArray(stored.urls) && stored.urls.length ? stored.urls : null,
    topicId: stored.topic_id ?? null,
    replyTo: null,
    forward: null,
    action: null,
    media: stored.media ?? null,
    todo: null,
    poll: null,
    groupedId: null,
    viaBotId: null,
    postAuthor: null,
  };
}

// The canonical message of a stored message, live or parsed back from the archive.
export function decodeMessage(stored) {
  if (!stored || typeof stored !== 'object') {
    return null;
  }
  return isTlMessage(stored.raw) ? decodeTl(stored.raw, stored) : decodeLegacy(stored);
}

// --- encoding ---

function messageDateSeconds(message) {
  if (message.date instanceof Date) {
    return Math.floor(message.date.getTime() / 1000);
  }
  return typeof message.date === 'number' ? Math.floor(message.date) : null;
}

function messageText(message) {
  if (typeof message.text === 'string') return message.text;
  if (typeof message.message === 'string') return message.message;
  if (message.text && typeof message.text.toString === 'function') return message.text.toString();
  return '';
}

// mtcute getters for the sender or the chat throw when a peer is missing from the update.
function readPeer(message, key) {
  try {
    return message[key] ?? null;
  } catch {
    return null;
  }
}

// describePeer of a message's sender, for writes that have mtcute's peers at hand.
export function describeSender(message) {
  return describePeer(readPeer(message, 'sender') ?? readPeer(message, 'from') ?? readPeer(message, 'author'));
}

function fallbackFromId(message) {
  const rawFrom = message.fromId ?? message.raw?.fromId;
  if (rawFrom && typeof rawFrom === 'object') {
    return (rawFrom.userId ?? rawFrom.channelId ?? rawFrom.chatId ?? 'unknown').toString();
  }
  return rawFrom ? rawFrom.toString() : 'unknown';
}

// The stored message of an mtcute Message: what the archive keeps of it and what a live
// read returns. The sender and the chat come from mtcute's peers, which TL alone lacks.
export function encodeMessage(message) {
  const raw = isTlMessage(message?.raw) ? message.raw : null;
  const decoded = raw ? decodeTl(raw, null) : null;
  const sender = readPeer(message, 'sender') ?? readPeer(message, 'from') ?? readPeer(message, 'author');
  const chat = readPeer(message, 'chat');
  const describedSender = describeSender(message);
  const text = messageText(message);
  const id = typeof message.id === 'number' ? message.id : Number(message.id || 0);

  return {
    id,
    date: messageDateSeconds(message),
    message: text,
    text,
    urls: decoded?.urls ?? null,
    from_id: describedSender?.id ?? decoded?.fromId ?? fallbackFromId(message),
    from_username: describedSender?.username ?? null,
    from_display_name: describedSender?.displayName ?? null,
    from_peer_type: sender ? normalizePeerType(sender) : null,
    from_is_bot: describedSender?.isBot ?? null,
    peer_type: normalizePeerType(chat),
    peer_id: chat?.id?.toString?.() ?? decoded?.chatId ?? 'unknown',
    topic_id: decoded?.topicId ?? null,
    media: summarizeMedia(readPeer(message, 'media')),
    raw: message.raw ?? null,
  };
}
