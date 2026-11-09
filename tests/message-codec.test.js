// The codec against real mtcute messages: what it decodes from TL, and that a message
// decodes the same live and after a round trip through the archive's raw_json.
import { Long, Message, PeersIndex } from '@mtcute/core';
import { describe, expect, it } from 'vitest';

import {
  decodeMessage,
  encodeMessage,
  parseStoredMessage,
  reviveTl,
} from '../core/message-codec.js';
import { toMessageView } from '../core/message-view.js';

const SELF_ID = 700000001;
const FRIEND_ID = 700000002;
const BARE_CHANNEL_ID = 700000500;
const CHANNEL_ID = '-1000700000500';
const DATE = 1700000000;

const ACCESS_HASH = Long.fromNumber(1);
const peers = PeersIndex.from({
  users: [
    { _: 'user', id: SELF_ID, firstName: 'Me', accessHash: ACCESS_HASH, self: true },
    { _: 'user', id: FRIEND_ID, firstName: 'Friend', lastName: 'One', username: 'friend_one', accessHash: ACCESS_HASH },
  ],
  chats: [
    { _: 'channel', id: BARE_CHANNEL_ID, title: 'Group', accessHash: ACCESS_HASH, megagroup: true, forum: true },
  ],
});

function text(value) {
  return { _: 'textWithEntities', text: value, entities: [] };
}

function channelMessage(id, overrides = {}) {
  return new Message({
    _: 'message',
    id,
    date: DATE + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    message: `message ${id}`,
    ...overrides,
  }, peers);
}

function serviceMessage(id, action) {
  return new Message({
    _: 'messageService',
    id,
    date: DATE + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    action,
  }, peers);
}

const PHOTO = {
  _: 'messageMediaPhoto',
  photo: {
    _: 'photo',
    id: Long.fromString('5000000000000000001'),
    accessHash: Long.fromString('-4000000000000000001'),
    fileReference: new Uint8Array([1, 2, 3, 250]),
    date: DATE,
    sizes: [{ _: 'photoSize', type: 'x', w: 800, h: 600, size: 1000 }],
    dcId: 2,
  },
};

const TODO = {
  _: 'messageMediaToDo',
  todo: {
    _: 'todoList',
    othersCanAppend: true,
    title: text('Trip'),
    list: [
      { _: 'todoItem', id: 1, title: text('Tickets') },
      { _: 'todoItem', id: 2, title: text('Hotel') },
    ],
  },
  completions: [{ _: 'todoCompletion', id: 1, completedBy: { _: 'peerUser', userId: SELF_ID }, date: DATE + 50 }],
};

const POLL = {
  _: 'messageMediaPoll',
  poll: {
    _: 'poll',
    id: Long.fromNumber(77),
    question: text('Lunch?'),
    answers: [
      { _: 'pollAnswer', text: text('Yes'), option: new Uint8Array([0]) },
      { _: 'pollAnswer', text: text('No'), option: new Uint8Array([1]) },
    ],
  },
  results: {
    _: 'pollResults',
    results: [
      { _: 'pollAnswerVoters', option: new Uint8Array([0]), voters: 3, chosen: true },
      { _: 'pollAnswerVoters', option: new Uint8Array([1]), voters: 1 },
    ],
    totalVoters: 4,
  },
};

// What the archive keeps of a message and reads back.
function throughArchive(message) {
  return parseStoredMessage(JSON.stringify(encodeMessage(message)));
}

const examples = {
  'plain text with links': channelMessage(10, {
    message: 'see https://example.org and docs',
    entities: [
      { _: 'messageEntityUrl', offset: 4, length: 19 },
      { _: 'messageEntityTextUrl', offset: 28, length: 4, url: 'https://docs.example.org/' },
    ],
  }),
  'photo album part': channelMessage(11, { media: PHOTO, groupedId: Long.fromString('1300000000000000001'), message: '' }),
  'reply in a topic': channelMessage(12, {
    replyTo: { _: 'messageReplyHeader', forumTopic: true, replyToMsgId: 9, replyToTopId: 5, quoteText: 'quoted' },
  }),
  'imported message': channelMessage(13, {
    fwdFrom: { _: 'messageFwdHeader', imported: true, date: DATE - 86400 * 400, fromName: 'Old Contact' },
  }),
  'own edited message': channelMessage(14, { out: true, fromId: { _: 'peerUser', userId: SELF_ID }, editDate: DATE + 999 }),
  'member added': serviceMessage(15, { _: 'messageActionChatAddUser', users: [SELF_ID] }),
  'todo list': channelMessage(16, { media: TODO, message: '' }),
  poll: channelMessage(17, { media: POLL, message: '' }),
};

describe('message codec', () => {
  it.each(Object.entries(examples))('decodes %s the same live and from the archive', (_name, message) => {
    const live = decodeMessage(encodeMessage(message));
    const archived = decodeMessage(throughArchive(message));
    expect(archived).toEqual(live);
  });

  it('keeps the keys older versions write and read', () => {
    const stored = encodeMessage(examples['plain text with links']);
    expect(Object.keys(stored).sort()).toEqual([
      'date', 'from_display_name', 'from_id', 'from_is_bot', 'from_peer_type', 'from_username',
      'id', 'media', 'message', 'peer_id', 'peer_type', 'raw', 'text', 'topic_id', 'urls',
    ]);
    expect(stored).toMatchObject({
      id: 10,
      date: DATE + 10,
      from_id: String(FRIEND_ID),
      from_username: 'friend_one',
      from_display_name: 'Friend One',
      from_peer_type: 'user',
      peer_id: CHANNEL_ID,
      peer_type: 'channel',
    });
  });

  it('takes urls from the entities', () => {
    expect(decodeMessage(throughArchive(examples['plain text with links'])).urls)
      .toEqual(['https://example.org', 'https://docs.example.org/']);
  });

  it('reads the media of a stored photo as mtcute reads the live one', () => {
    const live = encodeMessage(examples['photo album part']).media;
    const archived = decodeMessage(throughArchive(examples['photo album part']));
    expect(live).toMatchObject({ type: 'photo', width: 800, height: 600 });
    expect(archived.media).toEqual(live);
    expect(archived.groupedId).toBe('1300000000000000001');
  });

  it('tells a reply in a forum topic from a plain message of the topic', () => {
    const inTopic = decodeMessage(throughArchive(channelMessage(20, {
      replyTo: { _: 'messageReplyHeader', forumTopic: true, replyToMsgId: 5 },
    })));
    expect(inTopic).toMatchObject({ topicId: 5, replyTo: null });

    const reply = decodeMessage(throughArchive(examples['reply in a topic']));
    expect(reply).toMatchObject({
      topicId: 5,
      replyTo: { messageId: 9, topId: 5, chatId: null, quote: 'quoted' },
    });
  });

  it('puts a comment on a channel post in the thread of the post', () => {
    const comment = decodeMessage(throughArchive(channelMessage(21, {
      replyTo: { _: 'messageReplyHeader', replyToMsgId: 30, replyToTopId: 25 },
    })));
    expect(comment).toMatchObject({ topicId: 25, replyTo: { messageId: 30, topId: 25 } });
  });

  it('keeps the date an imported message was first sent', () => {
    const decoded = decodeMessage(throughArchive(examples['imported message']));
    expect(decoded.forward).toEqual({
      date: DATE - 86400 * 400,
      fromId: null,
      fromName: 'Old Contact',
      messageId: null,
      postAuthor: null,
      imported: true,
    });
    expect(toMessageView(decoded).forward.date).toBe(new Date((DATE - 86400 * 400) * 1000).toISOString());
  });

  it('says what a service message did, with user ids as strings', () => {
    const decoded = decodeMessage(throughArchive(examples['member added']));
    expect(decoded).toMatchObject({ text: '', action: { type: 'chatAddUser', users: [String(SELF_ID)] } });
  });

  it('names a chat in a service action by its archive key', () => {
    const decoded = decodeMessage(throughArchive(serviceMessage(41, {
      _: 'messageActionChatMigrateTo', channelId: 700000700,
    })));
    expect(decoded.action).toEqual({ type: 'chatMigrateTo', channelId: '-1000700000700' });
  });

  it('puts the message that opens a forum topic in that topic', () => {
    const decoded = decodeMessage(throughArchive(serviceMessage(40, {
      _: 'messageActionTopicCreate', title: 'Plans', iconColor: 7322096,
    })));
    expect(decoded).toMatchObject({ topicId: 40, action: { type: 'topicCreate', title: 'Plans', iconColor: 7322096 } });
  });

  it('reads the items of a todo list and who completed them', () => {
    const decoded = decodeMessage(throughArchive(examples['todo list']));
    expect(decoded.todo).toEqual({
      title: 'Trip',
      othersCanAppend: true,
      othersCanComplete: false,
      items: [
        { id: 1, text: 'Tickets', completedBy: String(SELF_ID), completedAt: DATE + 50 },
        { id: 2, text: 'Hotel', completedBy: null, completedAt: null },
      ],
    });
  });

  it('reads the answers of a poll with their votes', () => {
    const decoded = decodeMessage(throughArchive(examples.poll));
    expect(decoded.poll).toEqual({
      question: 'Lunch?',
      closed: false,
      quiz: false,
      multipleChoice: false,
      publicVoters: false,
      totalVoters: 4,
      answers: [
        { text: 'Yes', voters: 3, chosen: true, correct: null },
        { text: 'No', voters: 1, chosen: false, correct: null },
      ],
    });
  });

  it('marks own and edited messages', () => {
    const decoded = decodeMessage(throughArchive(examples['own edited message']));
    expect(decoded).toMatchObject({ out: true, editDate: DATE + 999, fromId: String(SELF_ID) });
  });

  it('decodes a stored message without TL from its own fields', () => {
    expect(decodeMessage({ id: 3, date: DATE, text: 'hi', from_id: 'unknown', topic_id: 4 })).toMatchObject({
      id: 3,
      date: DATE,
      text: 'hi',
      fromId: null,
      topicId: 4,
      replyTo: null,
      media: null,
    });
  });
});

describe('reviveTl', () => {
  it('turns JSON forms of Long and bytes back into them', () => {
    const revived = reviveTl(JSON.parse(JSON.stringify({
      _: 'photo',
      id: Long.fromString('-5000000000000000001'),
      fileReference: new Uint8Array([0, 255]),
      empty: new Uint8Array(0),
      sizes: [{ _: 'photoSize', w: 1 }],
    })));
    expect(Long.isLong(revived.id)).toBe(true);
    expect(revived.id.toString()).toBe('-5000000000000000001');
    expect(revived.fileReference).toEqual(new Uint8Array([0, 255]));
    expect(revived.empty).toEqual(new Uint8Array(0));
    expect(revived.sizes).toEqual([{ _: 'photoSize', w: 1 }]);
  });

  it('reads nothing from a row that holds no JSON', () => {
    expect(parseStoredMessage(null)).toBeNull();
    expect(parseStoredMessage('{not json')).toBeNull();
  });
});
