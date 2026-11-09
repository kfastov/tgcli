// The message view: the JSON every message read returns, and the line people see.
import { describe, expect, it } from 'vitest';

import { describeMessageView, toMessageView } from '../core/message-view.js';

const DATE = 1700000000;

function canonical(overrides = {}) {
  return {
    id: 7,
    chatId: '-1000700000500',
    date: DATE,
    editDate: null,
    out: false,
    fromId: '700000002',
    sender: { username: 'friend_one', displayName: 'Friend One', peerType: 'user', isBot: false },
    text: 'hello',
    urls: null,
    topicId: null,
    replyTo: null,
    forward: null,
    action: null,
    media: null,
    todo: null,
    poll: null,
    groupedId: null,
    viaBotId: null,
    postAuthor: null,
    ...overrides,
  };
}

const iso = (seconds) => new Date(seconds * 1000).toISOString();

describe('toMessageView', () => {
  it('names the chat as the caller knows it and dates as ISO', () => {
    const view = toMessageView(canonical({ editDate: DATE + 60 }), {
      chat: { channelId: '-1000700000500', peerTitle: 'Group', username: 'group_chat' },
    });
    expect(view).toMatchObject({
      channelId: '-1000700000500',
      peerTitle: 'Group',
      username: 'group_chat',
      messageId: 7,
      date: iso(DATE),
      editDate: iso(DATE + 60),
      fromUsername: 'friend_one',
      fromDisplayName: 'Friend One',
      fromIsBot: false,
    });
  });

  it('takes the sender from the caller over the message snapshot', () => {
    const view = toMessageView(canonical(), { sender: { username: 'renamed', displayName: null, peerType: 'user', isBot: null } });
    expect(view).toMatchObject({ fromUsername: 'renamed', fromDisplayName: null, fromIsBot: null });
  });

  it('dates the forward and the completed todo items as ISO', () => {
    const view = toMessageView(canonical({
      forward: { date: DATE - 100, fromId: null, fromName: 'Old', messageId: null, postAuthor: null, imported: true },
      todo: { title: 'Trip', othersCanAppend: false, othersCanComplete: false, items: [
        { id: 1, text: 'Tickets', completedBy: '700000001', completedAt: DATE + 5 },
        { id: 2, text: 'Hotel', completedBy: null, completedAt: null },
      ] },
    }));
    expect(view.forward.date).toBe(iso(DATE - 100));
    expect(view.todo.items.map((item) => item.completedAt)).toEqual([iso(DATE + 5), null]);
  });
});

describe('describeMessageView', () => {
  const line = (overrides) => describeMessageView(toMessageView(canonical(overrides)));

  it('says what a message without text holds', () => {
    expect(line({ text: '', action: { type: 'chatAddUser', users: ['700000001'] } })).toBe('[chatAddUser]');
    expect(line({ text: '', todo: { title: 'Trip', items: [{ completedBy: '1' }, { completedBy: null }] } }))
      .toBe('[todo: Trip (1/2 done)]');
    expect(line({ text: '', poll: { question: 'Lunch?', answers: [] } })).toBe('[poll: Lunch?]');
    expect(line({ text: '', media: { type: 'photo' } })).toBe('[photo]');
  });

  it('keeps the text of a message with media, on one line', () => {
    expect(line({ text: 'look\n at  this', media: { type: 'photo' } })).toBe('look at this');
  });

  it('marks replies and imported messages', () => {
    expect(line({ replyTo: { messageId: 9, topId: 5 } })).toBe('(reply to #9) hello');
    expect(line({ forward: { date: DATE - 100, imported: true } })).toBe(`(imported, sent ${iso(DATE - 100)}) hello`);
    expect(line({ forward: { date: DATE - 100, imported: false } })).toBe('(forwarded) hello');
  });
});
