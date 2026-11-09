// The message view: the JSON `messages list|search|show|context` return for a message,
// for live and archive reads alike. It is the one projection of the canonical message
// (core/message-codec.js), so a message reads the same whichever source it came from.

export function isoDate(seconds) {
  return Number.isFinite(seconds) && seconds > 0 ? new Date(seconds * 1000).toISOString() : null;
}

function forwardView(forward) {
  return forward ? { ...forward, date: isoDate(forward.date) } : null;
}

function todoView(todo) {
  if (!todo) {
    return null;
  }
  return {
    ...todo,
    items: todo.items.map((item) => ({ ...item, completedAt: isoDate(item.completedAt) })),
  };
}

// chat: { channelId, peerTitle, username } of the chat as the caller knows it.
// sender: { username, displayName, peerType, isBot } when the caller knows more of the
// sender than the message's own snapshot, as the archive's users table does.
export function toMessageView(message, { chat = {}, sender = null } = {}) {
  const from = sender ?? message.sender ?? {};
  return {
    channelId: chat.channelId ?? message.chatId ?? null,
    peerTitle: chat.peerTitle ?? null,
    username: chat.username ?? null,
    messageId: message.id,
    date: isoDate(message.date),
    editDate: isoDate(message.editDate),
    out: message.out,
    fromId: message.fromId,
    fromUsername: from.username ?? null,
    fromDisplayName: from.displayName ?? null,
    fromPeerType: from.peerType ?? null,
    fromIsBot: typeof from.isBot === 'boolean' ? from.isBot : null,
    text: message.text,
    urls: message.urls,
    media: message.media,
    topicId: message.topicId,
    replyTo: message.replyTo,
    forward: forwardView(message.forward),
    action: message.action,
    todo: todoView(message.todo),
    poll: message.poll,
    groupedId: message.groupedId,
    viaBotId: message.viaBotId,
    postAuthor: message.postAuthor,
  };
}

// One line of a message view for people: its text, or what the message holds when it
// has no text, marked when it answers or forwards another message.
export function describeMessageView(view) {
  const text = (view.text || '').replace(/\s+/g, ' ').trim();
  let body = text;
  if (view.action) {
    body = `[${view.action.type}]`;
  } else if (view.todo) {
    const done = view.todo.items.filter((item) => item.completedBy).length;
    body = `[todo: ${view.todo.title} (${done}/${view.todo.items.length} done)]`;
  } else if (view.poll) {
    body = `[poll: ${view.poll.question}]${text ? ` ${text}` : ''}`;
  } else if (!text && view.media?.type) {
    body = `[${view.media.type}]`;
  }
  const marks = [];
  if (view.replyTo?.messageId) {
    marks.push(`reply to #${view.replyTo.messageId}`);
  }
  if (view.forward) {
    marks.push(view.forward.imported ? `imported, sent ${view.forward.date ?? 'earlier'}` : 'forwarded');
  }
  return marks.length ? `(${marks.join(', ')}) ${body}`.trim() : body;
}
