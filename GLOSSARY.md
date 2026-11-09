# Glossary

## Chats

**Archive key** — the key the archive keeps a chat under: its Telegram id, marked as
mtcute marks it (`-100…` for channels and supergroups, `-…` for basic groups, the user id
for private chats). A username is resolved to the id before anything is written
(`core/chat-key.js`).

## Messages

A message lives in three forms; `core/message-codec.js` is the only module that turns
one into another, and the only one that reads Telegram's TL.

**Stored message** — what the archive keeps of a message in `messages.raw_json`, and what
the live reads of `telegram-client.js` return: the whole TL message under `raw`, a
snapshot of the sender (`from_id`, `from_username`, …) and a few fields older versions
read (`id`, `date`, `text`, `topic_id`, `media`). Made by `encodeMessage` from an mtcute
message, which knows the peers TL alone lacks.

**Canonical message** — the message decoded from a stored one by `decodeMessage`: id,
dates, sender, text, links, topic, reply, forward, service action, media, todo list,
poll, album. Archive reads decode `raw_json` every time, so a field the codec learns
reaches every archived message at once.

**Message view** — the JSON a caller gets for a message from `messages list|search|show|context`
and the MCP tools, projected from the canonical message by `toMessageView`
(`core/message-view.js`). Live and archive reads of a message give the same view, apart
from `source`.

**Reply** — a message that answers another (`replyTo`). In a forum every message of a
topic points at the topic's first message; only one that also names the topic answers a
message of its own.

**Topic** — the forum topic a message is in, or the thread of a channel post that a
comment in the discussion group belongs to (`topicId`).

**Imported message** — a message imported into Telegram from another app. Its `date` is
the day of the import; `forward.date` is when it was first sent.

## Archive

**Message archive** — the tables that hold messages (`messages`, `message_links`,
`message_media`, the FTS index `message_search`) with one write path and one read
projection (`core/message-archive.js`). The sync service decides what to write and when;
the archive decides how it is kept and read back.
