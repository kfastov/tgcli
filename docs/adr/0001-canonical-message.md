# 1. One canonical message, read from the stored TL

Date: 2026-11-09. Status: accepted.

## Context

The shape of a message was written down nowhere. `telegram-client.js` serialized an mtcute
message, the archive kept a narrow row plus that serialization in `raw_json`, and two
formatters turned it into JSON: `formatLiveMessage` for live reads and `formatArchivedRow`
for archive reads, behind five copies of the same SELECT list. Media was summarized twice.
The two forms drifted: live reads had `urls`, archive reads did not; neither had replies,
forwards, the original date of imported messages, service actions, todo lists or polls,
and adding one field meant about nine edits in three files.

Every archived row already keeps the whole TL message in `raw_json.raw`. JSON turns its
`Long` values into `{ low, high, unsigned }` and its bytes into `{ "0": …, "1": … }`, and
reviving both gives mtcute back an object it parses as it parsed the original: on a sample
of 5454 archived media messages the summary from the revived TL matched the stored one in
every case but nine polls, which need peers and are decoded by the codec itself.

## Decision

- `core/message-codec.js` is the only module that decodes TL messages. `encodeMessage` turns
  an mtcute message into the **stored message** the archive keeps (the same keys as
  before, so older and newer versions read each other's rows). `decodeMessage` turns a
  stored message, live or parsed back from `raw_json`, into the **canonical message**.
- `core/message-view.js` projects a canonical message into the **message view**, the JSON
  that `messages list|search|show|context` return, for live and archive reads alike.
- `core/message-archive.js` owns the message tables (`messages`, `message_links`,
  `message_media`, the FTS index): their schema, the one write path every message takes
  (one transaction with its senders) and the one read projection. Moving a chat from a
  username key to its id (`mergeChatKey`, `dropChatKey` in the sync service) still copies
  and deletes rows of these tables itself, as it does for every chat-keyed table; it moves
  rows as they are. The chat each row's TL names, which that move goes by, is read in SQL
  by the archive (`countByPeer`), the one place outside the codec that knows where TL sits
  in `raw_json`.
- Archive reads decode `raw_json` on every read instead of storing derived columns. A new
  field is a codec change, and every archived message has it at once, without a migration
  or a refetch. Columns stay only for what SQL filters or indexes: `date`, `topic_id`,
  `from_id`, the FTS text, and `message_links` / `message_media`, kept as indexes by link
  and by media type that no read uses yet.

## Consequences

- Live and archive reads of a message return the same JSON, apart from `source`; a test
  holds them to it. The one known gap is the sender's name and username: archive reads
  take them from the users table, which keeps the latest tgcli has seen (and a username
  the user has since dropped), live reads from the message's own snapshot.
- Archive reads parse `raw_json` for the rows they return: a read of one message took
  0.060 ms against 0.039 ms before, measured over 34260 messages copied from
  a real archive.
- Media is summarized at write time, with mtcute's peers, for `message_media` and as the
  stored message's `media`, and again from TL at read time. The stored summary answers
  for the few kinds mtcute cannot parse without peers, polls among them.

Left for later, each needing more than a codec change:

- The FTS index covers message text only. Indexing todo items, poll answers or actions
  needs a reindex of the archive in the background.
- Date filters and the order of reads use Telegram's date, which for an imported message
  is the day of the import; the original date is in `forward.date`. Filtering by it needs
  a column filled for rows already archived.
- An edit replaces the archived copy and a delete removes it; keeping revisions and
  tombstones changes what the archive is, beyond one model of a message.
- The canonical message carries `urls` from the entities, not the entities themselves,
  which would add a lot of JSON to every formatted message until a reader (an export)
  needs them. There is no `transcript` field until something transcribes voice messages.
- `raw_json` keeps its bloated JSON form of TL bytes. Storing binary TL would shrink the
  archive, at the cost of a migration of every row.
