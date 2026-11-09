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

- `core/message-codec.js` is the only module that reads TL messages. `encodeMessage` turns
  an mtcute message into the **stored message** the archive keeps (the same keys as
  before, so older and newer versions read each other's rows). `decodeMessage` turns a
  stored message, live or parsed back from `raw_json`, into the **canonical message**.
- `core/message-view.js` projects a canonical message into the **message view**, the JSON
  that `messages list|search|show|context` return, for live and archive reads alike.
- `core/message-archive.js` owns the message tables (`messages`, `message_links`,
  `message_media`, the FTS index) behind one write path and one read projection.
- Archive reads decode `raw_json` on every read instead of storing derived columns. A new
  field is a codec change, and every archived message has it at once, without a migration
  or a refetch. Columns stay only for what SQL filters or indexes.

## Consequences

- Live and archive reads of a message return the same JSON, apart from `source`; a test
  holds them to it.
- Archive reads parse `raw_json` for the rows they return, about 50 µs a row measured on
  the real archive.
- The FTS index still covers message text only. Indexing todo items, poll answers or
  actions needs a reindex of the archive in the background, left for later.
- `raw_json` keeps its bloated JSON form of TL bytes. Storing binary TL would shrink the
  archive, at the cost of a migration of every row, also left for later.
