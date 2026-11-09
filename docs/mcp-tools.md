# MCP tools

<!-- Generated from core/ops/ by `node scripts/gen-mcp-tools-doc.js`. Do not edit by hand. -->

MCP is off by default; set `mcp.enabled` to `true` in `config.json` and run `tgcli server`.
Every tool runs the same operation as its CLI command, through the same entry on the server,
and returns the JSON that command prints with `--json`. Params are checked strictly: a param
a tool does not take is an error.

## listChannels

Lists Telegram dialogs for the account, including unread counts; with a query, searches them by title or username.

CLI: `tgcli channels list` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `query` | text | no |  | Search by title or username |
| `limit` | a positive integer | no | `50` | Maximum number of dialogs to return |

## searchChannels

Search dialogs by title or username

MCP only · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `keywords` | non-empty text | yes |  | Keywords to search for in channel titles or usernames |
| `limit` | a positive integer | no | `100` | Maximum number of results to return |

## channelsShow

Returns a chat's archive record (sync state, cursors, metadata), or what Telegram says about a chat the archive does not track.

CLI: `tgcli channels show` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |

## channelsWatch

Subscribe a chat for archiving (enable sync, queue a backfill)

CLI: `tgcli channels watch` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |

## channelsUnwatch

Stops archiving a chat, or with missing, every chat doctor lists as missing from the dialog list with no answer from Telegram.

CLI: `tgcli channels unwatch` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | no |  | Chat ID or username |
| `missing` | true or false | no |  | Every chat doctor lists as missing from your dialogs with no answer from Telegram |

## markChannelRead

Marks a Telegram chat as read up to the given message ID (inclusive).

CLI: `tgcli channels mark-read` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `messageId` | a positive integer | yes |  | Mark as read up to this message ID (inclusive) |

## messagesList

Lists messages from the archive or live Telegram API, newest first.

CLI: `tgcli messages list` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelIds` | chat ids or usernames | no |  | Chat IDs or usernames to read (one or a list) |
| `topicId` | a positive integer | no |  | Forum topic ID |
| `source` | one of: archive, live, both | no | `"archive"` | Where to read: archive, live (Telegram), or both |
| `fromDate` | an ISO-8601 date | no |  | Earliest ISO-8601 timestamp to include |
| `toDate` | an ISO-8601 date | no |  | Latest ISO-8601 timestamp to include |
| `limit` | a positive integer | no | `50` | Maximum number of messages to return |
| `beforeId` | a positive integer | no |  | Only messages older than this message ID |
| `afterId` | a positive integer | no |  | Only messages newer than this message ID |

## messagesSearch

Searches messages across the archive or live Telegram API.

CLI: `tgcli messages search` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `query` | text | no |  | Full-text query (archive) or search text (live) |
| `regex` | text | no |  | Regex filter for message text |
| `source` | one of: archive, live, both | no | `"archive"` | Where to read: archive, live (Telegram), or both |
| `channelIds` | chat ids or usernames | no |  | Chat IDs or usernames to search (one or a list) |
| `tags` | a list of names | no |  | Only chats carrying these tags |
| `topicId` | a positive integer | no |  | Forum topic ID |
| `fromDate` | an ISO-8601 date | no |  | Earliest ISO-8601 timestamp to include |
| `toDate` | an ISO-8601 date | no |  | Latest ISO-8601 timestamp to include |
| `limit` | a positive integer | no | `100` | Maximum number of matches to return |
| `beforeId` | a positive integer | no |  | Only messages older than this message ID |
| `afterId` | a positive integer | no |  | Only messages newer than this message ID |
| `caseInsensitive` | true or false | no | `true` | Whether regex matching ignores case |

## messagesGet

Fetches a specific message from the archive or live Telegram API.

CLI: `tgcli messages show` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `messageId` | a positive integer | yes |  | Message ID |
| `source` | one of: archive, live, both | no | `"archive"` | Where to read: archive, live (Telegram), or both |

## messagesContext

Returns surrounding messages for a target message.

CLI: `tgcli messages context` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `messageId` | a positive integer | yes |  | Message ID |
| `source` | one of: archive, live, both | no | `"archive"` | Where to read: archive, live (Telegram), or both |
| `before` | a non-negative integer | no | `20` | Number of messages to include before the target |
| `after` | a non-negative integer | no | `20` | Number of messages to include after the target |

## messagesSend

Sends a text message to a chat, retrying on transient failures.

CLI: `tgcli send text` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username to send to |
| `text` | non-empty text | yes |  | Message text |
| `parseMode` | one of: markdown, html, none | no |  | Parse mode |
| `topicId` | a positive integer | no |  | Forum topic ID to send into |
| `replyToMessageId` | a positive integer | no |  | Message ID to reply to |
| `silent` | true or false | no |  | Send without notification sound |
| `noForwards` | true or false | no |  | Protect the message from forwarding |
| `noPreview` | true or false | no |  | Disable the link preview |
| `schedule` | an ISO-8601 date | no |  | Send at this ISO-8601 time instead of now (within 365 days) |
| `retries` | a non-negative integer | no | `2` | Max retries on failure |
| `retryBackoff` | non-empty text | no |  | Retry backoff in ms or strategy: constant\|linear\|exponential |
| `timeoutMs` | a non-negative integer | no |  | Give up on the send after this many milliseconds (0: never) |

## messagesSendPhoto

Sends a photo from a local file, with an optional caption.

CLI: `tgcli send photo` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username to send to |
| `photo` | non-empty text | yes |  | Path to the photo |
| `caption` | text | no |  | Caption |
| `captionAbove` | true or false | no |  | Show the caption above the media |
| `spoiler` | true or false | no |  | Blur the media until tapped |
| `parseMode` | one of: markdown, html, none | no |  | Parse mode for the caption |
| `topicId` | a positive integer | no |  | Forum topic ID to send into |
| `replyToMessageId` | a positive integer | no |  | Message ID to reply to |
| `silent` | true or false | no |  | Send without notification sound |
| `noForwards` | true or false | no |  | Protect the message from forwarding |
| `schedule` | an ISO-8601 date | no |  | Send at this ISO-8601 time instead of now (within 365 days) |
| `retries` | a non-negative integer | no | `2` | Max retries on failure |
| `retryBackoff` | non-empty text | no |  | Retry backoff in ms or strategy: constant\|linear\|exponential |
| `timeoutMs` | a non-negative integer | no |  | Give up on the send after this many milliseconds (0: never) |

## messagesSendFile

Sends a local file with an optional caption.

CLI: `tgcli send file` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username to send to |
| `filePath` | non-empty text | yes |  | Path to the file to upload |
| `caption` | text | no |  | Caption |
| `captionAbove` | true or false | no |  | Show the caption above the media |
| `spoiler` | true or false | no |  | Blur the media until tapped |
| `filename` | non-empty text | no |  | File name shown in Telegram |
| `forceDocument` | true or false | no |  | Send as an uncompressed document |
| `parseMode` | one of: markdown, html, none | no |  | Parse mode for the caption |
| `topicId` | a positive integer | no |  | Forum topic ID to send into |
| `replyToMessageId` | a positive integer | no |  | Message ID to reply to |
| `silent` | true or false | no |  | Send without notification sound |
| `noForwards` | true or false | no |  | Protect the message from forwarding |
| `schedule` | an ISO-8601 date | no |  | Send at this ISO-8601 time instead of now (within 365 days) |
| `retries` | a non-negative integer | no | `2` | Max retries on failure |
| `retryBackoff` | non-empty text | no |  | Retry backoff in ms or strategy: constant\|linear\|exponential |
| `timeoutMs` | a non-negative integer | no |  | Give up on the send after this many milliseconds (0: never) |

## mediaDownload

Downloads the media of a message to a file on the machine the server runs on, replacing a file already at that path.

CLI: `tgcli media download` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `messageId` | a positive integer | yes |  | ID of the message with the media |
| `outputPath` | non-empty text | no |  | File path or directory for the download (default: the store media directory) |

## topicsList

Lists forum topics of a supergroup.

CLI: `tgcli topics list` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Forum supergroup ID or username |
| `limit` | a positive integer | no | `100` | Maximum number of topics to return |

## topicsSearch

Searches forum topics of a supergroup by title.

CLI: `tgcli topics search` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Forum supergroup ID or username |
| `query` | non-empty text | yes |  | Search query for forum topic titles |
| `limit` | a positive integer | no | `100` | Maximum number of topics to return |

## setChannelTags

Replaces a chat's tags from one source, for later cross-chat search.

CLI: `tgcli tags set` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `tags` | a list of names | yes |  | Tags to attach to the chat; an empty list clears them |
| `source` | non-empty text | no |  | Tag source label (default: manual) |

## listChannelTags

Lists the tags attached to a chat.

CLI: `tgcli tags list` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |
| `source` | non-empty text | no |  | Only tags from this source |

## listTaggedChannels

Lists the chats that carry a tag.

CLI: `tgcli tags search` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `tag` | non-empty text | yes |  | Tag to look up |
| `source` | non-empty text | no |  | Only tags from this source |
| `limit` | a positive integer | no | `100` | Maximum number of chats to return |

## autoTagChannels

Tags chats from their title, username and cached metadata, replacing the tags it gave them before.

CLI: `tgcli tags auto` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelIds` | chat ids or usernames | no |  | Chats to tag (default: the archive picks) |
| `limit` | a positive integer | no | `50` | Maximum number of chats to tag |
| `source` | non-empty text | no |  | Tag source label (default: auto) |
| `refreshMetadata` | true or false | no | `true` | Refresh cached metadata before tagging |

## getChannelMetadata

Returns cached metadata for a chat, or what Telegram says when the archive has none.

CLI: `tgcli metadata get` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Chat ID or username |

## refreshChannelMetadata

Fetches and caches extended metadata for chats.

CLI: `tgcli metadata refresh` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelIds` | chat ids or usernames | no |  | Chats to refresh (default: the archive picks) |
| `limit` | a positive integer | no | `20` | Maximum number of chats to refresh |
| `force` | true or false | no |  | Refresh even if cached metadata is fresh |
| `onlyMissing` | true or false | no |  | Refresh only chats without cached metadata |

## contactsSearch

Searches contacts and users by name, username, alias, tags and notes.

CLI: `tgcli contacts search` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `query` | non-empty text | yes |  | Search query for contacts |
| `limit` | a positive integer | no | `50` | Maximum number of contacts to return |

## contactsGet

Returns a contact profile from the local store, refreshing contacts when it is missing.

CLI: `tgcli contacts show` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |

## contactsAliasSet

Sets an alias for a contact, replacing the one it had.

CLI: `tgcli contacts alias set` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |
| `alias` | non-empty text | yes |  | Alias for the contact |

## contactsAliasRemove

Removes the alias of a contact.

CLI: `tgcli contacts alias rm` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |

## contactsTagsAdd

Adds tags to a contact.

CLI: `tgcli contacts tags add` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |
| `tags` | a list of names | yes |  | Tags to add |

## contactsTagsRemove

Removes tags from a contact.

CLI: `tgcli contacts tags rm` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |
| `tags` | a list of names | yes |  | Tags to remove |

## contactsNotesSet

Sets the notes kept for a contact, replacing the old ones; empty text clears them.

CLI: `tgcli contacts notes set` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `userId` | a user id or username | yes |  | User ID or username |
| `notes` | text | yes |  | Notes to attach to the contact |

## groupsList

Lists the group chats and supergroups the account is in.

CLI: `tgcli groups list` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `query` | text | no |  | Search by title |
| `limit` | a positive integer | no | `100` | Maximum number of groups to return |

## groupsInfo

Fetches group information and metadata.

CLI: `tgcli groups info` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |

## groupsRename

Renames a group chat or supergroup.

CLI: `tgcli groups rename` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |
| `name` | non-empty text | yes |  | New group title |

## groupsMembersAdd

Adds members to a group.

CLI: `tgcli groups members add` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |
| `userIds` | user ids or usernames | yes |  | User IDs or usernames to add |

## groupsMembersRemove

Removes members from a group.

CLI: `tgcli groups members remove` · destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |
| `userIds` | user ids or usernames | yes |  | User IDs or usernames to remove |

## groupsInviteLinkGet

Gets the primary invite link of a group.

CLI: `tgcli groups invite get` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |

## groupsInviteLinkRevoke

Revokes the primary invite link of a group and returns the new one.

CLI: `tgcli groups invite revoke` · destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |

## groupsJoin

Joins a group by invite link, invite code or username.

CLI: `tgcli groups join` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `invite` | non-empty text | yes |  | Invite link, code or @username |

## groupsLeave

Leaves a group chat or channel.

CLI: `tgcli groups leave` · destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Group ID or username |

## foldersList

List all folders

CLI: `tgcli folders list` · read-only

No params.

## foldersShow

Show folder details

CLI: `tgcli folders show` · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `folder` | a folder id or title | yes |  | Folder ID or title |
| `resolve` | true or false | no |  | Resolve peer IDs to names (slower, requires API calls) |

## foldersCreate

Create a new folder

CLI: `tgcli folders create` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `title` | non-empty text | yes |  | Folder name (max 12 chars) |
| `emoji` | text | no |  | Emoji icon |
| `contacts` | true or false | no |  | Include contacts |
| `nonContacts` | true or false | no |  | Include non-contacts |
| `groups` | true or false | no |  | Include groups |
| `broadcasts` | true or false | no |  | Include channels |
| `bots` | true or false | no |  | Include bots |
| `excludeMuted` | true or false | no |  | Exclude muted |
| `excludeRead` | true or false | no |  | Exclude read |
| `excludeArchived` | true or false | no |  | Exclude archived |
| `includePeers` | chat ids or usernames | no |  | Chat to include (repeatable) |
| `excludePeers` | chat ids or usernames | no |  | Chat to exclude (repeatable) |
| `pinnedPeers` | chat ids or usernames | no |  | Pin chat in folder (repeatable) |

## foldersEdit

Edits a folder; settings left out stay as they are, and a non-empty chat list replaces the old one.

CLI: `tgcli folders edit` · destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `folder` | a folder id or title | yes |  | Folder ID or title |
| `title` | non-empty text | no |  | Folder name (max 12 chars) |
| `emoji` | text | no |  | Emoji icon |
| `contacts` | true or false | no |  | Include contacts |
| `nonContacts` | true or false | no |  | Include non-contacts |
| `groups` | true or false | no |  | Include groups |
| `broadcasts` | true or false | no |  | Include channels |
| `bots` | true or false | no |  | Include bots |
| `excludeMuted` | true or false | no |  | Exclude muted |
| `excludeRead` | true or false | no |  | Exclude read |
| `excludeArchived` | true or false | no |  | Exclude archived |
| `includePeers` | chat ids or usernames | no |  | Chat to include (repeatable) |
| `excludePeers` | chat ids or usernames | no |  | Chat to exclude (repeatable) |
| `pinnedPeers` | chat ids or usernames | no |  | Pin chat in folder (repeatable) |

## foldersDelete

Delete a folder

CLI: `tgcli folders delete` · destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `folder` | a folder id or title | yes |  | Folder ID or title |

## foldersReorder

Reorder folders

CLI: `tgcli folders reorder` · not destructive, idempotent

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `ids` | a list of integers | yes |  | Folder IDs in the desired order |

## foldersChatsAdd

Add chat to folder

CLI: `tgcli folders chats add` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `folder` | a folder id or title | yes |  | Folder ID or title |
| `channelId` | a chat id or username | yes |  | Chat ID or username |

## foldersChatsRemove

Remove chat from folder

CLI: `tgcli folders chats remove` · destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `folder` | a folder id or title | yes |  | Folder ID or title |
| `channelId` | a chat id or username | yes |  | Chat ID or username |

## foldersJoin

Join shared folder via invite link

CLI: `tgcli folders join` · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `link` | non-empty text | yes |  | Shared folder invite link |

## listActiveChannels

Lists dialogs tracked in the local archive registry.

MCP only · read-only

No params.

## scheduleMessageSync

Schedules a background job to archive channel messages locally.

MCP only · not destructive

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Numeric channel ID or username |
| `depth` | a positive integer up to 50000 | no |  | Maximum messages to retain per channel (default 1000) |
| `minDate` | an ISO-8601 date | no |  | Earliest ISO-8601 timestamp to backfill |

## getSyncedMessageStats

Returns summary statistics for stored messages in a channel.

MCP only · read-only

| Param | Takes | Required | Default | Description |
|---|---|---|---|---|
| `channelId` | a chat id or username | yes |  | Numeric channel ID or username |

## listMessageSyncJobs

Lists tracked message sync jobs and their current status.

MCP only · read-only

No params.
