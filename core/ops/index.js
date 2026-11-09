// Every operation the server runs, in the order the CLI help lists them.

import { CHANNEL_OPERATIONS } from './channels.js';
import { MESSAGE_OPERATIONS } from './messages.js';
import { SEND_OPERATIONS } from './send.js';
import { MEDIA_OPERATIONS } from './media.js';
import { TOPIC_OPERATIONS } from './topics.js';
import { TAG_OPERATIONS } from './tags.js';
import { METADATA_OPERATIONS } from './metadata.js';
import { CONTACT_OPERATIONS } from './contacts.js';
import { GROUP_OPERATIONS } from './groups.js';
import { FOLDER_OPERATIONS } from './folders.js';
import { ARCHIVE_OPERATIONS } from './archive.js';

export const OPERATION_LIST = Object.freeze([
  ...CHANNEL_OPERATIONS,
  ...MESSAGE_OPERATIONS,
  ...SEND_OPERATIONS,
  ...MEDIA_OPERATIONS,
  ...TOPIC_OPERATIONS,
  ...TAG_OPERATIONS,
  ...METADATA_OPERATIONS,
  ...CONTACT_OPERATIONS,
  ...GROUP_OPERATIONS,
  ...FOLDER_OPERATIONS,
  ...ARCHIVE_OPERATIONS,
]);

// By name: the control API dispatches only these.
export const OPERATIONS = Object.freeze(Object.fromEntries(OPERATION_LIST.map((op) => [op.name, op])));

// Help for the CLI command groups operations live under.
export const COMMAND_GROUPS = Object.freeze({
  channels: 'Channel discovery and settings',
  messages: 'List and search messages',
  send: 'Send text, photos, or files',
  media: 'Download media',
  topics: 'Forum topics',
  tags: 'Channel tags',
  metadata: 'Channel metadata cache',
  contacts: 'Contacts and people',
  'contacts alias': 'Manage contact aliases',
  'contacts tags': 'Manage contact tags',
  'contacts notes': 'Manage contact notes',
  groups: 'Group management',
  'groups members': 'Manage group members',
  'groups invite': 'Manage invite links',
  folders: 'Chat folder management',
  'folders chats': 'Manage chats in a folder',
});
