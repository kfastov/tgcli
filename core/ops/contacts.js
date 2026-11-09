// People the account knows, with the aliases, tags and notes kept for them.

import { defineOperation, field } from './operation.js';

const userInput = () => field.user('User ID or username').flag('--user <id>', { help: 'User id' });

export const contactsSearch = defineOperation({
  name: 'contactsSearch',
  command: 'contacts search',
  summary: 'Search contacts',
  description: 'Searches contacts and users by name, username, alias, tags and notes.',
  annotations: { readOnly: true },
  input: {
    query: field.text('Search query for contacts').positional('<query...>'),
    limit: field.int('Maximum number of contacts to return').default(50).flag('--limit <n>', { help: 'Limit results' }),
  },
  async run(ctx, args) {
    await ctx.messageSyncService.refreshContacts();
    return ctx.messageSyncService.searchContacts(args.query, { limit: args.limit });
  },
  render(contacts, { print }) {
    for (const contact of contacts) {
      const label = contact.alias || contact.displayName || contact.username || contact.userId;
      print(`${label} (${contact.userId})`);
    }
  },
});

export const contactsGet = defineOperation({
  name: 'contactsGet',
  command: 'contacts show',
  summary: 'Show contact profile',
  description: 'Returns a contact profile from the local store, refreshing contacts when it is missing.',
  annotations: { readOnly: true },
  input: { userId: userInput() },
  async run(ctx, args) {
    let contact = ctx.messageSyncService.getContact(args.userId);
    if (!contact) {
      await ctx.messageSyncService.refreshContacts();
      contact = ctx.messageSyncService.getContact(args.userId);
    }
    if (!contact) {
      throw new Error('Contact not found.');
    }
    return contact;
  },
});

export const contactsAliasSet = defineOperation({
  name: 'contactsAliasSet',
  // Aliases, tags and notes are the archive's own; Telegram has no part in them.
  telegram: false,
  command: 'contacts alias set',
  summary: 'Set contact alias',
  description: 'Sets an alias for a contact, replacing the one it had.',
  annotations: { idempotent: true, destructive: true },
  input: {
    userId: userInput(),
    alias: field.text('Alias for the contact').flag('--alias <name>', { help: 'Alias' }),
  },
  run(ctx, args) {
    const alias = ctx.messageSyncService.setContactAlias(args.userId, args.alias);
    return { userId: args.userId, alias };
  },
  render(result, { print }) {
    print(`Alias set for ${result.userId}: ${result.alias}`);
  },
});

export const contactsAliasRemove = defineOperation({
  name: 'contactsAliasRemove',
  telegram: false,
  command: 'contacts alias rm',
  summary: 'Remove contact alias',
  description: 'Removes the alias of a contact.',
  annotations: { idempotent: true, destructive: true },
  input: { userId: userInput() },
  run(ctx, args) {
    ctx.messageSyncService.removeContactAlias(args.userId);
    return { userId: args.userId, removed: true };
  },
  render(result, { print }) {
    print(`Alias removed for ${result.userId}`);
  },
});

const tagsInput = (help) => field.strings(help).flag('--tag <tag>', { help: 'Tag' });

function renderContactTags(result, { print }) {
  print(`Tags updated for ${result.userId}: ${result.tags.join(', ')}`);
}

export const contactsTagsAdd = defineOperation({
  name: 'contactsTagsAdd',
  telegram: false,
  command: 'contacts tags add',
  summary: 'Add contact tags',
  description: 'Adds tags to a contact.',
  annotations: { idempotent: true },
  input: { userId: userInput(), tags: tagsInput('Tags to add') },
  refine(args, fail) {
    if (!args.tags.length) {
      fail('{tags} is required');
    }
  },
  run(ctx, args) {
    return { userId: args.userId, tags: ctx.messageSyncService.addContactTags(args.userId, args.tags) };
  },
  render: renderContactTags,
});

export const contactsTagsRemove = defineOperation({
  name: 'contactsTagsRemove',
  telegram: false,
  command: 'contacts tags rm',
  summary: 'Remove contact tags',
  description: 'Removes tags from a contact.',
  annotations: { idempotent: true, destructive: true },
  input: { userId: userInput(), tags: tagsInput('Tags to remove') },
  refine(args, fail) {
    if (!args.tags.length) {
      fail('{tags} is required');
    }
  },
  run(ctx, args) {
    return { userId: args.userId, tags: ctx.messageSyncService.removeContactTags(args.userId, args.tags) };
  },
  render: renderContactTags,
});

export const contactsNotesSet = defineOperation({
  name: 'contactsNotesSet',
  telegram: false,
  command: 'contacts notes set',
  summary: 'Set contact notes',
  description: 'Sets the notes kept for a contact, replacing the old ones; empty text clears them.',
  annotations: { idempotent: true, destructive: true },
  input: {
    userId: userInput(),
    notes: field.textOrEmpty('Notes to attach to the contact').flag('--notes <text>', { help: 'Notes' }),
  },
  run(ctx, args) {
    return { userId: args.userId, notes: ctx.messageSyncService.setContactNotes(args.userId, args.notes) };
  },
  render(result, { print }) {
    print(`Notes updated for ${result.userId}.`);
  },
});

export const CONTACT_OPERATIONS = [
  contactsSearch,
  contactsGet,
  contactsAliasSet,
  contactsAliasRemove,
  contactsTagsAdd,
  contactsTagsRemove,
  contactsNotesSet,
];
