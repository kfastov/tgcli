// Forum topics of a supergroup.

import { defineOperation, field, toIsoDate } from './operation.js';
import { chatOption } from './chat-keys.js';

function formatTopic(topic) {
  let lastMessage = null;
  try {
    const message = topic.lastMessage;
    if (message) {
      lastMessage = {
        id: message.id,
        date: toIsoDate(message.date),
        text: message.text ?? message.message ?? '',
      };
    }
  } catch {
    // mtcute throws when the topic's last message was not loaded with it.
    lastMessage = null;
  }
  return {
    id: topic.id,
    title: topic.title,
    date: toIsoDate(topic.date),
    isClosed: topic.isClosed,
    isPinned: topic.isPinned,
    unreadCount: topic.unreadCount,
    lastMessage,
  };
}

// Lists or searches the topics and refreshes the archive's copy of them.
async function listTopics(ctx, args) {
  const listOptions = { limit: args.limit };
  if (args.query) {
    listOptions.query = args.query;
  }
  const chatId = await ctx.messageSyncService.resolveChatKey(args.channelId);
  const topics = await ctx.telegramClient.listForumTopics(args.channelId, listOptions);
  ctx.messageSyncService.upsertTopics(chatId, topics);
  const formatted = topics.map(formatTopic);
  return { total: topics.total ?? formatted.length, returned: formatted.length, topics: formatted };
}

function renderTopics(result, { print }) {
  for (const topic of result.topics) {
    print(`#${topic.id} ${topic.title ?? ''}`.trim());
  }
}

const limitInput = () => field.int('Maximum number of topics to return').default(100).flag('--limit <n>', { help: 'Limit results' });

export const topicsList = defineOperation({
  name: 'topicsList',
  command: 'topics list',
  summary: 'List topics',
  description: 'Lists forum topics of a supergroup.',
  annotations: { readOnly: true },
  input: { channelId: chatOption('Forum supergroup ID or username'), limit: limitInput() },
  run: listTopics,
  render: renderTopics,
});

export const topicsSearch = defineOperation({
  name: 'topicsSearch',
  command: 'topics search',
  summary: 'Search topics',
  description: 'Searches forum topics of a supergroup by title.',
  annotations: { readOnly: true },
  input: {
    channelId: chatOption('Forum supergroup ID or username'),
    query: field.text('Search query for forum topic titles').flag('--query <text>', { help: 'Search query' }),
    limit: limitInput(),
  },
  run: listTopics,
  render: renderTopics,
});

export const TOPIC_OPERATIONS = [topicsList, topicsSearch];
