// Sending text, photos and files. The server runs the retry and FLOOD_WAIT logic
// against its warm client; the caller bounds how long it waits.

import path from 'path';

import { defineOperation, field } from './operation.js';
import {
  buildSendSuccessPayload,
  classifySendError,
  executeSendWithRetries,
  formatErrorMessage,
  parseRetryBackoff,
  SendCommandError,
} from '../send-utils.js';

const SEND_PARSE_MODES = ['markdown', 'html', 'none'];
const DEFAULT_SEND_RETRIES = 2;
const MAX_SCHEDULE_AHEAD_MS = 365 * 24 * 60 * 60 * 1000;
// Sends serve agents and scripts that must not block forever on a hung MTProto
// connection: without --timeout the caller waits this long, and the server stops
// retrying once it is spent.
export const DEFAULT_SEND_TIMEOUT_MS = 30000;

function formatTimeoutDuration(timeoutMs) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return `${timeoutMs}ms`;
  }
  return timeoutMs % 1000 === 0 ? `${timeoutMs / 1000}s` : `${timeoutMs}ms`;
}

export function formatSendTimeoutMessage(timeoutMs) {
  return `Send timed out after ${formatTimeoutDuration(timeoutMs)} (no response from Telegram). `
    + 'Pass --timeout <duration> to extend, or --timeout 0 to disable.';
}

export function normalizeSendCommandError(error, { method, retries, attempt = 1 } = {}) {
  if (error instanceof SendCommandError) return error;
  if (error instanceof TypeError || error instanceof ReferenceError || error instanceof SyntaxError || error instanceof RangeError) return error;
  return new SendCommandError(classifySendError(error, { method, retries, attempt }));
}

// A request the caller gave up on (AbortSignal.timeout fires a TimeoutError, an
// aborted request an AbortError) is reported as a send timeout.
function isRequestTimeoutError(error) {
  return error?.name === 'TimeoutError' || error?.name === 'AbortError';
}

function mapSendError(method) {
  return (error, { args, timeoutMs }) => {
    const retries = Number.isInteger(args?.retries) ? args.retries : DEFAULT_SEND_RETRIES;
    if (isRequestTimeoutError(error)) {
      return new SendCommandError({
        type: 'timeout',
        method,
        message: formatSendTimeoutMessage(timeoutMs),
        code: null,
        attempt: 1,
        retries,
        retryable: false,
      });
    }
    return normalizeSendCommandError(error, { method, retries });
  };
}

// The label comes from the server's result, not from --schedule: the server says
// whether Telegram scheduled the message.
export function printSendOutcome(label, { messageId, scheduled, warning }, { print, warn }) {
  print(`${label} ${scheduled ? 'scheduled' : 'sent'} (${messageId}).`);
  if (warning) {
    warn(`Warning: ${warning}`);
  }
}

export function buildSendPhotoSuccessPayload({ method, inputChatId, result, attempts }) {
  return buildSendSuccessPayload({
    method,
    chatId: result?.chatId ?? inputChatId,
    messageId: result?.messageId,
    media: result?.media ?? { type: 'photo' },
    attempts,
    warning: result?.warning,
    scheduled: result?.scheduled,
  });
}

// Shared send executor: runs the send-utils retry/flood/timeout logic against the
// warm client. `sendFn` performs one send attempt; on exhausted retries this throws
// a SendCommandError carrying structured details, which the control server
// serializes and the CLI reconstructs.
//
// `sendFn` also gets `onSent` to hand to the client: mtcute keeps a client's own
// sends out of onNewMessage, so this is the only way they reach the archive, and a
// send that never reports its message comes back with a warning. onSent must not
// throw, because an error out of an attempt would retry a send that has already
// gone through and post the message twice.
async function runSend(ctx, method, args, sendFn) {
  let archiveWarning = 'Message sent but not archived: the send did not report its message.';
  const onSent = (message) => {
    try {
      ctx.messageSyncService.archiveSentMessage(message);
      archiveWarning = null;
    } catch (error) {
      const reason = formatErrorMessage(error);
      console.error(`[archive] ${method} to ${args.channelId}: message ${message?.id} was sent but not archived: ${reason}`);
      archiveWarning = `Message sent but not archived: ${reason}`;
    }
  };
  const { result, attempts } = await executeSendWithRetries(() => sendFn(onSent), {
    method,
    retries: args.retries,
    retryBackoff: parseRetryBackoff(args.retryBackoff),
    timeoutMs: args.timeoutMs,
  });
  if (!archiveWarning) {
    return { result, attempts };
  }
  const warning = result?.warning ? `${result.warning}; ${archiveWarning}` : archiveWarning;
  return { result: { ...result, warning }, attempts };
}

function toScheduleDate(schedule) {
  return schedule ? Math.floor(Date.parse(schedule) / 1000) : undefined;
}

// The send options every send command shares, in the client's spelling.
function deliveryOptions(args) {
  return {
    topicId: args.topicId,
    replyToMessageId: args.replyToMessageId,
    parseMode: args.parseMode,
    silent: args.silent || false,
    noforwards: args.noForwards || false,
    scheduleDate: toScheduleDate(args.schedule),
  };
}

function withAttempts(payload, attempts) {
  return attempts > 1 ? { ...payload, attempts } : payload;
}

const deliveryInput = () => ({
  channelId: field.chat('Chat ID or username to send to')
    .flag('--to <id|username>', { help: 'Recipient id or username', aliases: ['--chat <id|username>'] }),
});

const sendOptionsInput = (captionMode) => ({
  parseMode: field.choice(SEND_PARSE_MODES, `Parse mode${captionMode ? ' for the caption' : ''}`).optional()
    .flag('--parse-mode <mode>', { help: `Parse mode${captionMode ? ' for caption' : ''}: markdown|html|none` }),
  topicId: field.int('Forum topic ID to send into').optional().flag('--topic <id>', { help: 'Forum topic id' }),
  replyToMessageId: field.int('Message ID to reply to').optional().flag('--reply-to <id>', { help: 'Reply to message id' }),
  silent: field.bool('Send without notification sound').optional().flag('--silent'),
  noForwards: field.bool('Protect the message from forwarding').optional()
    .flag('--no-forwards', { help: 'Protect message from forwarding' }),
});

const retryInput = () => ({
  schedule: field.date('Send at this ISO-8601 time instead of now (within 365 days)').optional()
    .flag('--schedule <iso>', { help: 'Schedule message (ISO 8601 datetime)' }),
  retries: field.int('Max retries on failure', { min: 0 }).default(DEFAULT_SEND_RETRIES).flag('--retries <n>'),
  retryBackoff: field.text('Retry backoff in ms or strategy: constant|linear|exponential').optional()
    .flag('--retry-backoff <value>'),
  // The caller's wait: the server stops retrying once it is spent. 0 waits forever.
  timeoutMs: field.int('Give up on the send after this many milliseconds (0: never)', { min: 0 }).optional(),
});

const mediaInput = () => ({
  caption: field.textOrEmpty('Caption').optional().flag('--caption <text>', { help: 'Optional caption' }),
  captionAbove: field.bool('Show the caption above the media').optional().flag('--caption-above', { help: 'Show caption above media' }),
  spoiler: field.bool('Blur the media until tapped').optional().flag('--spoiler', { help: 'Blur media until tapped' }),
});

function checkDelivery(args, fail) {
  if (args.schedule) {
    const at = Date.parse(args.schedule);
    const now = Date.now();
    if (at < now) {
      fail('{schedule} must be in the future');
    } else if (at > now + MAX_SCHEDULE_AHEAD_MS) {
      fail('{schedule} must be within 365 days from now');
    }
  }
  if (args.retryBackoff !== undefined) {
    try {
      parseRetryBackoff(args.retryBackoff);
    } catch {
      fail('{retryBackoff} must be a non-negative integer or one of: constant, linear, exponential');
    }
  }
}

function checkCaption(command) {
  return (args, fail) => {
    const hasCaption = typeof args.caption === 'string' && args.caption.trim() !== '';
    if (args.parseMode && !hasCaption) {
      fail(`{parseMode} requires {caption} for ${command}`);
    }
    if (args.captionAbove && !hasCaption) {
      fail(`{captionAbove} requires {caption} for ${command}`);
    }
  };
}

function sendClient(method, { fileKey } = {}) {
  return {
    timeoutMs: DEFAULT_SEND_TIMEOUT_MS,
    prepare(args, caller) {
      const prepared = { ...args, timeoutMs: caller.timeoutMs };
      if (fileKey) {
        prepared[fileKey] = path.resolve(caller.cwd, args[fileKey]);
      }
      return prepared;
    },
    mapError: mapSendError(method),
  };
}

export const messagesSend = defineOperation({
  name: 'messagesSend',
  command: 'send text',
  summary: 'Send a text message',
  description: 'Sends a text message to a chat, retrying on transient failures.',
  annotations: { destructive: false, idempotent: false },
  input: {
    ...deliveryInput(),
    text: field.text('Message text').flag('--message <text>', { help: 'Message text', aliases: ['--text <text>'] }),
    ...sendOptionsInput(false),
    noPreview: field.bool('Disable the link preview').optional().flag('--no-preview', { help: 'Disable link preview' }),
    ...retryInput(),
  },
  refine: checkDelivery,
  client: sendClient('sendText'),
  async run(ctx, args) {
    const { result, attempts } = await runSend(ctx, 'sendText', args, (onSent) => (
      ctx.telegramClient.sendTextMessage(args.channelId, args.text, {
        ...deliveryOptions(args),
        noPreview: args.noPreview,
        onSent,
      })
    ));
    return withAttempts({ channelId: args.channelId, ...result }, attempts);
  },
  render: (payload, out) => printSendOutcome('Message', payload, out),
});

export const messagesSendPhoto = defineOperation({
  name: 'messagesSendPhoto',
  command: 'send photo',
  summary: 'Send a photo with preview',
  description: 'Sends a photo from a local file, with an optional caption.',
  annotations: { destructive: false, idempotent: false },
  input: {
    ...deliveryInput(),
    photo: field.text('Path to the photo').flag('--photo <path>', { help: 'Photo path' }),
    ...mediaInput(),
    ...sendOptionsInput(true),
    ...retryInput(),
  },
  refine(args, fail) {
    checkCaption('send photo')(args, fail);
    checkDelivery(args, fail);
  },
  client: sendClient('sendPhoto', { fileKey: 'photo' }),
  async run(ctx, args) {
    const prepared = await ctx.telegramClient.preparePhotoMessage(args.channelId, args.photo, {
      ...deliveryOptions(args),
      caption: args.caption,
      captionAbove: args.captionAbove || false,
      spoiler: args.spoiler || false,
    });
    const { result, attempts } = await runSend(ctx, 'sendPhoto', args, (onSent) => (
      ctx.telegramClient.sendPreparedPhotoMessage(prepared, { onSent })
    ));
    return buildSendPhotoSuccessPayload({ method: 'sendPhoto', inputChatId: args.channelId, result, attempts });
  },
  render: (payload, out) => printSendOutcome('Photo', { ...payload, messageId: payload.message_id }, out),
});

export const messagesSendFile = defineOperation({
  name: 'messagesSendFile',
  command: 'send file',
  summary: 'Send a file',
  description: 'Sends a local file with an optional caption.',
  annotations: { destructive: false, idempotent: false },
  input: {
    ...deliveryInput(),
    filePath: field.text('Path to the file to upload').flag('--file <path>', { help: 'File path' }),
    ...mediaInput(),
    filename: field.text('File name shown in Telegram').optional().flag('--filename <name>', { help: 'Override filename' }),
    forceDocument: field.bool('Send as an uncompressed document').optional()
      .flag('--force-document', { help: 'Send as uncompressed document' }),
    ...sendOptionsInput(true),
    ...retryInput(),
  },
  refine(args, fail) {
    checkCaption('send file')(args, fail);
    checkDelivery(args, fail);
  },
  client: sendClient('sendFile', { fileKey: 'filePath' }),
  async run(ctx, args) {
    const { result, attempts } = await runSend(ctx, 'sendFile', args, (onSent) => (
      ctx.telegramClient.sendFileMessage(args.channelId, args.filePath, {
        ...deliveryOptions(args),
        caption: args.caption,
        filename: args.filename,
        captionAbove: args.captionAbove || false,
        spoiler: args.spoiler || false,
        forceDocument: args.forceDocument || false,
        onSent,
      })
    ));
    return withAttempts({ channelId: args.channelId, ...result }, attempts);
  },
  render: (payload, out) => printSendOutcome('File', payload, out),
});

export const SEND_OPERATIONS = [messagesSend, messagesSendPhoto, messagesSendFile];
