import { TelegramClient as MtCuteClient } from '@mtcute/node';
import { InputMedia, Message, MtPeerNotFoundError, PeersIndex } from '@mtcute/core';
import { randomLong, toggleChannelIdMark } from '@mtcute/core/utils.js';
import { html } from '@mtcute/html-parser';
import { md } from '@mtcute/markdown-parser';
import QRCode from 'qrcode';
import EventEmitter from 'events';
import fs from 'fs';
import { stat } from 'fs/promises';
import os from 'os';
import path from 'path';
import readline from 'readline';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { nodeReadableToFuman } from '@fuman/node';
import { resolveStoreDir, resolveStorePaths } from './core/store.js';

const timeoutPatchKey = Symbol.for('tgcli.timeoutPatch');
if (!globalThis[timeoutPatchKey]) {
  const originalSetTimeout = globalThis.setTimeout;
  if (typeof originalSetTimeout === 'function') {
    const wrapped = (handler, delay, ...args) => {
      const safeDelay = Number.isFinite(delay) ? Math.max(0, delay) : 0;
      return originalSetTimeout(handler, safeDelay, ...args);
    };
    globalThis.setTimeout = wrapped;
  }
  globalThis[timeoutPatchKey] = true;
}

const DEFAULT_STORE_DIR = resolveStoreDir();
const { sessionPath: DEFAULT_SESSION_PATH } = resolveStorePaths(DEFAULT_STORE_DIR);
const DEFAULT_DOWNLOAD_DIR = path.join(DEFAULT_STORE_DIR, 'downloads');
const MIME_EXTENSION_MAP = {
  'image/jpeg': '.jpg',
  'image/png': '.png',
  'image/webp': '.webp',
  'image/gif': '.gif',
  'video/mp4': '.mp4',
  'video/quicktime': '.mov',
  'audio/mpeg': '.mp3',
  'audio/mp4': '.m4a',
  'audio/ogg': '.ogg',
  'audio/opus': '.opus',
  'audio/wav': '.wav',
  'application/pdf': '.pdf',
  'application/zip': '.zip',
};

const IS_TTY = typeof process === 'object' && Boolean(process.stdout?.isTTY);
const LOG_BASE_FORMAT = IS_TTY ? '%s [%s] [%s%s\x1B[0m] ' : '%s [%s] [%s] ';
const LOG_LEVEL_NAMES = IS_TTY
  ? [
      '',
      '\x1B[31mERR\x1B[0m',
      '\x1B[33mWRN\x1B[0m',
      '\x1B[34mINF\x1B[0m',
      '\x1B[36mDBG\x1B[0m',
      '\x1B[35mVRB\x1B[0m',
    ]
  : ['', 'ERR', 'WRN', 'INF', 'DBG', 'VRB'];
const LOG_TAG_COLORS = [6, 2, 3, 4, 5, 1].map((i) => `\x1B[3${i};1m`);
const LOG_HANDLER = IS_TTY
  ? (color, level, tag, fmt, args) => {
      console.log(
        LOG_BASE_FORMAT + fmt,
        new Date().toISOString(),
        LOG_LEVEL_NAMES[level],
        LOG_TAG_COLORS[color],
        tag,
        ...args,
      );
    }
  : (color, level, tag, fmt, args) => {
      console.log(
        LOG_BASE_FORMAT + fmt,
        new Date().toISOString(),
        LOG_LEVEL_NAMES[level],
        tag,
        ...args,
      );
    };

async function normalizeUploadFile(file) {
  if (typeof file === 'string') {
    file = fs.createReadStream(file);
  }
  if (file instanceof fs.ReadStream) {
    const filePath = file.path.toString();
    const fileName = path.basename(filePath);
    const fileSize = await stat(filePath).then((stats) => stats.size);
    return {
      file: nodeReadableToFuman(file),
      fileName,
      fileSize,
    };
  }
  if (file instanceof Readable) {
    return {
      file: nodeReadableToFuman(file),
    };
  }
  return null;
}

function createPlatform() {
  return {
    beforeExit: (callback) => {
      if (typeof process === 'undefined') {
        return () => {};
      }
      const handler = () => {
        callback();
      };
      process.on('exit', handler);
      return () => {
        process.off('exit', handler);
      };
    },
    log: LOG_HANDLER,
    getDefaultLogLevel: () => {
      const envLogLevel = Number.parseInt(process.env.MTCUTE_LOG_LEVEL ?? '', 10);
      if (!Number.isNaN(envLogLevel)) {
        return envLogLevel;
      }
      return null;
    },
    getDeviceModel: () => `Node.js/${process.version} (${os.type()} ${os.arch()})`,
    normalizeFile: normalizeUploadFile,
  };
}

function sanitizeString(value) {
  return typeof value === 'string' ? value : '';
}

function normalizeParseMode(value) {
  if (value === undefined || value === null || value === '') return null;
  const normalized = String(value).trim().toLowerCase();
  if (!['markdown', 'html', 'none'].includes(normalized)) {
    throw new Error('Invalid parse mode. Allowed values: markdown, html, none');
  }
  return normalized;
}

function applyParseMode(text, parseMode) {
  if (parseMode === 'markdown') return md(text);
  if (parseMode === 'html') return html(text);
  return text;
}

function resolveScheduleDate(options) {
  if (options.scheduleDate !== undefined && options.scheduleDate !== null) return options.scheduleDate;
  if (options.schedule) {
    const date = new Date(options.schedule);
    if (Number.isNaN(date.getTime())) {
      throw new Error('Invalid schedule date: must be a valid ISO 8601 datetime');
    }
    return Math.floor(date.getTime() / 1000);
  }
  return undefined;
}

function resolveReplyTo(options = {}) {
  if (Number.isFinite(options.replyToMessageId)) return options.replyToMessageId;
  if (Number.isFinite(options.topicId)) return options.topicId;
  return undefined;
}

function buildTextSendParams(options = {}) {
  const params = {};
  const replyTo = resolveReplyTo(options);
  if (replyTo) params.replyTo = replyTo;
  if (options.noPreview) params.disableWebPreview = true;
  if (options.silent) params.silent = true;
  if (options.noForwards || options.noforwards) params.forbidForwards = true;
  const scheduleDate = resolveScheduleDate(options);
  if (scheduleDate) params.schedule = new Date(scheduleDate * 1000);
  return Object.keys(params).length ? params : undefined;
}

function buildMediaSendParams(options = {}) {
  const params = {};
  const replyTo = resolveReplyTo(options);
  if (replyTo) params.replyTo = replyTo;
  if (options.silent) params.silent = true;
  if (options.noForwards || options.noforwards) params.forbidForwards = true;
  const scheduleDate = resolveScheduleDate(options);
  if (scheduleDate) params.schedule = new Date(scheduleDate * 1000);
  if (options.captionAbove) params.invert = true;
  return Object.keys(params).length ? params : undefined;
}

function resolveUploadPath(filePath) {
  if (!filePath || typeof filePath !== 'string') {
    throw new Error('filePath must be a string.');
  }
  const resolved = path.resolve(filePath);
  if (!fs.existsSync(resolved)) {
    throw new Error(`File not found: ${resolved}`);
  }
  return `file:${resolved}`;
}

// mtcute writes a min peer over the complete one it already has, access hash included
// (still so in 0.32.7). A min row counts as having no hash, so from then on the chat
// cannot be looked up, and an update that hits a pts gap is lost: getChannelDifference
// finds no peer, or is refused with CHANNEL_INVALID. A min copy adds nothing worth that.
function keepCompletePeersOverMinCopies(client) {
  const { peers, refMsgs } = client.storage;
  const store = peers.store.bind(peers);
  peers.store = async (peer) => {
    const isChannel = peer._ === 'channel' || peer._ === 'channelForbidden';
    const id = isChannel ? toggleChannelIdMark(peer.id) : peer.id;
    if (peer.min) {
      const existing = await peers.getCompleteById(id);
      if (existing && !existing.min) {
        await refMsgs.deleteByPeer(id);
        return;
      }
    }
    await store(peer);
    // store() caches a channel under its bare id while lookups go by the marked one, so
    // a lookup that cached the min row would go on answering with its hash. Overwritten,
    // not deleted, since LruMap.delete unlinks everything older than the entry.
    if (isChannel && !peer.min && peers._cache.get(id)) {
      peers._cache.set(id, peers._cache.get(peer.id));
    }
  };
}

function resolveOptionalCaption(caption) {
  if (typeof caption !== 'string') return undefined;
  const trimmed = caption.trim();
  return trimmed ? trimmed : undefined;
}

function buildSendMessageResult(sent, { method, defaultMediaType } = {}) {
  const result = { messageId: sent?.id ?? null };
  if (sent?.isScheduled) result.scheduled = true;
  if (method) result.method = method;
  const media = summarizeMedia(sent?.media) ?? (defaultMediaType ? { type: defaultMediaType } : null);
  if (media) result.media = media;
  return result;
}

function buildLowLevelReplyTo(options = {}) {
  const replyTo = resolveReplyTo(options);
  if (!replyTo) return undefined;
  return { _: 'inputReplyToMessage', replyToMsgId: replyTo };
}

function splitInputText(value) {
  if (!value) return { message: '', entities: undefined };
  if (typeof value === 'string') return { message: value, entities: undefined };
  if (typeof value === 'object' && typeof value.text === 'string') {
    return {
      message: value.text,
      entities: Array.isArray(value.entities) && value.entities.length > 0 ? value.entities : undefined,
    };
  }
  return { message: String(value), entities: undefined };
}

function randomIdsEqual(left, right) {
  if (left?.eq && typeof left.eq === 'function') return left.eq(right);
  return String(left) === String(right);
}

const SENT_MESSAGE_UPDATES = new Set(['updateNewMessage', 'updateNewChannelMessage', 'updateNewScheduledMessage']);

// What a send response says about the message it sent: its id and, when the
// response carries the message itself, the update holding it.
function readSendResponse(response, randomId) {
  const updates = Array.isArray(response?.updates) ? response.updates : [];
  const assignedId = updates.find(
    (update) => update?._ === 'updateMessageID' && randomIdsEqual(update.randomId, randomId),
  )?.id;
  const messageUpdates = updates.filter((update) => SENT_MESSAGE_UPDATES.has(update?._));
  const messageUpdate = assignedId === undefined
    ? messageUpdates[0]
    : messageUpdates.find((update) => update.message?.id === assignedId);
  return { messageId: assignedId ?? messageUpdate?.message?.id ?? null, messageUpdate };
}

// The send has already gone through at this point, so a response that cannot be
// parsed must not fail it.
function parseSentMessage(messageUpdate, response, peerRef) {
  try {
    return new Message(messageUpdate.message, PeersIndex.from(response), messageUpdate._ === 'updateNewScheduledMessage');
  } catch (error) {
    console.error(`[sendPhoto] Failed to parse sent message ${messageUpdate.message?.id} for peer ${peerRef}: ${error.message}`);
    return null;
  }
}

function coerceApiId(value) {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = parseInt(value, 10);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  throw new Error('TELEGRAM_API_ID must be a number');
}

function normalizePeerType(peer) {
  if (!peer) return 'chat';
  if (peer.type === 'user' || peer.type === 'bot') return 'user';
  if (peer.type === 'channel') return 'channel';
  if (peer.type === 'chat' && peer.chatType && peer.chatType !== 'group') return 'channel';
  return 'chat';
}

function isGroupPeer(peer) {
  if (!peer) return false;
  if (typeof peer.isGroup === 'boolean') {
    return peer.isGroup;
  }
  if (peer.type === 'chat') {
    return true;
  }
  if (peer.type === 'channel' && typeof peer.chatType === 'string') {
    return peer.chatType !== 'channel';
  }
  return false;
}

function extractTopicId(message) {
  if (!message) return null;
  if (typeof message.replyToMessage?.threadId === 'number') {
    return message.replyToMessage.threadId;
  }
  if (message.action?.type === 'topic_created' && typeof message.id === 'number') {
    return message.id;
  }
  const raw = message.raw ?? message;
  const replyTo = raw?.replyTo;
  if (replyTo?.replyToTopId) {
    return replyTo.replyToTopId;
  }
  return null;
}

function normalizeMediaText(value) {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value);
  }
  return null;
}

function normalizeMediaNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function readMediaProperty(media, prop) {
  if (!media) {
    return null;
  }
  try {
    return media[prop];
  } catch (error) {
    return null;
  }
}

function buildWebpageExtras(media) {
  if (!media || media.type !== 'webpage' || !media.preview) {
    return null;
  }
  const preview = media.preview;
  const previewData = {
    url: preview.url ?? null,
    displayUrl: preview.displayUrl ?? null,
    siteName: preview.siteName ?? null,
    title: preview.title ?? null,
    description: preview.description ?? null,
    author: preview.author ?? null,
    previewType: preview.previewType ?? null,
  };
  const hasPreview = Object.values(previewData).some((value) => value);
  const extras = {};
  if (hasPreview) {
    extras.preview = previewData;
  }
  if (typeof media.displaySize === 'string') {
    extras.displaySize = media.displaySize;
  }
  if (typeof media.manual === 'boolean') {
    extras.manual = media.manual;
  }
  return Object.keys(extras).length ? extras : null;
}

export function summarizeMedia(media) {
  if (!media || typeof media !== 'object') {
    return null;
  }
  const type = normalizeMediaText(media.type);
  if (!type) {
    return null;
  }

  const summary = {
    type,
    fileId: normalizeMediaText(readMediaProperty(media, 'fileId') ?? media.file_id),
    uniqueFileId: normalizeMediaText(readMediaProperty(media, 'uniqueFileId') ?? media.unique_file_id),
    fileName: normalizeMediaText(readMediaProperty(media, 'fileName') ?? media.file_name),
    mimeType: normalizeMediaText(readMediaProperty(media, 'mimeType') ?? media.mime_type),
    fileSize: normalizeMediaNumber(readMediaProperty(media, 'fileSize') ?? media.file_size),
    width: normalizeMediaNumber(readMediaProperty(media, 'width') ?? media.width),
    height: normalizeMediaNumber(readMediaProperty(media, 'height') ?? media.height),
    duration: normalizeMediaNumber(readMediaProperty(media, 'duration') ?? media.duration),
    extras: null,
  };

  if (!summary.mimeType && type === 'photo') {
    summary.mimeType = 'image/jpeg';
  }

  if (media.extras && typeof media.extras === 'object') {
    summary.extras = media.extras;
  } else {
    summary.extras = buildWebpageExtras(media);
  }

  if (type === 'webpage' && media.preview) {
    const previewMedia = media.preview.document ?? media.preview.photo ?? null;
    if (previewMedia) {
      const previewSummary = summarizeMedia(previewMedia);
      if (previewSummary) {
        summary.fileId = summary.fileId ?? previewSummary.fileId;
        summary.uniqueFileId = summary.uniqueFileId ?? previewSummary.uniqueFileId;
        summary.fileName = summary.fileName ?? previewSummary.fileName;
        summary.mimeType = summary.mimeType ?? previewSummary.mimeType;
        summary.fileSize = summary.fileSize ?? previewSummary.fileSize;
        summary.width = summary.width ?? previewSummary.width;
        summary.height = summary.height ?? previewSummary.height;
        summary.duration = summary.duration ?? previewSummary.duration;
      }
    }
  }

  return summary;
}

function extensionFromMime(mimeType) {
  if (!mimeType || typeof mimeType !== 'string') {
    return null;
  }
  return MIME_EXTENSION_MAP[mimeType.toLowerCase()] ?? null;
}

function buildDownloadFileName(summary, messageId) {
  const rawName = normalizeMediaText(summary?.fileName);
  if (rawName) {
    return path.basename(rawName);
  }
  const ext = extensionFromMime(summary?.mimeType) ?? '';
  const baseType = normalizeMediaText(summary?.type) ?? 'media';
  return `${baseType}-${messageId}${ext}`;
}

function resolveDownloadPath(outputPath, { channelId, messageId, summary }) {
  const fileName = buildDownloadFileName(summary, messageId);
  if (!outputPath) {
    return path.resolve(DEFAULT_DOWNLOAD_DIR, String(channelId), fileName);
  }
  const resolved = path.resolve(outputPath);
  if (fs.existsSync(resolved)) {
    const stat = fs.statSync(resolved);
    if (stat.isDirectory()) {
      return path.join(resolved, fileName);
    }
  } else if (/[\\/]$/.test(outputPath)) {
    return path.join(resolved, fileName);
  }
  return resolved;
}

function resolveDownloadLocation(media) {
  if (!media || typeof media !== 'object') {
    return null;
  }
  if (media.type === 'webpage' && media.preview) {
    return media.preview.document ?? media.preview.photo ?? null;
  }
  if (media.location && typeof media.location === 'object') {
    return media;
  }
  return null;
}

// Errors about a chat itself: the account has left it or was removed, it is gone, or
// Telegram does not know it. Anything else, such as a broken session, says nothing
// about the chat.
const CHAT_GONE_ERRORS = new Set([
  'CHANNEL_INVALID',
  'CHANNEL_PRIVATE',
  'CHAT_FORBIDDEN',
  'CHAT_ID_INVALID',
  'PEER_ID_INVALID',
  'USER_BANNED_IN_CHANNEL',
]);

export function isChatGone(error) {
  return error instanceof MtPeerNotFoundError || CHAT_GONE_ERRORS.has(error?.text);
}

// What Telegram says about the account in a channel or group: 'member', 'left' (left on
// its own), 'gone' (removed, banned, the chat deleted or migrated) or 'unknown' (not in
// the local peer cache, so there is nothing to ask with).
function membershipOf(chat) {
  switch (chat?._) {
    case 'channel':
      return chat.left ? 'left' : 'member';
    case 'chat':
      if (chat.deactivated || chat.migratedTo) {
        return 'gone';
      }
      return chat.left ? 'left' : 'member';
    case 'channelForbidden':
    case 'chatForbidden':
    case 'chatEmpty':
      return 'gone';
    default:
      return null;
  }
}

export function normalizeChannelId(channelId) {
  if (typeof channelId === 'number') {
    return channelId;
  }
  if (typeof channelId === 'bigint') {
    return Number(channelId);
  }
  if (typeof channelId === 'string') {
    const trimmed = channelId.trim();
    if (/^-?\d+$/.test(trimmed)) {
      const numeric = Number(trimmed);
      if (!Number.isNaN(numeric)) {
        return numeric;
      }
    }
    return trimmed;
  }
  throw new Error('Invalid channel ID provided');
}

function normalizePositiveMessageId(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
}

function filterSerializedMessagesById(messages, options = {}) {
  const beforeId = normalizePositiveMessageId(options.beforeId);
  const afterId = normalizePositiveMessageId(options.afterId);
  if (!beforeId && !afterId) {
    return messages;
  }
  return messages.filter((message) => {
    const messageId = normalizePositiveMessageId(message?.id);
    if (!messageId) {
      return false;
    }
    if (beforeId && messageId >= beforeId) {
      return false;
    }
    if (afterId && messageId <= afterId) {
      return false;
    }
    return true;
  });
}

class TelegramClient {
  constructor(apiId, apiHash, phoneNumber, sessionPath = DEFAULT_SESSION_PATH, options = {}) {
    this.apiId = coerceApiId(apiId);
    this.apiHash = sanitizeString(apiHash);
    this.phoneNumber = sanitizeString(phoneNumber);
    this.sessionPath = path.resolve(sessionPath);
    this.options = options;

    const dataDir = path.dirname(this.sessionPath);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    this.updateEmitter = new EventEmitter();
    this.updatesRunning = false;
    this.rawUpdateHandler = null;
    const userUpdates = options.updates ?? {};
    const updatesConfig = {
      ...userUpdates,
      catchUp: userUpdates.catchUp ?? true,
      onChannelTooLong: (channelId, diff) => {
        if (typeof userUpdates.onChannelTooLong === 'function') {
          userUpdates.onChannelTooLong(channelId, diff);
        }
        this.updateEmitter.emit('channelTooLong', { channelId, diff });
      },
    };
    this.updatesConfig = updatesConfig;
    this.client = this._createClient();
  }

  _createClient() {
    const clientOptions = {
      apiId: this.apiId,
      apiHash: this.apiHash,
      storage: this.sessionPath,
      // A delayed write of a min copy can land after the complete row that replaced it.
      storageOptions: { peers: { updatesWriteInterval: 0 } },
      platform: createPlatform(),
    };
    if (this.options.disableUpdates) {
      clientOptions.disableUpdates = true;
    } else {
      clientOptions.updates = this.updatesConfig;
    }
    const client = new MtCuteClient(clientOptions);
    keepCompletePeersOverMinCopies(client);
    return client;
  }

  _isAuthKeyUnregisteredError(error) {
    if (!error) return false;
    const code = error.code || error.status || error.errorCode;
    const message = (error.errorMessage || error.text || error.message || '').toUpperCase();
    return code === 401 && message.includes('AUTH_KEY_UNREGISTERED');
  }

  _isSessionResetError(error) {
    if (!error) return false;
    const message = (error.errorMessage || error.text || error.message || '').toUpperCase();
    return message.includes('SESSION IS RESET');
  }

  async _recreateClient() {
    try {
      await this.client.destroy();
    } catch (error) {
      console.warn('[warning] failed to destroy MTProto client during reset:', error?.message || error);
    }
    this.client = this._createClient();
    this.updatesRunning = false;
    this.rawUpdateHandler = null;
    this.updateEmitter.emit('clientReplaced');
  }

  async _resetSessionAndClient() {
    const sessionFiles = [this.sessionPath, `${this.sessionPath}-wal`, `${this.sessionPath}-shm`];
    for (const filePath of sessionFiles) {
      try {
        fs.unlinkSync(filePath);
      } catch (error) {
        if (error?.code !== 'ENOENT') {
          throw error;
        }
      }
    }
    await this._recreateClient();
  }

  _isUnauthorizedError(error) {
    if (!error) return false;
    const code = error.code || error.status || error.errorCode;
    if (code === 401) {
      return true;
    }
    const message = (error.errorMessage || error.message || '').toUpperCase();
    return message.includes('AUTH_KEY') || message.includes('AUTHORIZATION') || message.includes('SESSION_PASSWORD_NEEDED');
  }

  async _isAuthorized() {
    try {
      await this.client.getMe();
      return true;
    } catch (error) {
      if (this._isUnauthorizedError(error)) {
        return false;
      }
      throw error;
    }
  }

  async isAuthorized() {
    return this._isAuthorized();
  }

  async getCurrentUser() {
    try {
      return await this.client.getMe();
    } catch (error) {
      if (this._isUnauthorizedError(error)) {
        return null;
      }
      throw error;
    }
  }

  async _askQuestion(prompt) {
    const ask = () => {
      if (process.stdin.isPaused()) {
        process.stdin.resume();
      }
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
      });

      return new Promise(resolve => {
        rl.question(prompt, answer => {
          rl.close();
          resolve(answer.trim());
        });
      });
    };

    let answer = await ask();
    if (!answer) {
      answer = await ask();
    }
    return answer;
  }

  async _askHiddenQuestion(prompt) {
    if (!process.stdin.isTTY) {
      return this._askQuestion(prompt);
    }

    if (process.stdin.isPaused()) {
      process.stdin.resume();
    }
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: true,
    });
    rl.stdoutMuted = false;
    const writeOutput = rl._writeToOutput.bind(rl);
    rl._writeToOutput = (stringToWrite) => {
      if (!rl.stdoutMuted) {
        writeOutput(stringToWrite);
      }
    };

    return new Promise(resolve => {
      rl.question(prompt, answer => {
        rl.output.write('\n');
        rl.close();
        resolve(answer.trim());
      });
      rl.stdoutMuted = true;
    });
  }

  _buildStartParams() {
    const startParams = {
      password: async () => {
        const value = await this._askHiddenQuestion('Enter your 2FA password (leave empty if not enabled): ');
        return value.length ? value : undefined;
      },
    };

    if (this.options.useQr) {
      startParams.qrCodeHandler = async (url, expiresAt) => {
        const expiresLabel = expiresAt instanceof Date && !Number.isNaN(expiresAt.getTime())
          ? expiresAt.toISOString()
          : 'unknown';
        let qrFile = null;

        if (this.options.qrFilePath) {
          qrFile = this.options.qrFilePath;
          await QRCode.toFile(qrFile, url, { type: 'png', width: 300 });
        }

        if (this.options.json) {
          process.stderr.write(`${JSON.stringify({
            event: 'qr',
            url,
            expiresAt: expiresLabel,
            qrFile,
          })}\n`);
          return;
        }

        console.log('\nScan this QR code in Telegram: Settings -> Devices -> Link Desktop Device');
        const terminalQr = await QRCode.toString(url, { type: 'terminal', small: true });
        console.log(terminalQr);
        console.log(`QR expires at: ${expiresLabel}`);
        if (qrFile) {
          console.log(`QR saved to: ${qrFile}`);
        }
      };
    } else {
      startParams.phone = this.phoneNumber;
      startParams.code = async () => await this._askQuestion('Enter the code you received: ');
      startParams.codeSentCallback = async (sentCode) => {
        if (this.options.forceSms && (sentCode.type === 'app' || sentCode.type === 'email')) {
          try {
            await this.client.resendCode({ phone: this.phoneNumber, phoneCodeHash: sentCode.phoneCodeHash });
            console.log('Code re-sent via SMS.');
          } catch (error) {
            const message = (error.text || error.message || '').toUpperCase();
            if (message.includes('SEND_CODE_UNAVAILABLE')) {
              console.log('SMS unavailable for this number. Please use the code sent via app.');
            } else {
              console.log(`Could not request SMS (${error.text || error.message}). Using code sent via ${sentCode.type}.`);
            }
          }
        } else {
          console.log(`The confirmation code has been sent via ${sentCode.type}.`);
        }
      };
    }

    return startParams;
  }

  async login(retriedAfterReset = false, retriedAfterSessionReset = false) {
    try {
      const hasExistingSession = await this._isAuthorized();

      if (!hasExistingSession && !this.options.useQr && !this.phoneNumber) {
        throw new Error('TELEGRAM_PHONE_NUMBER is not configured.');
      }

      await this.client.start(this._buildStartParams());

      console.log(hasExistingSession ? 'Existing session is valid.' : 'Logged in successfully!');
      return true;
    } catch (error) {
      if (!retriedAfterReset && this._isAuthKeyUnregisteredError(error)) {
        console.log('Detected AUTH_KEY_UNREGISTERED. Resetting local session and retrying login once...');
        try {
          await this._resetSessionAndClient();
          return await this.login(true, retriedAfterSessionReset);
        } catch (resetError) {
          console.error('Failed to recover from AUTH_KEY_UNREGISTERED:', resetError);
          return false;
        }
      }
      if (!retriedAfterSessionReset && this._isSessionResetError(error)) {
        console.log('Detected session reset during login. Recreating MTProto client and retrying once...');
        try {
          await this._recreateClient();
          return await this.login(retriedAfterReset, true);
        } catch (resetError) {
          console.error('Failed to recover from session reset:', resetError);
          return false;
        }
      }
      console.error('Error during login:', error);
      return false;
    }
  }

  async ensureLogin() {
    if (!(await this._isAuthorized())) {
      throw new Error('Not logged in to Telegram. Please restart the server.');
    }
    return true;
  }

  async initializeDialogCache() {
    console.log('Initializing dialog list...');
    const loginSuccess = await this.login();
    if (!loginSuccess) {
      throw new Error('Failed to login to Telegram. Cannot proceed.');
    }
    await this.startUpdates();
    console.log('Dialogs ready.');
    return true;
  }

  // Asks Telegram about the account's membership in channels and groups, given marked
  // ids, with one request per 100 of each kind. Returns a Map of id to 'member', 'left',
  // 'gone' or 'unknown'; private chats have no membership and are left out.
  async getChatMembership(chatIds) {
    await this.ensureLogin();
    const result = new Map();
    const channels = [];
    const groups = [];
    for (const id of chatIds) {
      let peer;
      try {
        peer = await this.client.resolvePeer(Number(id));
      } catch (error) {
        if (!(error instanceof MtPeerNotFoundError)) {
          throw error;
        }
        result.set(String(id), 'unknown');
        continue;
      }
      if (peer._ === 'inputPeerChannel') {
        channels.push({ id: String(id), input: { _: 'inputChannel', channelId: peer.channelId, accessHash: peer.accessHash } });
      } else if (peer._ === 'inputPeerChat') {
        groups.push({ id: String(id), input: peer.chatId });
      }
    }

    const ask = async (items, request, markedId) => {
      for (let start = 0; start < items.length; start += 100) {
        const batch = items.slice(start, start + 100);
        let chats;
        try {
          ({ chats } = await this.client.call(request(batch.map((item) => item.input))));
        } catch (error) {
          if (!isChatGone(error)) {
            throw error;
          }
          if (batch.length === 1) {
            result.set(batch[0].id, 'gone');
            continue;
          }
          // One refused chat fails the whole batch, so ask about each on its own.
          for (const item of batch) {
            await ask([item], request, markedId);
          }
          continue;
        }
        const byId = new Map(chats.map((chat) => [markedId(chat.id), membershipOf(chat)]));
        for (const item of batch) {
          result.set(item.id, byId.get(item.id) ?? 'unknown');
        }
      }
    };
    await ask(channels, (id) => ({ _: 'channels.getChannels', id }), (rawId) => String(toggleChannelIdMark(rawId)));
    await ask(groups, (id) => ({ _: 'messages.getChats', id }), (rawId) => String(-rawId));
    return result;
  }

  async listDialogs(limit = 50, { includeArchived = false, retriedAfterSessionReset = false } = {}) {
    try {
      await this.ensureLogin();
      const effectiveLimit = limit && limit > 0 ? limit : Infinity;
      const results = [];

      for await (const dialog of this.client.iterDialogs({ archived: includeArchived ? 'keep' : 'exclude' })) {
        const peer = dialog.peer;
        if (!peer) continue;

        const id = peer.id.toString();
        const username = 'username' in peer ? peer.username ?? null : null;
        const chatType = typeof peer.chatType === 'string' ? peer.chatType : null;
        const isForum = typeof peer.isForum === 'boolean' ? peer.isForum : null;
        const isGroup = typeof peer.isGroup === 'boolean' ? peer.isGroup : null;
        results.push({
          id,
          type: normalizePeerType(peer),
          title: peer.displayName || 'Unknown',
          username,
          chatType,
          isForum,
          isGroup,
          unreadCount: typeof dialog.unreadCount === 'number' ? dialog.unreadCount : 0,
          unreadMentionsCount: typeof dialog.unreadMentionsCount === 'number' ? dialog.unreadMentionsCount : 0,
          topMessageId: dialog.raw.topMessage,
        });

        if (results.length >= effectiveLimit) {
          break;
        }
      }

      return results;
    } catch (error) {
      if (!retriedAfterSessionReset && this._isSessionResetError(error)) {
        console.log('Detected session reset while listing dialogs. Recreating MTProto client and retrying once...');
        await this._recreateClient();
        const loginSuccess = await this.login();
        if (!loginSuccess) {
          throw new Error('Failed to restore session after dialog fetch reset.');
        }
        return this.listDialogs(limit, { includeArchived, retriedAfterSessionReset: true });
      }
      throw error;
    }
  }

  async searchPeers(query, limit = 50) {
    await this.ensureLogin();
    const result = await this.client.call({
      _: 'contacts.search',
      q: query,
      limit,
    });
    const results = [];
    for (const chat of (result.chats || [])) {
      results.push({
        id: chat.id.toString(),
        type: normalizePeerType(chat),
        title: chat.title || chat.firstName || 'Unknown',
        username: chat.username ?? null,
        chatType: chat._ ?? null,
        isForum: chat.forum ?? null,
        isGroup: null,
      });
    }
    for (const user of (result.users || [])) {
      results.push({
        id: user.id.toString(),
        type: 'user',
        title: [user.firstName, user.lastName].filter(Boolean).join(' ') || 'Unknown',
        username: user.username ?? null,
        chatType: 'user',
        isForum: null,
        isGroup: null,
      });
    }
    return results;
  }

  async searchDialogs(keyword, limit = 100, { local = false } = {}) {
    const query = sanitizeString(keyword).trim().toLowerCase();
    if (!query) {
      return [];
    }

    if (!local) {
      return this.searchPeers(query, limit);
    }

    await this.ensureLogin();
    const results = [];

    for await (const dialog of this.client.iterDialogs({})) {
      const peer = dialog.peer;
      if (!peer) continue;

      const title = (peer.displayName || '').toLowerCase();
      const username = ('username' in peer && peer.username ? peer.username : '').toLowerCase();

      if (title.includes(query) || username.includes(query)) {
        const chatType = typeof peer.chatType === 'string' ? peer.chatType : null;
        const isForum = typeof peer.isForum === 'boolean' ? peer.isForum : null;
        const isGroup = typeof peer.isGroup === 'boolean' ? peer.isGroup : null;
        results.push({
          id: peer.id.toString(),
          type: normalizePeerType(peer),
          title: peer.displayName || 'Unknown',
          username: 'username' in peer ? peer.username ?? null : null,
          chatType,
          isForum,
          isGroup,
          unreadCount: typeof dialog.unreadCount === 'number' ? dialog.unreadCount : 0,
          unreadMentionsCount: typeof dialog.unreadMentionsCount === 'number' ? dialog.unreadMentionsCount : 0,
        });
      }

      if (results.length >= limit) {
        break;
      }
    }

    return results;
  }

  async getMessagesByChannelId(channelId, limit = 100, options = {}) {
    await this.ensureLogin();

    const {
      maxId = 0,
      reverse = false,
      offsetId = 0,
      beforeId = 0,
      afterId = 0,
    } = options;
    const peerRef = normalizeChannelId(channelId);
    const peer = await this.client.resolvePeer(peerRef);

    const effectiveLimit = limit && limit > 0 ? limit : 100;
    const effectiveBeforeId = normalizePositiveMessageId(beforeId)
      || normalizePositiveMessageId(offsetId);
    const effectiveAfterId = normalizePositiveMessageId(afterId);
    const effectiveMaxId = normalizePositiveMessageId(maxId);
    const useAfterIdPagination = Boolean(effectiveAfterId);
    const messages = [];

    const iterOptions = {
      limit: effectiveLimit,
      chunkSize: Math.min(effectiveLimit, 100),
      reverse: useAfterIdPagination ? true : reverse,
    };

    if (effectiveMaxId) {
      iterOptions.maxId = effectiveMaxId;
    }

    if (useAfterIdPagination) {
      // Telegram slices the history by offset before it applies min_id, so the slice
      // has to start right above afterId. mtcute's default reverse offset starts at the
      // chat's oldest messages, and min_id would then filter that whole slice away.
      iterOptions.offset = { id: effectiveAfterId + 1, date: 0 };
    }

    if (effectiveBeforeId) {
      if (useAfterIdPagination || iterOptions.maxId) {
        // max_id excludes its own id, as beforeId does.
        iterOptions.maxId = iterOptions.maxId
          ? Math.min(iterOptions.maxId, effectiveBeforeId)
          : effectiveBeforeId;
      } else {
        iterOptions.offset = { id: effectiveBeforeId, date: 0 };
        iterOptions.addOffset = 0;
      }
    }

    // resolvePeer gives an input peer, which has no title or type of its own.
    let chat = null;
    for await (const message of this.client.iterHistory(peer, iterOptions)) {
      chat ??= message.chat;
      messages.push(this._serializeMessage(message, peer));
      if (messages.length >= effectiveLimit) {
        break;
      }
    }

    if (useAfterIdPagination) {
      messages.reverse();
    }

    return {
      peerTitle: chat?.displayName ?? null,
      peerId: peer?.id?.toString?.() ?? String(channelId),
      peerType: chat ? normalizePeerType(chat) : null,
      messages: filterSerializedMessagesById(messages, {
        beforeId: effectiveBeforeId,
        afterId: effectiveAfterId,
      }),
    };
  }

  async getMessageById(channelId, messageId) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    const [message] = await this.client.getMessages(peerRef, Number(messageId));
    if (!message) {
      return null;
    }
    const peer = message.chat ?? await this.client.resolvePeer(peerRef);
    return this._serializeMessage(message, peer);
  }

  async getMessageContext(channelId, messageId, options = {}) {
    await this.ensureLogin();
    const safeBefore = Number.isFinite(options.before) && options.before >= 0
      ? Number(options.before)
      : 20;
    const safeAfter = Number.isFinite(options.after) && options.after >= 0
      ? Number(options.after)
      : 20;
    const total = safeBefore + safeAfter + 1;

    const peerRef = normalizeChannelId(channelId);
    const [message] = await this.client.getMessages(peerRef, Number(messageId));
    if (!message) {
      return { target: null, before: [], after: [] };
    }

    let dateSeconds = 0;
    if (message.date instanceof Date) {
      dateSeconds = Math.floor(message.date.getTime() / 1000);
    } else if (typeof message.date === 'number') {
      dateSeconds = Math.floor(message.date);
    }

    const history = await this.client.getHistory(peerRef, {
      offset: {
        id: message.id,
        date: dateSeconds,
      },
      addOffset: -safeAfter,
      limit: total,
    });

    const peer = message.chat ?? await this.client.resolvePeer(peerRef);
    const target = this._serializeMessage(message, peer);
    const before = [];
    const after = [];

    for (const entry of history) {
      const serialized = this._serializeMessage(entry, peer);
      if (serialized.id < target.id) {
        before.push(serialized);
      } else if (serialized.id > target.id) {
        after.push(serialized);
      }
    }

    before.sort((a, b) => a.id - b.id);
    after.sort((a, b) => a.id - b.id);

    return {
      target,
      before: before.slice(-safeBefore),
      after: after.slice(0, safeAfter),
    };
  }

  async searchChannelMessages(channelId, options = {}) {
    await this.ensureLogin();
    const query = typeof options.query === 'string' ? options.query : '';
    const limit = options.limit && options.limit > 0 ? Number(options.limit) : 50;
    const threadId = typeof options.topicId === 'number' ? options.topicId : undefined;
    const peerRef = normalizeChannelId(channelId);
    const peer = await this.client.resolvePeer(peerRef);
    const results = await this.client.searchMessages({
      chatId: peerRef,
      threadId,
      limit,
      query,
    });
    const messages = filterSerializedMessagesById(
      results.map((message) => this._serializeMessage(message, peer)),
      options,
    );

    return {
      peerTitle: peer?.displayName || 'Unknown',
      peerId: peer?.id?.toString?.() ?? String(channelId),
      peerType: normalizePeerType(peer),
      total: results.total ?? messages.length,
      next: results.next ?? null,
      messages,
    };
  }

  async sendTextMessage(channelId, text, options = {}) {
    await this.ensureLogin();
    const messageText = typeof text === 'string' ? text : String(text ?? '');
    if (!messageText.trim()) {
      throw new Error('Message text cannot be empty.');
    }
    const parseMode = normalizeParseMode(options.parseMode);
    const inputText = applyParseMode(messageText, parseMode);
    const params = buildTextSendParams(options);
    const peerRef = normalizeChannelId(channelId);
    const sent = await this.client.sendText(peerRef, inputText, params);
    options.onSent?.(sent);
    return sent.isScheduled ? { messageId: sent.id, scheduled: true } : { messageId: sent.id };
  }

  async sendFileMessage(channelId, filePath, options = {}) {
    await this.ensureLogin();
    const uploadPath = resolveUploadPath(filePath);
    const caption = resolveOptionalCaption(options.caption);
    const parseMode = normalizeParseMode(options.parseMode);
    if (parseMode && !caption) {
      throw new Error('--parse-mode requires --caption for send file');
    }
    const parsedCaption = caption ? applyParseMode(caption, parseMode) : undefined;
    const fileName = typeof options.filename === 'string' && options.filename.trim()
      ? options.filename.trim() : undefined;
    if (options.captionAbove && !caption) {
      throw new Error('--caption-above requires --caption for send file');
    }
    const mediaOptions = { caption: parsedCaption, fileName };
    if (options.spoiler) mediaOptions.spoiler = true;
    if (options.forceDocument) mediaOptions.forceDocument = true;
    const media = InputMedia.auto(uploadPath, mediaOptions);
    const peerRef = normalizeChannelId(channelId);
    const sent = await this.client.sendMedia(peerRef, media, buildMediaSendParams(options));
    options.onSent?.(sent);
    return buildSendMessageResult(sent, { method: 'sendDocument', defaultMediaType: 'document' });
  }

  async preparePhotoMessage(channelId, filePath, options = {}) {
    await this.ensureLogin();
    const uploadPath = resolveUploadPath(filePath);
    const caption = resolveOptionalCaption(options.caption);
    const parseMode = normalizeParseMode(options.parseMode);
    if (parseMode && !caption) {
      throw new Error('--parse-mode requires --caption for send photo');
    }
    if (options.captionAbove && !caption) {
      throw new Error('--caption-above requires --caption for send photo');
    }
    const mediaOptions = {};
    let parsedCaption;
    if (caption) {
      parsedCaption = applyParseMode(caption, parseMode);
      mediaOptions.caption = parsedCaption;
    }
    if (options.spoiler) mediaOptions.spoiler = true;
    const media = InputMedia.photo(uploadPath, mediaOptions);
    const { message, entities } = splitInputText(parsedCaption);
    return {
      method: 'sendPhoto',
      peerRef: normalizeChannelId(channelId),
      media,
      request: {
        _: 'messages.sendMedia',
        silent: options.silent ? true : undefined,
        replyTo: buildLowLevelReplyTo(options),
        randomId: options.randomId ?? randomLong(),
        scheduleDate: resolveScheduleDate(options),
        message,
        entities,
        noforwards: options.noForwards || options.noforwards ? true : undefined,
        invertMedia: options.captionAbove ? true : undefined,
      },
    };
  }

  async sendPreparedPhotoMessage(prepared, { onSent } = {}) {
    const peer = await this.client.resolvePeer(prepared.peerRef);
    const chatId = peer?._ === 'inputPeerSelf'
      ? String(prepared.peerRef)
      : this._extractPeerId(peer);
    const normalizedMedia = await this.client._normalizeInputMedia(prepared.media, { uploadPeer: peer });
    const request = { ...prepared.request, peer, media: normalizedMedia };
    const result = await this.client.call(request);
    this.client.handleClientUpdate(result, true);
    const { messageId, messageUpdate } = readSendResponse(result, prepared.request.randomId);
    const sentMessage = onSent && messageUpdate ? parseSentMessage(messageUpdate, result, prepared.peerRef) : null;
    if (sentMessage) onSent(sentMessage);
    if (!messageId) {
      throw new Error('Failed to resolve sent photo message id from Telegram updates.');
    }
    const scheduledField = prepared.request.scheduleDate ? { scheduled: true } : {};
    let sent;
    try {
      // Scheduled messages have their own id space; getMessages would miss them or hit an unrelated message.
      [sent] = prepared.request.scheduleDate
        ? await this.client.getScheduledMessages(prepared.peerRef, Number(messageId))
        : await this.client.getMessages(prepared.peerRef, Number(messageId));
    } catch (error) {
      console.error(`[sendPhoto] message lookup for enrichment failed for peer ${prepared.peerRef}, message ${messageId}: ${error.message}`);
    }
    if (sent) {
      return { chatId, ...buildSendMessageResult(sent, { method: 'sendPhoto', defaultMediaType: 'photo' }), ...scheduledField };
    }
    return {
      chatId,
      messageId: Number(messageId),
      ...scheduledField,
      method: 'sendPhoto',
      media: { type: 'photo' },
      warning: 'Media enrichment failed; file_id unavailable',
    };
  }

  async sendPhotoMessage(channelId, filePath, options = {}) {
    const prepared = await this.preparePhotoMessage(channelId, filePath, options);
    return this.sendPreparedPhotoMessage(prepared, options);
  }

  async downloadMessageMedia(channelId, messageId, options = {}) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    const [message] = await this.client.getMessages(peerRef, Number(messageId));
    if (!message) {
      throw new Error('Message not found.');
    }

    const location = resolveDownloadLocation(message.media);
    if (!location) {
      throw new Error('Message has no downloadable media.');
    }

    const summary = summarizeMedia(message.media);
    const targetPath = resolveDownloadPath(options.outputPath, {
      channelId,
      messageId,
      summary,
    });
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    // Do not switch to mtcute's downloadToFile: it resolves before its write
    // stream flushes, so an immediate stat can report 0 bytes. pipeline
    // resolves only after the destination stream finishes, so the stat below
    // always sees the complete file.
    try {
      await pipeline(this.client.downloadAsNodeStream(location), fs.createWriteStream(targetPath));
    } catch (error) {
      try {
        fs.rmSync(targetPath, { force: true });
      } catch {
        // Best-effort cleanup of the partial file; surface the original error.
      }
      throw error;
    }
    const stats = fs.statSync(targetPath);

    return {
      path: targetPath,
      bytes: stats.size,
      mimeType: summary?.mimeType ?? null,
      downloadedAt: new Date().toISOString(),
    };
  }

  async listContacts() {
    await this.ensureLogin();
    return this.client.getContacts();
  }

  async getUserProfile(userId) {
    await this.ensureLogin();
    return this.client.getUser(userId);
  }

  async listGroups(options = {}) {
    await this.ensureLogin();
    const query = typeof options.query === 'string' ? options.query.trim().toLowerCase() : '';
    const limit = options.limit && options.limit > 0 ? Number(options.limit) : 100;
    const results = [];

    for await (const dialog of this.client.iterDialogs({})) {
      const peer = dialog.peer;
      if (!peer || !isGroupPeer(peer)) {
        continue;
      }
      const title = peer.displayName || 'Unknown';
      const username = 'username' in peer ? peer.username ?? null : null;
      const haystack = `${title} ${username ?? ''}`.toLowerCase();
      if (query && !haystack.includes(query)) {
        continue;
      }

      results.push({
        id: peer.id.toString(),
        title,
        username,
        chatType: typeof peer.chatType === 'string' ? peer.chatType : null,
        isForum: typeof peer.isForum === 'boolean' ? peer.isForum : null,
        membersCount: peer.membersCount ?? null,
      });

      if (results.length >= limit) {
        break;
      }
    }

    return results;
  }

  async getGroupInfo(channelId) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    const chat = await this.client.getChat(peerRef);
    const full = await this.client.getFullChat(peerRef).catch(() => null);
    return {
      id: chat.id?.toString?.() ?? String(channelId),
      title: chat.displayName || chat.title || 'Unknown',
      username: chat.username ?? null,
      chatType: typeof chat.chatType === 'string' ? chat.chatType : null,
      isForum: typeof chat.isForum === 'boolean' ? chat.isForum : null,
      isMember: typeof chat.isMember === 'boolean' ? chat.isMember : null,
      isAdmin: typeof chat.isAdmin === 'boolean' ? chat.isAdmin : null,
      isCreator: typeof chat.isCreator === 'boolean' ? chat.isCreator : null,
      membersCount: full?.membersCount ?? chat.membersCount ?? null,
      about: full?.bio ?? null,
    };
  }

  async renameGroup(channelId, title) {
    await this.ensureLogin();
    const value = typeof title === 'string' ? title.trim() : '';
    if (!value) {
      throw new Error('Group title must be a non-empty string.');
    }
    const peerRef = normalizeChannelId(channelId);
    await this.client.setChatTitle(peerRef, value);
    return true;
  }

  async addGroupMembers(channelId, userIds) {
    await this.ensureLogin();
    const users = Array.isArray(userIds) ? userIds : [userIds];
    if (!users.length) {
      throw new Error('userIds must include at least one entry.');
    }
    const peerRef = normalizeChannelId(channelId);
    const failed = await this.client.addChatMembers(peerRef, users);
    return failed.map((entry) => ({
      userId: entry.userId?.toString?.() ?? null,
      error: entry.error ?? null,
    }));
  }

  async removeGroupMembers(channelId, userIds) {
    await this.ensureLogin();
    const users = Array.isArray(userIds) ? userIds : [userIds];
    if (!users.length) {
      throw new Error('userIds must include at least one entry.');
    }
    const peerRef = normalizeChannelId(channelId);
    const removed = [];
    const failed = [];
    for (const userId of users) {
      try {
        await this.client.kickChatMember({ chatId: peerRef, userId });
        removed.push(String(userId));
      } catch (error) {
        failed.push({ userId: String(userId), error: error?.message ?? String(error) });
      }
    }
    return { removed, failed };
  }

  async getGroupInviteLink(channelId) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    return this.client.getPrimaryInviteLink(peerRef);
  }

  async revokeGroupInviteLink(channelId, link) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    return this.client.revokeInviteLink(peerRef, link);
  }

  async joinGroup(invite) {
    await this.ensureLogin();
    return this.client.joinChat(invite);
  }

  async leaveGroup(channelId) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    await this.client.leaveChat(peerRef);
    return true;
  }

  async markChannelRead(channelId, messageId) {
    await this.ensureLogin();
    const msgId = Number(messageId);
    if (!Number.isInteger(msgId) || msgId <= 0) {
      throw new Error('messageId must be a positive integer');
    }
    const peerRef = normalizeChannelId(channelId);
    const peer = await this.client.resolvePeer(peerRef);
    await this.client.readHistory(peer, { maxId: msgId });
    return { channelId: peer?.id?.toString?.() ?? String(channelId), messageId: msgId };
  }

  async getPeerMetadata(channelId, peerType) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);

    const buildUserMetadata = async () => {
      const user = await this.client.getFullUser(peerRef);
      return {
        peerTitle: user.displayName || 'Unknown',
        username: user.username ?? null,
        peerType: normalizePeerType(user),
        chatType: null,
        isForum: null,
        about: user.bio || null,
      };
    };

    if (peerType === 'user') {
      return buildUserMetadata();
    }

    let peerTitle = null;
    let username = null;
    let resolvedType = peerType ?? null;
    let chatType = null;
    let isForum = null;
    try {
      const chat = await this.client.getChat(peerRef);
      peerTitle = chat.displayName || chat.title || 'Unknown';
      username = chat.username ?? null;
      resolvedType = normalizePeerType(chat);
      chatType = typeof chat.chatType === 'string' ? chat.chatType : null;
      isForum = typeof chat.isForum === 'boolean' ? chat.isForum : null;
    } catch (error) {
      return buildUserMetadata();
    }

    let about = null;
    try {
      const fullChat = await this.client.getFullChat(peerRef);
      about = fullChat.bio || null;
    } catch (error) {
      about = null;
    }

    return {
      peerTitle,
      username,
      peerType: resolvedType,
      chatType,
      isForum,
      about,
    };
  }

  _serializeMessage(message, peer = null) {
    const resolvedPeer = peer ?? message?.chat ?? null;
    const id = typeof message.id === 'number' ? message.id : Number(message.id || 0);
    let dateSeconds = null;
    if (message.date instanceof Date) {
      dateSeconds = Math.floor(message.date.getTime() / 1000);
    } else if (typeof message.date === 'number') {
      dateSeconds = Math.floor(message.date);
    }

    let textContent = '';
    if (typeof message.text === 'string') {
      textContent = message.text;
    } else if (typeof message.message === 'string') {
      textContent = message.message;
    } else if (message.text && typeof message.text.toString === 'function') {
      textContent = message.text.toString();
    }

    // Extract URLs from message entities (text_link and url kinds)
    const urls = [];
    const entities = message.entities;
    if (Array.isArray(entities)) {
      for (const entity of entities) {
        try {
          const kind = entity.kind;
          if (kind === 'text_link') {
            const url = entity.params?.url;
            if (url) urls.push(url);
          } else if (kind === 'url') {
            const urlText = entity.text;
            if (urlText) urls.push(urlText);
          }
        } catch {}
      }
    }

    const sender = message.sender || message.from || message.author;
    let senderId = sender?.id ? sender.id.toString() : null;
    if (!senderId) {
      const rawFrom = message.fromId ?? message.raw?.fromId;
      if (rawFrom && typeof rawFrom === 'object') {
        senderId = (rawFrom.userId ?? rawFrom.channelId ?? rawFrom.chatId ?? 'unknown').toString();
      } else if (rawFrom) {
        senderId = rawFrom.toString();
      } else {
        senderId = 'unknown';
      }
    }
    const topicId = extractTopicId(message);
    const senderUsername = typeof sender?.username === 'string' && sender.username ? sender.username : null;
    let senderDisplayName = null;
    if (typeof sender?.displayName === 'string' && sender.displayName.trim()) {
      senderDisplayName = sender.displayName.trim();
    } else {
      const nameParts = [sender?.firstName, sender?.lastName].filter(Boolean);
      senderDisplayName = nameParts.length ? nameParts.join(' ') : null;
    }
    const senderPeerType = sender ? normalizePeerType(sender) : null;
    const senderIsBot = typeof sender?.isBot === 'boolean' ? sender.isBot : null;
    const mediaSummary = summarizeMedia(message.media);

    return {
      id,
      date: dateSeconds,
      message: textContent,
      text: textContent,
      urls: urls.length > 0 ? urls : null,
      from_id: senderId,
      from_username: senderUsername,
      from_display_name: senderDisplayName,
      from_peer_type: senderPeerType,
      from_is_bot: senderIsBot,
      peer_type: normalizePeerType(resolvedPeer),
      peer_id: resolvedPeer?.id?.toString?.() ?? 'unknown',
      topic_id: topicId,
      media: mediaSummary,
      raw: message.raw ?? null,
    };
  }

  _extractPeerId(peer) {
    if (peer == null) return 'unknown';
    if (typeof peer !== 'object') return String(peer);
    const id = peer.userId ?? peer.channelId ?? peer.chatId;
    return id != null ? String(id) : 'unknown';
  }

  filterMessagesByPattern(messages, pattern) {
    if (!Array.isArray(messages)) {
      return [];
    }

    const regex = new RegExp(pattern);
    return messages
      .map(msg => (typeof msg === 'string' ? msg : msg.message || msg.text || ''))
      .filter(text => typeof text === 'string' && regex.test(text));
  }

  async destroy() {
    if (this.updatesRunning) {
      try {
        await this.client.stopUpdatesLoop();
      } catch (error) {
        console.warn('[warning] failed to stop updates loop:', error?.message || error);
      }
      this.updatesRunning = false;
    }
    if (this.rawUpdateHandler) {
      this.client.onRawUpdate.remove(this.rawUpdateHandler);
      this.rawUpdateHandler = null;
    }
    await this.client.destroy();
  }

  onUpdate(listener) {
    this.updateEmitter.on('update', listener);
    return () => this.updateEmitter.off('update', listener);
  }

  onChannelTooLong(listener) {
    this.updateEmitter.on('channelTooLong', listener);
    return () => this.updateEmitter.off('channelTooLong', listener);
  }

  // Listeners on the mtcute client die with it. Fires before the new client logs in,
  // so whoever re-subscribes here is in time for its catch-up.
  onClientReplaced(listener) {
    this.updateEmitter.on('clientReplaced', listener);
    return () => this.updateEmitter.off('clientReplaced', listener);
  }

  async startUpdates() {
    if (this.updatesRunning) {
      return;
    }
    try {
      if (!this.rawUpdateHandler) {
        this.rawUpdateHandler = (update) => {
          this.updateEmitter.emit('update', update);
        };
        this.client.onRawUpdate.add(this.rawUpdateHandler);
      }
      await this.client.startUpdatesLoop();
      this.updatesRunning = true;
    } catch (error) {
      console.warn('[warning] failed to start updates loop:', error?.message || error);
    }
  }

  async listForumTopics(channelId, options = {}) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    return this.client.getForumTopics(peerRef, options);
  }

  async getTopicMessages(channelId, topicId, limit = 50, options = {}) {
    await this.ensureLogin();
    const peerRef = normalizeChannelId(channelId);
    const results = await this.client.searchMessages({
      chatId: peerRef,
      threadId: topicId,
      limit,
      query: options.query ?? '',
    });
    const messages = filterSerializedMessagesById(
      results.map((message) => this._serializeMessage(message, message.chat)),
      options,
    );

    return {
      total: results.total ?? messages.length,
      next: results.next ?? null,
      messages,
    };
  }
  // --- Chat Folders ---

  async getFolders() {
    await this.ensureLogin();
    const result = await this.client.getFolders();
    const filters = result?.filters ?? [];
    return filters.map((f) => {
      if (f._ === 'dialogFilterDefault') return { id: 0, title: 'All Chats', type: 'default' };
      return {
        id: f.id,
        title: typeof f.title === 'string' ? f.title : (f.title?.text ?? 'Unknown'),
        emoji: f.emoticon ?? null,
        color: f.color ?? null,
        type: f._ === 'dialogFilterChatlist' ? 'chatlist' : 'filter',
        contacts: f.contacts ?? false,
        nonContacts: f.nonContacts ?? false,
        groups: f.groups ?? false,
        broadcasts: f.broadcasts ?? false,
        bots: f.bots ?? false,
        excludeMuted: f.excludeMuted ?? false,
        excludeRead: f.excludeRead ?? false,
        excludeArchived: f.excludeArchived ?? false,
        includePeers: f.includePeers?.length ?? 0,
        excludePeers: f.excludePeers?.length ?? 0,
        pinnedPeers: f.pinnedPeers?.length ?? 0,
      };
    });
  }

  async findFolder(idOrName) {
    await this.ensureLogin();
    const trimmed = String(idOrName).trim();
    if (trimmed === '') throw new Error('Folder identifier cannot be empty');
    if (/^\d+$/.test(trimmed)) return this.client.findFolder({ id: Number(trimmed) });
    return this.client.findFolder({ title: trimmed });
  }

  async showFolder(idOrName, options = {}) {
    await this.ensureLogin();
    const folder = await this.findFolder(idOrName);
    if (!folder) throw new Error(`Folder not found: ${idOrName}`);

    const normalizePeers = (peers) => (peers ?? []).map((p) => this._normalizePeer(p));

    const resolvePeers = async (peers) => {
      const normalized = normalizePeers(peers);
      if (!options.resolve) return normalized;

      return Promise.all(normalized.map(async (peer) => {
        const name = await this._resolvePeerName(peer.type, peer.id);
        const nameField = peer.type === 'user' ? 'name' : 'title';
        return { ...peer, [nameField]: name ?? '(unresolved)' };
      }));
    };

    return {
      id: folder.id,
      title: typeof folder.title === 'string' ? folder.title : (folder.title?.text ?? 'Unknown'),
      emoji: folder.emoticon ?? null,
      color: folder.color ?? null,
      type: folder._ === 'dialogFilterChatlist' ? 'chatlist' : folder._ === 'dialogFilterDefault' ? 'default' : 'filter',
      contacts: folder.contacts ?? false,
      nonContacts: folder.nonContacts ?? false,
      groups: folder.groups ?? false,
      broadcasts: folder.broadcasts ?? false,
      bots: folder.bots ?? false,
      excludeMuted: folder.excludeMuted ?? false,
      excludeRead: folder.excludeRead ?? false,
      excludeArchived: folder.excludeArchived ?? false,
      includePeers: await resolvePeers(folder.includePeers),
      excludePeers: await resolvePeers(folder.excludePeers),
      pinnedPeers: await resolvePeers(folder.pinnedPeers),
    };
  }

  async createFolder(options) {
    await this.ensureLogin();
    if (!options.title) throw new Error('Folder title is required');
    const params = { title: options.title };
    if (options.emoji) params.emoticon = options.emoji;
    if (options.contacts !== undefined) params.contacts = options.contacts;
    if (options.nonContacts !== undefined) params.nonContacts = options.nonContacts;
    if (options.groups !== undefined) params.groups = options.groups;
    if (options.broadcasts !== undefined) params.broadcasts = options.broadcasts;
    if (options.bots !== undefined) params.bots = options.bots;
    if (options.excludeMuted !== undefined) params.excludeMuted = options.excludeMuted;
    if (options.excludeRead !== undefined) params.excludeRead = options.excludeRead;
    if (options.excludeArchived !== undefined) params.excludeArchived = options.excludeArchived;
    if (options.includePeers?.length) params.includePeers = options.includePeers;
    if (options.excludePeers?.length) params.excludePeers = options.excludePeers;
    if (options.pinnedPeers?.length) params.pinnedPeers = options.pinnedPeers;
    const result = await this.client.createFolder(params);
    return { id: result.id, title: typeof result.title === 'string' ? result.title : (result.title?.text ?? 'Unknown') };
  }

  async editFolder(idOrName, modification) {
    await this.ensureLogin();
    const folder = await this.findFolder(idOrName);
    if (!folder) throw new Error(`Folder not found: ${idOrName}`);
    if (this._isDefaultFolder(folder)) throw new Error('Cannot modify the default "All Chats" folder');
    const mod = {};
    if (modification.title !== undefined) mod.title = modification.title;
    if (modification.emoji !== undefined) mod.emoticon = modification.emoji;
    if (modification.contacts !== undefined) mod.contacts = modification.contacts;
    if (modification.nonContacts !== undefined) mod.nonContacts = modification.nonContacts;
    if (modification.groups !== undefined) mod.groups = modification.groups;
    if (modification.broadcasts !== undefined) mod.broadcasts = modification.broadcasts;
    if (modification.bots !== undefined) mod.bots = modification.bots;
    if (modification.excludeMuted !== undefined) mod.excludeMuted = modification.excludeMuted;
    if (modification.excludeRead !== undefined) mod.excludeRead = modification.excludeRead;
    if (modification.excludeArchived !== undefined) mod.excludeArchived = modification.excludeArchived;
    if (modification.includePeers !== undefined) mod.includePeers = modification.includePeers;
    if (modification.excludePeers !== undefined) mod.excludePeers = modification.excludePeers;
    if (modification.pinnedPeers !== undefined) mod.pinnedPeers = modification.pinnedPeers;
    const result = await this.client.editFolder({ folder, modification: mod });
    return { id: result.id, title: typeof result.title === 'string' ? result.title : (result.title?.text ?? 'Unknown') };
  }

  async deleteFolder(idOrName) {
    await this.ensureLogin();
    const folder = await this.findFolder(idOrName);
    if (!folder) throw new Error(`Folder not found: ${idOrName}`);
    if (this._isDefaultFolder(folder)) throw new Error('Cannot delete the default "All Chats" folder');
    await this.client.deleteFolder(folder.id);
    return { deleted: true, id: folder.id };
  }

  async setFoldersOrder(ids) {
    await this.ensureLogin();
    if (!ids.length) throw new Error('At least one folder ID is required');
    const numericIds = ids.map((id) => {
      const n = Number(id);
      if (!Number.isInteger(n) || n < 0) throw new Error(`Invalid folder ID: ${id}`);
      return n;
    });
    const unique = new Set(numericIds);
    if (unique.size !== numericIds.length) throw new Error('Duplicate folder IDs are not allowed');
    await this.client.setFoldersOrder(numericIds);
    return { ok: true };
  }

  async addChatToFolder(idOrName, chatId) {
    await this.ensureLogin();
    const folder = await this.findFolder(idOrName);
    if (!folder) throw new Error(`Folder not found: ${idOrName}`);
    if (this._isDefaultFolder(folder)) throw new Error('Cannot modify the default "All Chats" folder');
    const peers = folder.includePeers ? [...folder.includePeers] : [];
    const chatIdStr = String(chatId);
    const alreadyIncluded = peers.some((p) => {
      const id = this._extractPeerId(p);
      return id === chatIdStr;
    });
    if (alreadyIncluded) throw new Error(`Chat ${chatId} already in folder ${folder.id}`);
    peers.push(chatId);
    await this.client.editFolder({ folder, modification: { includePeers: peers } });
    return { ok: true, folderId: folder.id };
  }

  async removeChatFromFolder(idOrName, chatId) {
    await this.ensureLogin();
    const folder = await this.findFolder(idOrName);
    if (!folder) throw new Error(`Folder not found: ${idOrName}`);
    if (this._isDefaultFolder(folder)) throw new Error('Cannot modify the default "All Chats" folder');
    const chatIdStr = String(chatId);
    const originalPeers = folder.includePeers ?? [];
    const peers = originalPeers.filter((p) => {
      const peerId = this._extractPeerId(p);
      return peerId !== chatIdStr;
    });
    if (peers.length === originalPeers.length) {
      throw new Error(`Chat ${chatId} not found in folder ${folder.id}`);
    }
    await this.client.editFolder({ folder, modification: { includePeers: peers } });
    return { ok: true, folderId: folder.id };
  }

  async joinChatlist(link) {
    await this.ensureLogin();
    if (!/^https?:\/\/t\.me\/addlist\/[a-zA-Z0-9_-]+\/?(\?[^\s]*)?$/.test(link)) {
      throw new Error(`Invalid chatlist link: ${link}. Expected format: https://t.me/addlist/<slug>`);
    }
    const result = await this.client.joinChatlist(link);
    if (!result) throw new Error(`Failed to join chatlist: no result returned for ${link}`);
    return {
      id: result.id,
      title: typeof result.title === 'string' ? result.title : (result.title?.text ?? 'Unknown'),
      type: 'chatlist',
    };
  }

  // --- Folder helper methods ---

  _isDefaultFolder(folder) {
    return folder.id === 0 || folder._ === 'dialogFilterDefault';
  }

  _extractPeerId(peer) {
    if (peer == null) throw new Error(`Peer is ${peer}, cannot extract ID`);
    if (typeof peer !== 'object') return String(peer);
    const id = peer.userId ?? peer.channelId ?? peer.chatId;
    if (id == null) throw new Error(`Peer object has no recognizable ID field: ${JSON.stringify(peer)}`);
    return String(id);
  }

  _normalizePeer(peer) {
    if (peer == null) throw new Error(`Peer is ${peer}, cannot normalize`);
    if (typeof peer !== 'object') throw new Error(`Peer must be an object, got ${typeof peer}`);

    let type, id;
    if (peer.userId != null) {
      type = 'user';
      id = Number(peer.userId);
    } else if (peer.channelId != null) {
      type = 'channel';
      id = Number(peer.channelId);
    } else if (peer.chatId != null) {
      type = 'chat';
      id = Number(peer.chatId);
    } else {
      throw new Error(`Peer object has no recognizable ID field: ${JSON.stringify(peer)}`);
    }

    return { type, id };
  }

  async _resolvePeerName(type, id) {
    try {
      if (type === 'user') {
        const user = await this.client.getFullUser(id);
        return user.displayName || user.firstName || null;
      }
      const chat = await this.client.getChat(id);
      return chat.displayName || chat.title || null;
    } catch {
      return null;
    }
  }
}

export default TelegramClient;
