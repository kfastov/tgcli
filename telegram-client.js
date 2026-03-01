import { TelegramClient as MtCuteClient } from '@mtcute/node';
import { InputMedia } from '@mtcute/core';
import EventEmitter from 'events';
import fs from 'fs';
import { stat } from 'fs/promises';
import os from 'os';
import path from 'path';
import readline from 'readline';
import { Readable } from 'stream';
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

class TelegramClient {
  constructor(apiId, apiHash, phoneNumber, sessionPath = DEFAULT_SESSION_PATH, options = {}) {
    this.apiId = coerceApiId(apiId);
    this.apiHash = sanitizeString(apiHash);
    this.phoneNumber = sanitizeString(phoneNumber);
    this.sessionPath = path.resolve(sessionPath);

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

    this.client = new MtCuteClient({
      apiId: this.apiId,
      apiHash: this.apiHash,
      storage: this.sessionPath,
      platform: createPlatform(),
      updates: updatesConfig,
    });
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
  }

  async _askHiddenQuestion(prompt) {
    if (!process.stdin.isTTY) {
      return this._askQuestion(prompt);
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

  async login() {
    try {
      if (await this._isAuthorized()) {
        console.log('Existing session is valid.');
        return true;
      }

      if (!this.phoneNumber) {
        throw new Error('TELEGRAM_PHONE_NUMBER is not configured.');
      }

      await this.client.start({
        phone: this.phoneNumber,
        code: async () => await this._askQuestion('Enter the code you received: '),
        password: async () => {
          const value = await this._askHiddenQuestion('Enter your 2FA password (leave empty if not enabled): ');
          return value.length ? value : undefined;
        },
      });

      console.log('Logged in successfully!');
      return true;
    } catch (error) {
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

  async listDialogs(limit = 50) {
    await this.ensureLogin();
    const effectiveLimit = limit && limit > 0 ? limit : Infinity;
    const results = [];

    for await (const dialog of this.client.iterDialogs({})) {
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
      });

      if (results.length >= effectiveLimit) {
        break;
      }
    }

    return results;
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
      minId = 0,
      maxId = 0,
      reverse = false,
      offsetId = 0,
    } = options;
    const peerRef = normalizeChannelId(channelId);
    const peer = await this.client.resolvePeer(peerRef);

    const effectiveLimit = limit && limit > 0 ? limit : 100;
    const messages = [];

    const iterOptions = {
      limit: effectiveLimit,
      chunkSize: Math.min(effectiveLimit, 100),
      reverse,
    };

    if (minId) {
      iterOptions.minId = minId;
    }

    if (maxId) {
      iterOptions.maxId = maxId;
    }

    if (offsetId) {
      iterOptions.offset = { id: offsetId, date: 0 };
      iterOptions.addOffset = 0;
    }

    for await (const message of this.client.iterHistory(peer, iterOptions)) {
      messages.push(this._serializeMessage(message, peer));
      if (messages.length >= effectiveLimit) {
        break;
      }
    }

    return {
      peerTitle: peer?.displayName || 'Unknown',
      peerId: peer?.id?.toString?.() ?? String(channelId),
      peerType: normalizePeerType(peer),
      messages,
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
    const messages = results.map((message) => this._serializeMessage(message, peer));

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
    const replyTo = Number.isFinite(options.replyToMessageId)
      ? options.replyToMessageId
      : (Number.isFinite(options.topicId) ? options.topicId : undefined);
    const params = replyTo ? { replyTo } : undefined;
    const peerRef = normalizeChannelId(channelId);
    const sent = await this.client.sendText(peerRef, messageText, params);
    return { messageId: sent.id };
  }

  async sendFileMessage(channelId, filePath, options = {}) {
    await this.ensureLogin();
    if (!filePath || typeof filePath !== 'string') {
      throw new Error('filePath must be a string.');
    }
    const resolved = path.resolve(filePath);
    if (!fs.existsSync(resolved)) {
      throw new Error(`File not found: ${resolved}`);
    }
    const uploadPath = `file:${resolved}`;
    const caption = typeof options.caption === 'string' && options.caption.trim()
      ? options.caption
      : undefined;
    const fileName = typeof options.filename === 'string' && options.filename.trim()
      ? options.filename.trim()
      : undefined;
    const replyTo = Number.isFinite(options.topicId) ? options.topicId : undefined;
    const params = replyTo ? { replyTo } : undefined;
    const media = InputMedia.auto(uploadPath, {
      caption,
      fileName,
    });
    const peerRef = normalizeChannelId(channelId);
    const sent = await this.client.sendMedia(peerRef, media, params);
    return { messageId: sent.id };
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
    await this.client.downloadToFile(targetPath, location);
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
    const messages = results.map((message) => this._serializeMessage(message, message.chat));

    return {
      total: results.total ?? messages.length,
      next: results.next ?? null,
      messages,
    };
  }
}

export default TelegramClient;
