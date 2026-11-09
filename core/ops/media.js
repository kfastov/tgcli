// Downloading the media of a message.

import path from 'path';

import { defineOperation, field } from './operation.js';
import { chatOption } from './chat-keys.js';

function resolveKeepingTrailingSeparator(cwd, target) {
  const resolved = path.resolve(cwd, target);
  return /[\\/]$/.test(target) && !resolved.endsWith(path.sep) ? `${resolved}${path.sep}` : resolved;
}

export const mediaDownload = defineOperation({
  name: 'mediaDownload',
  command: 'media download',
  summary: 'Download message media',
  description: 'Downloads the media of a message to a file on the machine the server runs on, replacing a file already at that path.',
  annotations: { idempotent: true, destructive: true },
  input: {
    channelId: chatOption(),
    messageId: field.int('ID of the message with the media').flag('--id <msgId>', { help: 'Message id' }),
    outputPath: field.text('File path or directory for the download (default: the store media directory)').optional()
      .flag('--output <path>', { help: 'Output file path' }),
  },
  client: {
    // The server runs elsewhere: a relative path means the caller's directory. A
    // trailing separator stays, since it tells the server the path is a directory.
    prepare: (args, caller) => (args.outputPath
      ? { ...args, outputPath: resolveKeepingTrailingSeparator(caller.cwd, args.outputPath) }
      : args),
  },
  run: (ctx, args) => ctx.telegramClient.downloadMessageMedia(args.channelId, args.messageId, { outputPath: args.outputPath }),
  render(result, { print }) {
    print(`Downloaded to ${result.path} (${result.bytes} bytes).`);
  },
});

export const MEDIA_OPERATIONS = [mediaDownload];
